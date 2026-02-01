#!/usr/bin/env python3
# gmail_vehicle_backfill_range.py
#
# Lee correos IMAP (Gmail) en un rango fijo y registra VEHÍCULOS en la API
# SOLO si el tipo de alerta es: IMPACTO, FRENADA o ACELERACION.
#
# - Rango: 01-Nov-2025 (Lima) -> 31-Jan-2026 00:00 (Lima)  (incluye hasta 30-Ene-2026)
# - Endpoint: POST /api/vehicles
# - DTO: CreateVehicleRequest { vehicleCodeRaw, vehicleCodeNorm, licensePlate, companyId }
#
# Requisitos: pip install requests
#
# Extra: Cachea por:
#   - mensaje (Message-ID o subject+date) => no reprocesar el mismo correo
#   - vehicleCodeNorm => no intentar registrar el mismo vehículo múltiples veces
#   - licensePlate => idem (si viene placa)
#
# Nota: el backend ya tiene UNIQUE(company_id, vehicle_code_norm). El cache extra evita spam de requests.

import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import os
from datetime import datetime, timedelta, timezone
import re
import json
import html as html_lib

import requests

# =============== CONFIG ===============
GMAIL_USER = os.environ.get("GMAIL_USER", "yap32k@gmail.com") # (Reemplazar por la real)
GMAIL_PASS = os.environ.get("GMAIL_PASS", "intn rkry alig xhtl")  # app password (Reemplazar por la real)

IMAP_HOST = "imap.gmail.com"
IMAP_FOLDER = "INBOX"

API_BASE_URL = os.environ.get("ALERT_API_BASE", "https://samloto.com:4016")
VEHICLE_ENDPOINT = f"{API_BASE_URL}/api/vehicles"
COMPANY_ID = int(os.environ.get("ALERT_COMPANY_ID", "1"))

CACHE_DIR = "cache"
LIMA_TZ = timezone(timedelta(hours=-5))

# Rango pedido: desde noviembre 2025 hasta 30 enero 2026 (inclusive)
START_LIMA = datetime(2025, 11, 1, 0, 0, 0, tzinfo=LIMA_TZ)
END_EXCLUSIVE_LIMA = datetime(2026, 1, 31, 0, 0, 0, tzinfo=LIMA_TZ)  # exclusivo

# Tipos permitidos
ALLOWED_TYPES = {"IMPACTO", "FRENADA", "ACELERACION"}
# ======================================


# ---------- Helpers básicos ----------

def decode_maybe(encoded_header):
    if not encoded_header:
        return ""
    parts = decode_header(encoded_header)
    decoded = []
    for text, enc in parts:
        if isinstance(text, bytes):
            decoded.append(text.decode(enc or "utf-8", errors="ignore"))
        else:
            decoded.append(text)
    return "".join(decoded)


def connect():
    mail = imaplib.IMAP4_SSL(IMAP_HOST)
    mail.login(GMAIL_USER, GMAIL_PASS)
    mail.select(IMAP_FOLDER)
    return mail


def imap_date(dt: datetime) -> str:
    # IMAP espera: "01-Dec-2025"
    return dt.strftime("%d-%b-%Y")


def fetch_range_any(mail, start_lima: datetime, end_exclusive_lima: datetime):
    """
    Devuelve IDs de correos en el rango.
    IMAP: SINCE start, BEFORE end
    """
    date_from = imap_date(start_lima)
    date_to = imap_date(end_exclusive_lima)

    status, data = mail.search(None, "SINCE", date_from, "BEFORE", date_to)
    if status != "OK":
        print("Error al buscar mensajes del rango:", status)
        return []
    return data[0].split()


def get_message_datetime(msg):
    date_hdr = msg.get("Date")
    if not date_hdr:
        return datetime.now(timezone.utc)
    try:
        dt = parsedate_to_datetime(date_hdr)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception as e:
        print("No se pudo parsear fecha del mensaje, usando ahora():", e)
        return datetime.now(timezone.utc)


# ---------- CACHE (mensajes + códigos + placas) ----------

def ensure_cache_dir():
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)


def msg_cache_path():
    ensure_cache_dir()
    # cache único para este rango fijo (mensajes procesados)
    return os.path.join(CACHE_DIR, "vehicles_cache_msgs_20251101_20260130.json")


def codes_cache_path():
    ensure_cache_dir()
    return os.path.join(CACHE_DIR, "vehicles_cache_codes_20251101_20260130.json")


def plates_cache_path():
    ensure_cache_dir()
    return os.path.join(CACHE_DIR, "vehicles_cache_plates_20251101_20260130.json")


def load_cache_file(path: str) -> set:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(data)
        if isinstance(data, dict):
            return set(data.keys())
        return set()
    except Exception:
        return set()


def append_cache_key(path: str, cache_key: str):
    keys = load_cache_file(path)
    if cache_key in keys:
        return
    keys.add(cache_key)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(keys), f, ensure_ascii=False, indent=2)


# ---------- PARSEO CORREO ----------

def extract_body_text(msg):
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disp = str(part.get("Content-Disposition", ""))
            if content_type == "text/plain" and "attachment" not in content_disp:
                try:
                    return part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="ignore"
                    )
                except Exception:
                    pass
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disp = str(part.get("Content-Disposition", ""))
            if content_type == "text/html" and "attachment" not in content_disp:
                try:
                    return part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="ignore"
                    )
                except Exception:
                    pass
        return ""
    else:
        try:
            return msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", errors="ignore"
            )
        except Exception:
            return ""


def looks_like_html(s: str) -> bool:
    if not s:
        return False
    low = s.lower()
    return "<html" in low or "<body" in low or "<br" in low or "</div" in low or "<p" in low


def html_to_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</p\s*>", "\n", s)
    s = re.sub(r"(?i)</div\s*>", "\n", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html_lib.unescape(s)
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n", s)
    return s.strip()


def parse_subject(subject: str):
    """
    "xxx - ALERT_TYPE - VEHCODE (PLATE)".
    Devuelve: (alert_type, vehicle_code, license_plate)
    """
    alert_type = None
    vehicle_code = None
    license_plate = None

    if not subject:
        return alert_type, vehicle_code, license_plate

    parts = [p.strip() for p in subject.split("-")]
    if len(parts) >= 3:
        alert_type = parts[1].strip() or None
        third = parts[2].strip()
        m = re.match(r"([^\s(]+)\s*\(([^)]+)\)", third)
        if m:
            vehicle_code = m.group(1).strip() or None
            license_plate = m.group(2).strip() or None
        else:
            tokens = third.split()
            if tokens:
                vehicle_code = tokens[0].strip() or None

    return alert_type, vehicle_code, license_plate


def normalize_code(s: str) -> str:
    # igual que tu backend: trim + upper + quitar espacios internos
    if s is None:
        return ""
    x = s.strip().upper()
    x = re.sub(r"\s+", "", x)
    return x


def normalize_plate(s: str) -> str:
    if not s:
        return ""
    x = s.strip().upper()
    x = re.sub(r"\s+", "", x)
    return x


def normalize_alert_type(s: str) -> str:
    if not s:
        return ""
    x = s.strip().upper()
    # normalizar acentos
    x = (x.replace("Ó", "O")
           .replace("Á", "A")
           .replace("É", "E")
           .replace("Í", "I")
           .replace("Ú", "U"))
    return x


def guess_alert_type(subject: str, body_text: str) -> str:
    """
    Si subject no trae el tipo, intenta deducirlo del cuerpo.
    """
    s = normalize_alert_type(subject or "")
    t = normalize_alert_type(body_text or "")

    for k in ALLOWED_TYPES:
        if k in s or k in t:
            return k

    # tolerancias comunes (por si viene "ACELERACIÓN")
    if "ACELERACI" in s or "ACELERACI" in t:
        return "ACELERACION"
    return ""


def build_vehicle_payload(subject: str, body_text: str) -> dict | None:
    """
    Devuelve payload para POST /api/vehicles o None si:
    - no es IMPACTO/FRENADA/ACELERACION
    - no hay vehicleCode
    """
    alert_type, vehicle_code, license_plate = parse_subject(subject)

    parse_text = html_to_text(body_text) if looks_like_html(body_text) else (body_text or "")

    if not alert_type:
        alert_type = guess_alert_type(subject, parse_text)

    alert_type_norm = normalize_alert_type(alert_type)

    if alert_type_norm not in ALLOWED_TYPES:
        return None

    if not vehicle_code or vehicle_code.strip() == "":
        # fallback: intenta capturar un código razonable en el cuerpo
        m = re.search(r"\b(MG\d{2,}|[A-Z0-9-]{4,})\b", normalize_alert_type(parse_text))
        vehicle_code = m.group(1) if m else None

    if not vehicle_code:
        return None

    raw = vehicle_code.strip()
    norm = normalize_code(raw)

    plate = license_plate.strip()[:50] if license_plate else None

    return {
        "companyId": COMPANY_ID,
        "vehicleCodeRaw": raw[:50],
        "vehicleCodeNorm": norm[:50],
        "licensePlate": plate,
    }


# ---------- API ----------

def send_vehicle_to_api(payload: dict) -> bool:
    """
    POST /api/vehicles
    Si ya existe, tu API devuelve 409 => lo tomamos como OK.
    """
    try:
        resp = requests.post(VEHICLE_ENDPOINT, json=payload, timeout=15)
        if 200 <= resp.status_code < 300:
            print(f">>> API OK ({resp.status_code}) - vehículo registrado")
            return True
        if resp.status_code == 409:
            print(f">>> API OK (409) - vehículo ya existía")
            return True
        print(f">>> API ERROR ({resp.status_code}): {resp.text}")
        return False
    except Exception as e:
        print(">>> ERROR llamando a la API:", e)
        return False


# ---------- Procesamiento ----------

def process_message(
    mail,
    msg_id,
    processed_msgs: set,
    seen_codes: set,
    seen_plates: set,
    msgs_cache_fp: str,
    codes_cache_fp: str,
    plates_cache_fp: str,
):
    status, msg_data = mail.fetch(msg_id, "(RFC822)")
    if status != "OK":
        print(f"Error al descargar mensaje {msg_id}: {status}")
        return False

    msg = email.message_from_bytes(msg_data[0][1])

    subject = decode_maybe(msg.get("Subject"))
    from_ = decode_maybe(msg.get("From"))
    msg_dt_utc = get_message_datetime(msg)
    message_id = (msg.get("Message-ID") or "").strip()

    msg_key = message_id or f"{subject}|{msg_dt_utc.isoformat()}"
    if msg_key in processed_msgs:
        return False

    body_text = extract_body_text(msg)

    # filtro básico: no procesar basura
    text_to_search = (subject or "") + "\n" + (body_text or "")
    low = text_to_search.lower()
    if ("alarma" not in low) and ("checklist" not in low) and ("impacto" not in low) and ("fren" not in low) and ("aceler" not in low):
        return False

    vehicle_payload = build_vehicle_payload(subject, body_text)
    if vehicle_payload is None:
        return False

    code_norm = normalize_code(vehicle_payload.get("vehicleCodeNorm") or "")
    plate_norm = normalize_plate(vehicle_payload.get("licensePlate") or "")

    # ---- dedupe por vehículo en la misma corrida/rango ----
    if code_norm and code_norm in seen_codes:
        # igual marcamos el msg como procesado para no revisit
        append_cache_key(msgs_cache_fp, msg_key)
        processed_msgs.add(msg_key)
        return False

    if plate_norm and plate_norm in seen_plates:
        append_cache_key(msgs_cache_fp, msg_key)
        processed_msgs.add(msg_key)
        return False

    print("=" * 60)
    print(f"IMAP ID: {msg_id}")
    print(f"Message-ID: {message_id}")
    print(f"From: {from_}")
    print(f"Subject: {subject}")
    print(f"Header date (UTC): {msg_dt_utc}")
    print(f"Payload vehicle: {vehicle_payload}")

    if send_vehicle_to_api(vehicle_payload):
        # cache msg
        append_cache_key(msgs_cache_fp, msg_key)
        processed_msgs.add(msg_key)

        # cache dedupe (code/plate)
        if code_norm:
            seen_codes.add(code_norm)
            append_cache_key(codes_cache_fp, code_norm)

        if plate_norm:
            seen_plates.add(plate_norm)
            append_cache_key(plates_cache_fp, plate_norm)

        # opcional: marcar visto
        # mail.store(msg_id, "+FLAGS", "\\Seen")

        return True

    return False


def main():
    print("Backfill vehículos (IMPACTO/FRENADA/ACELERACION)")
    print(f"Rango IMAP (Lima): {START_LIMA} -> {END_EXCLUSIVE_LIMA} (end exclusivo)")

    msgs_cache_fp = msg_cache_path()
    codes_cache_fp = codes_cache_path()
    plates_cache_fp = plates_cache_path()

    processed_msgs = load_cache_file(msgs_cache_fp)
    seen_codes = load_cache_file(codes_cache_fp)
    seen_plates = load_cache_file(plates_cache_fp)

    print(f"Mensajes ya procesados (cache): {len(processed_msgs)}")
    print(f"Códigos ya vistos (cache): {len(seen_codes)}")
    print(f"Placas ya vistas (cache): {len(seen_plates)}")

    mail = connect()
    print("Conectado a IMAP. Buscando correos del rango...")

    try:
        msg_ids = fetch_range_any(mail, START_LIMA, END_EXCLUSIVE_LIMA)
        print(f"Encontrados {len(msg_ids)} correos en el rango.")

        sent = 0
        skipped = 0

        for msg_id in msg_ids:
            ok = process_message(
                mail=mail,
                msg_id=msg_id,
                processed_msgs=processed_msgs,
                seen_codes=seen_codes,
                seen_plates=seen_plates,
                msgs_cache_fp=msgs_cache_fp,
                codes_cache_fp=codes_cache_fp,
                plates_cache_fp=plates_cache_fp,
            )
            if ok:
                sent += 1
            else:
                skipped += 1

        print("=" * 60)
        print(f"FIN. Vehículos registrados (o ya existían): {sent} | Saltados: {skipped}")
        print(f"Cache msgs:   {msgs_cache_fp}")
        print(f"Cache codes:  {codes_cache_fp}")
        print(f"Cache plates: {plates_cache_fp}")

    finally:
        mail.logout()
        print("Desconectado de IMAP.")


if __name__ == "__main__":
    main()
