# gmail_alert_month_backfill.py
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import os
from datetime import datetime, timedelta, timezone
import re
import json
import glob
import html as html_lib

import requests  # pip install requests

# =============== CONFIG ===============
GMAIL_USER = os.environ.get("GMAIL_USER", "yap32k@gmail.com")
GMAIL_PASS = os.environ.get("GMAIL_PASS", "intn rkry alig xhtl")  # app password

IMAP_HOST = "imap.gmail.com"
IMAP_FOLDER = "INBOX"

API_BASE_URL = os.environ.get("ALERT_API_BASE", "https://samloto.com:4016")
ALERT_ENDPOINT = f"{API_BASE_URL}/api/alerts"
COMPANY_ID = int(os.environ.get("ALERT_COMPANY_ID", "1"))

CACHE_DIR = "cache"
LIMA_TZ = timezone(timedelta(hours=-5))

# Permite forzar año/mes (opcional):
#   ALERT_YEAR=2025 ALERT_MONTH=12 python3 gmail_alert_month_backfill.py
FORCE_YEAR = os.environ.get("ALERT_YEAR")
FORCE_MONTH = os.environ.get("ALERT_MONTH")
# ======================================


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
    # IMAP espera formato: "01-Dec-2025"
    return dt.strftime("%d-%b-%Y")


def month_range_lima():
    now = datetime.now(LIMA_TZ)
    year = int(FORCE_YEAR) if FORCE_YEAR else now.year
    month = int(FORCE_MONTH) if FORCE_MONTH else now.month

    start = datetime(year=year, month=month, day=1, tzinfo=LIMA_TZ)

    if month == 12:
        end = datetime(year=year + 1, month=1, day=1, tzinfo=LIMA_TZ)
    else:
        end = datetime(year=year, month=month + 1, day=1, tzinfo=LIMA_TZ)

    return start, end, year, month


def fetch_month_any(mail, month_start_lima: datetime, next_month_start_lima: datetime):
    """
    Devuelve IDs de correos (leídos y no leídos) del mes.
    IMAP usa rango por día: SINCE start, BEFORE end
    """
    date_from = imap_date(month_start_lima)
    date_to = imap_date(next_month_start_lima)

    status, data = mail.search(None, "SINCE", date_from, "BEFORE", date_to)
    if status != "OK":
        print("Error al buscar mensajes del mes:", status)
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


# ---------- CACHÉS (diario + mensual) ----------

def ensure_cache_dir():
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)


def today_cache_path():
    ensure_cache_dir()
    today_lima = datetime.now(LIMA_TZ).strftime("%Y%m%d")
    return os.path.join(CACHE_DIR, f"alerts_cache_{today_lima}.json")


def month_cache_path(year: int, month: int):
    ensure_cache_dir()
    return os.path.join(CACHE_DIR, f"alerts_cache_month_{year}{month:02d}.json")


def load_cache_file(path: str) -> set:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data)
    except Exception:
        return set()


def append_cache_key(path: str, cache_key: str):
    keys = load_cache_file(path)
    if cache_key in keys:
        return
    keys.add(cache_key)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(keys), f, ensure_ascii=False, indent=2)


def load_all_month_daily_caches(year: int, month: int) -> set:
    """
    Lee TODOS los caches diarios del mes (los que genera tu listener),
    más el cache mensual, y también el cache de hoy.
    """
    ensure_cache_dir()

    pattern = os.path.join(CACHE_DIR, f"alerts_cache_{year}{month:02d}*.json")
    daily_files = glob.glob(pattern)

    all_keys = set()
    for fp in daily_files:
        all_keys |= load_cache_file(fp)

    # cache mensual (para re-ejecutar este backfill sin duplicar)
    all_keys |= load_cache_file(month_cache_path(year, month))

    # cache de hoy (lo usa el listener para no re-enviar)
    all_keys |= load_cache_file(today_cache_path())

    return all_keys


# ---------- PARSEO DEL CORREO ----------

def extract_body_text(msg):
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disp = str(part.get("Content-Disposition", ""))
            if content_type == "text/plain" and "attachment" not in content_disp:
                try:
                    return part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8",
                        errors="ignore",
                    )
                except Exception:
                    pass
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disp = str(part.get("Content-Disposition", ""))
            if content_type == "text/html" and "attachment" not in content_disp:
                try:
                    return part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8",
                        errors="ignore",
                    )
                except Exception:
                    pass
        return ""
    else:
        try:
            return msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8",
                errors="ignore",
            )
        except Exception:
            return ""


def looks_like_html(s: str) -> bool:
    if not s:
        return False
    low = s.lower()
    return "<html" in low or "<body" in low or "<br" in low or "</div" in low or "<p" in low


def html_to_text(s: str) -> str:
    """
    Convierte HTML básico a texto para poder parsear campos (Área, Planta, Operador, etc).
    Sin dependencias externas.
    """
    if not s:
        return ""

    # saltos de línea típicos
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</p\s*>", "\n", s)
    s = re.sub(r"(?i)</div\s*>", "\n", s)

    # quitar tags
    s = re.sub(r"<[^>]+>", " ", s)

    # decode entities
    s = html_lib.unescape(s)

    # normalizar espacios
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n", s)

    return s.strip()


def parse_subject(subject: str):
    alert_type = None
    vehicle_code = None
    license_plate = None

    if not subject:
        return alert_type, vehicle_code, license_plate, "GENERIC_EMAIL"

    subj_lower = subject.lower()
    if "checklist" in subj_lower:
        template_source = "CHECKLIST_EMAIL"
    elif "alarma" in subj_lower:
        template_source = "ALARM_EMAIL"
    else:
        template_source = "GENERIC_EMAIL"

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

    return alert_type, vehicle_code, license_plate, template_source


def parse_plant(body_text: str):
    if not body_text:
        return None

    for line in body_text.splitlines():
        line_clean = line.strip()
        m = re.search(r"(Planta|Sede)\s*:\s*(.+)", line_clean, re.IGNORECASE)
        if m:
            val = m.group(2).strip()
            val = re.split(r"\s{2,}|\t|\|", val)[0].strip()
            return val or None

    m2 = re.search(r"(Planta|Sede)\s*:\s*([^\n\r]+)", body_text, re.IGNORECASE)
    if m2:
        val = m2.group(2).strip()
        val = re.split(r"\s{2,}|\t|\|", val)[0].strip()
        return val or None

    return None


def parse_area(body_text: str):
    """
    Busca Área / Area / Zona / Ubicación / Lugar, tolerante a formatos.
    """
    if not body_text:
        return None

    # 1) Intento line-by-line
    for line in body_text.splitlines():
        line_clean = line.strip()
        if not line_clean:
            continue

        m = re.search(
            r"(?:\bÁrea\b|\bArea\b|\bZona\b|\bUbicación\b|\bUbicacion\b|\bLugar\b)\s*:\s*(.+)",
            line_clean,
            re.IGNORECASE,
        )
        if m:
            val = m.group(1).strip()
            val = re.split(r"\s{2,}|\t|\|", val)[0].strip()
            return val or None

    # 2) Fallback: buscar en todo el texto (si vino “pegado”)
    m2 = re.search(
        r"(?:\bÁrea\b|\bArea\b|\bZona\b|\bUbicación\b|\bUbicacion\b|\bLugar\b)\s*:\s*([^\n\r]+)",
        body_text,
        re.IGNORECASE,
    )
    if m2:
        val = m2.group(1).strip()
        val = re.split(r"\s{2,}|\t|\|", val)[0].strip()
        return val or None

    return None


def parse_operator(body_text: str):
    operator_name = None
    operator_id = None

    for line in body_text.splitlines():
        line_clean = line.strip()
        m_name = re.search(r"Operador\s*:\s*(.+)", line_clean, re.IGNORECASE)
        if m_name and not operator_name:
            operator_name = m_name.group(1).strip()

        m_id = re.search(r"(ID\s*Operador|DNI)\s*:\s*(.+)", line_clean, re.IGNORECASE)
        if m_id and not operator_id:
            operator_id = m_id.group(2).strip()

    return operator_name, operator_id


def parse_event_time_from_body(body_text: str, fallback_dt_utc: datetime):
    pattern = re.compile(
        r"(?:Alarma\s+Fecha|Fecha)\s*:\s*([0-9]{2}-[A-Za-z]{3}-[0-9]{4}).*?Hora\s*:\s*([0-9]{2}:[0-9]{2})",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(body_text)
    if not match:
        return fallback_dt_utc

    date_str = match.group(1)
    time_str = match.group(2)

    months = {
        "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
        "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    }

    try:
        d, mon_str, y = date_str.split("-")
        mon = months.get(mon_str.lower())
        if not mon:
            return fallback_dt_utc

        hh, mm = time_str.split(":")
        dt_lima = datetime(
            year=int(y), month=mon, day=int(d),
            hour=int(hh), minute=int(mm),
            tzinfo=LIMA_TZ,
        )
        return dt_lima.astimezone(timezone.utc)
    except Exception:
        return fallback_dt_utc


def guess_severity(alert_type: str, body_text: str) -> str:
    t = (alert_type or "").upper()
    text = (body_text or "").lower()

    if "sin condiciones" in text or "bloquea" in text:
        return "BLOQUEA_OPERACION"
    if "IMPACTO" in t:
        return "CRITICAL"
    if "EXCESO" in t or "VELOCIDAD" in t:
        return "WARNING"
    return "INFO"


def build_alert_payload(subject: str, body_text: str, msg_dt_utc: datetime) -> dict:
    alert_type, vehicle_code, license_plate, template_source = parse_subject(subject)

    parse_text = html_to_text(body_text) if looks_like_html(body_text) else (body_text or "")

    plant = parse_plant(parse_text)
    area = parse_area(parse_text)
    operator_name, operator_id = parse_operator(parse_text)

    event_time_dt = parse_event_time_from_body(parse_text, msg_dt_utc)
    severity = guess_severity(alert_type, parse_text)

    if not vehicle_code:
        vehicle_code = "UNKNOWN"

    if not alert_type:
        m = re.search(r"(IMPACTO|EXCESO\s+VELOCIDAD|CHECKLIST|ALARMA)", parse_text, re.IGNORECASE)
        alert_type = m.group(1).upper() if m else "DESCONOCIDO"

    short_description = f"{alert_type} - {vehicle_code}"
    if plant:
        short_description += f" - Planta: {plant}"
    if area:
        short_description += f" - Área: {area}"
    short_description = short_description[:1000]

    raw_payload = body_text or (subject or "")
    if not raw_payload.strip():
        raw_payload = "EMPTY_EMAIL"

    return {
        "vehicleCode": vehicle_code,
        "alertType": alert_type,
        "eventTime": event_time_dt.isoformat(),
        "companyId": COMPANY_ID,

        "licensePlate": license_plate,
        "alertSubtype": None,
        "templateSource": template_source,
        "severity": severity,

        "subject": (subject[:255] if subject else None),
        "plant": plant,
        "area": area,
        "ownerOrVendor": None,
        "brandModel": None,

        "operatorName": operator_name,
        "operatorId": operator_id,

        "shortDescription": short_description,
        "details": None,

        "rawPayload": raw_payload[:5000],
    }


def send_alert_to_api(payload: dict) -> bool:
    try:
        resp = requests.post(ALERT_ENDPOINT, json=payload, timeout=15)
        if 200 <= resp.status_code < 300:
            print(f">>> API OK ({resp.status_code}) - alerta registrada")
            return True
        print(f">>> API ERROR ({resp.status_code}): {resp.text}")
        return False
    except Exception as e:
        print(">>> ERROR llamando a la API:", e)
        return False


def process_message(mail, msg_id, processed_keys: set, month_cache_fp: str, today_cache_fp: str):
    status, msg_data = mail.fetch(msg_id, "(RFC822)")
    if status != "OK":
        print(f"Error al descargar mensaje {msg_id}: {status}")
        return False

    msg = email.message_from_bytes(msg_data[0][1])

    subject = decode_maybe(msg.get("Subject"))
    from_ = decode_maybe(msg.get("From"))
    msg_dt_utc = get_message_datetime(msg)
    message_id = (msg.get("Message-ID") or "").strip()

    cache_key = message_id or f"{subject}|{msg_dt_utc.isoformat()}"

    if cache_key in processed_keys:
        return False

    body_text = extract_body_text(msg)

    # Filtrado básico: solo correos relevantes
    text_to_search = (subject or "") + "\n" + (body_text or "")
    low = text_to_search.lower()
    if ("alarma" not in low) and ("checklist" not in low):
        return False

    print("=" * 60)
    print(f"IMAP ID: {msg_id}")
    print(f"Message-ID: {message_id}")
    print(f"From: {from_}")
    print(f"Subject: {subject}")
    print(f"Header date (UTC): {msg_dt_utc}")

    payload = build_alert_payload(subject, body_text, msg_dt_utc)

    if send_alert_to_api(payload):
        append_cache_key(month_cache_fp, cache_key)
        append_cache_key(today_cache_fp, cache_key)
        processed_keys.add(cache_key)

        mail.store(msg_id, "+FLAGS", "\\Seen")
        return True

    return False


def main():
    month_start, next_month_start, year, month = month_range_lima()
    print(f"Backfill del mes: {year}-{month:02d}")
    print(f"Rango IMAP (Lima): {month_start} -> {next_month_start}")

    month_fp = month_cache_path(year, month)
    today_fp = today_cache_path()

    processed_keys = load_all_month_daily_caches(year, month)
    print(f"Claves ya procesadas (diarios del mes + mensual + hoy): {len(processed_keys)}")

    mail = connect()
    print("Conectado a IMAP. Buscando correos del mes (leídos y no leídos)...")

    try:
        msg_ids = fetch_month_any(mail, month_start, next_month_start)
        print(f"Encontrados {len(msg_ids)} correos en el rango del mes.")

        sent = 0
        skipped = 0

        for msg_id in msg_ids:
            ok = process_message(mail, msg_id, processed_keys, month_fp, today_fp)
            if ok:
                sent += 1
            else:
                skipped += 1

        print("=" * 60)
        print(f"FIN. Enviadas a API: {sent} | Saltadas (cache/no-alerta/fallo): {skipped}")
        print(f"Cache mensual: {month_fp}")
        print(f"Cache hoy: {today_fp}")

    finally:
        mail.logout()
        print("Desconectado de IMAP.")


if __name__ == "__main__":
    main()
