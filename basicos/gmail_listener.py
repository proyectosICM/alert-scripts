# gmail_alert_listener.py
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import time
import os
from datetime import datetime, timedelta, timezone
import re
import json

import requests  # pip install requests

# =============== CONFIG ===============

GMAIL_USER = os.environ.get("GMAIL_USER", "yap32k@gmail.com")  # (Reemplazar por la real)
GMAIL_PASS = os.environ.get("GMAIL_PASS", "intn rkry alig xhtl")  # app password (NO hardcodear)

IMAP_HOST = "imap.gmail.com"
IMAP_FOLDER = "INBOX"

# API de tu backend Spring
API_BASE_URL = os.environ.get("ALERT_API_BASE", "https://samloto.com:4016")
ALERT_ENDPOINT = f"{API_BASE_URL}/api/alerts"
COMPANY_ID = int(os.environ.get("ALERT_COMPANY_ID", "1"))

# Ventana activa / pausa
WORK_WINDOW_SECONDS = 60            # trabaja 1 minuto
POLL_INTERVAL_SECONDS = 10          # chequea cada 10 s dentro de ese minuto
SLEEP_BETWEEN_CYCLES_SECONDS = 120  # duerme 2 minutos

# Caché diario
CACHE_DIR = "cache"
LIMA_TZ = timezone(timedelta(hours=-5))

# SOLO estos tipos se envían a /api/alerts
ALLOWED_TYPES = {"IMPACTO", "FRENADA", "ACELERACION"}

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


def fetch_recent_any(mail, days_back: int = 1):
    """
    Devuelve IDs de correos (LEÍDOS y NO LEÍDOS) desde hace 'days_back' días.
    Ej: days_back=1 -> desde AYER (incluye ayer y hoy).

    OJO: no filtramos por UNSEEN.
    """
    date_from = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")
    status, data = mail.search(None, "SINCE", date_from)

    if status != "OK":
        print("Error al buscar mensajes SINCE", date_from, ":", status)
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
        print("No se pudo parsear la fecha del mensaje, usando ahora():", e)
        return datetime.now(timezone.utc)


# ---------- CACHÉ (HOY + AYER) ----------

def ensure_cache_dir():
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)


def cache_path_for_day(day_lima: datetime):
    ensure_cache_dir()
    ymd = day_lima.strftime("%Y%m%d")
    return os.path.join(CACHE_DIR, f"alerts_cache_{ymd}.json")


def get_today_cache_path():
    return cache_path_for_day(datetime.now(LIMA_TZ))


def load_cache_file(path: str) -> set:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()


def load_recent_cache(days: int = 2) -> set:
    """
    Carga cache de HOY + (days-1) días atrás.
    Útil para no duplicar cuando haces fetch desde AYER.
    """
    now = datetime.now(LIMA_TZ)
    keys = set()
    for i in range(days):
        d = now - timedelta(days=i)
        keys |= load_cache_file(cache_path_for_day(d))
    return keys


def append_cache_key(cache_key: str):
    path = get_today_cache_path()
    keys = load_cache_file(path)
    if cache_key in keys:
        return
    keys.add(cache_key)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(keys), f, ensure_ascii=False, indent=2)
    print(f">>> Cache actualizado ({path}): {cache_key}")


# ---------- PARSEO DEL CORREO ----------

def extract_body_text(msg):
    """
    Devuelve el cuerpo como texto.
    Preferimos text/plain; si no hay, usamos text/html como texto crudo.
    """
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
                except Exception as e:
                    print("Error decodificando text/plain:", e)

        for part in msg.walk():
            content_type = part.get_content_type()
            content_disp = str(part.get("Content-Disposition", ""))
            if content_type == "text/html" and "attachment" not in content_disp:
                try:
                    return part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8",
                        errors="ignore",
                    )
                except Exception as e:
                    print("Error decodificando text/html:", e)

        return ""
    else:
        try:
            return msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8",
                errors="ignore",
            )
        except Exception as e:
            print("Error decodificando body:", e)
            return ""


def normalize_alert_text(s: str) -> str:
    if not s:
        return ""
    x = s.strip().upper()
    x = (x.replace("Ó", "O")
           .replace("Á", "A")
           .replace("É", "E")
           .replace("Í", "I")
           .replace("Ú", "U"))
    return x


def canonical_alert_type(s: str) -> str:
    """
    Mapea variaciones a los 3 tipos permitidos:
      - contiene IMPACTO => IMPACTO
      - contiene FREN... => FRENADA
      - contiene ACELER... => ACELERACION
    """
    x = normalize_alert_text(s)
    if "IMPACTO" in x:
        return "IMPACTO"
    if "FREN" in x:
        return "FRENADA"
    if "ACELER" in x:
        return "ACELERACION"
    return ""


def guess_allowed_type(subject: str, body_text: str) -> str:
    t = canonical_alert_type(subject or "")
    if t:
        return t
    return canonical_alert_type(body_text or "")


def parse_subject(subject: str):
    """
    Ejemplo típico:
      'Alarma - IMPACTO - MG069 (308FG25-3)'

    Devuelve:
      alert_type    -> 'IMPACTO'
      vehicle_code  -> 'MG069'
      license_plate -> '308FG25-3' (si viene entre paréntesis)
      template_src  -> ALARM_EMAIL / CHECKLIST_EMAIL / GENERIC_EMAIL
    """
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
    for line in body_text.splitlines():
        line_clean = line.strip()
        m = re.search(r"(Planta|Sede)\s*:\s*(.+)", line_clean, re.IGNORECASE)
        if m:
            return m.group(2).strip()
    return None


def parse_area(body_text: str):
    area = None
    for line in body_text.splitlines():
        line_clean = line.strip()
        m = re.search(r"Área?\s*:\s*(.+)", line_clean, re.IGNORECASE)
        if m:
            area = m.group(1).strip()
            break
    return area


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
        mon = months.get(mon_str.lower(), None)
        if mon is None:
            return fallback_dt_utc

        hh, mm = time_str.split(":")
        dt_lima = datetime(
            year=int(y),
            month=mon,
            day=int(d),
            hour=int(hh),
            minute=int(mm),
            tzinfo=LIMA_TZ,
        )
        return dt_lima.astimezone(timezone.utc)
    except Exception as e:
        print("No se pudo parsear fecha/hora desde el body:", e)
        return fallback_dt_utc


def guess_severity(alert_type: str, body_text: str) -> str:
    t = normalize_alert_text(alert_type or "")
    text = (body_text or "").lower()

    if "sin condiciones" in text or "bloquea" in text:
        return "BLOQUEA_OPERACION"

    if "IMPACTO" in t:
        return "CRITICAL"

    if "FREN" in t or "ACELER" in t:
        return "WARNING"

    return "INFO"


def build_alert_payload(subject: str, body_text: str, msg_dt_utc: datetime) -> dict:
    alert_type_raw, vehicle_code, license_plate, template_source = parse_subject(subject)
    plant = parse_plant(body_text)
    area = parse_area(body_text)
    operator_name, operator_id = parse_operator(body_text)

    event_time_dt = parse_event_time_from_body(body_text, msg_dt_utc)

    # Solo mapeamos a los 3 permitidos (o DESCONOCIDO)
    alert_type = canonical_alert_type(alert_type_raw or "")
    if not alert_type:
        alert_type = guess_allowed_type(subject, body_text) or "DESCONOCIDO"

    severity = guess_severity(alert_type, body_text)

    if not vehicle_code:
        vehicle_code = "UNKNOWN"

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
        resp = requests.post(ALERT_ENDPOINT, json=payload, timeout=10)
        if 200 <= resp.status_code < 300:
            print(f">>> API OK ({resp.status_code}) - alerta registrada")
            return True
        print(f">>> API ERROR ({resp.status_code}): {resp.text}")
        return False
    except Exception as e:
        print(">>> ERROR llamando a la API:", e)
        return False


def cache_as_processed(cache_key: str, processed_keys: set):
    append_cache_key(cache_key)
    processed_keys.add(cache_key)


# ---------- PROCESO PRINCIPAL POR MENSAJE ----------

def process_message(mail, msg_id, processed_keys: set):
    status, msg_data = mail.fetch(msg_id, "(RFC822)")
    if status != "OK":
        print(f"Error al descargar mensaje {msg_id}: {status}")
        return

    msg = email.message_from_bytes(msg_data[0][1])

    subject = decode_maybe(msg.get("Subject"))
    from_ = decode_maybe(msg.get("From"))
    msg_dt_utc = get_message_datetime(msg)
    message_id = (msg.get("Message-ID") or "").strip()

    cache_key = message_id or f"{subject}|{msg_dt_utc.isoformat()}"

    if cache_key in processed_keys:
        return

    body_text = extract_body_text(msg)

    # filtro rápido: si no parece relacionado, salir sin cache
    text_to_search = (subject or "") + "\n" + (body_text or "")
    low = text_to_search.lower()
    if (
        ("alarma" not in low)
        and ("checklist" not in low)
        and ("impacto" not in low)
        and ("fren" not in low)
        and ("aceler" not in low)
    ):
        return

    # 1) CHECKLIST nunca se envía, pero sí se cachea
    if "checklist" in low:
        cache_as_processed(cache_key, processed_keys)
        return

    payload = build_alert_payload(subject, body_text, msg_dt_utc)
    alert_type = payload.get("alertType") or ""

    # 2) Si no es de los 3 permitidos, NO se envía, pero sí se cachea
    if alert_type not in ALLOWED_TYPES:
        cache_as_processed(cache_key, processed_keys)
        return

    print("=" * 60)
    print("Nuevo correo -> alerta permitida:")
    print(f"ID IMAP: {msg_id}")
    print(f"Message-ID: {message_id}")
    print(f"De: {from_}")
    print(f"Asunto: {subject}")
    print(f"Fecha header (UTC): {msg_dt_utc}")
    print(f"AlertType (allowed): {alert_type}")

    if send_alert_to_api(payload):
        cache_as_processed(cache_key, processed_keys)
        mail.store(msg_id, "+FLAGS", "\\Seen")
    else:
        print(">>> La API falló, NO se marca como procesado para reintentar luego.")


def check_mail_once():
    now_lima = datetime.now(LIMA_TZ)
    print(f"Chequeando correos a las {now_lima.isoformat()} (hora Lima)")
    print(f"ALLOWED_TYPES: {sorted(ALLOWED_TYPES)} (CHECKLIST bloqueado y cacheado)")

    # Importante: cache de hoy + ayer (evita duplicar con days_back=1)
    processed_keys = load_recent_cache(days=2)
    print(f"Claves ya procesadas (hoy+ayer): {len(processed_keys)}")

    mail = connect()
    print("Conectado a Gmail IMAP, buscando correos (leídos y no leídos) desde ayer…")

    try:
        msg_ids = fetch_recent_any(mail, days_back=1)
        if msg_ids:
            print(f"Encontrados {len(msg_ids)} correo(s) desde ayer (leídos y no leídos).")
            for msg_id in msg_ids:
                process_message(mail, msg_id, processed_keys)
        else:
            print("Sin correos en el rango (desde ayer).")
    finally:
        mail.logout()
        print("Desconectado de IMAP.")


def main():
    while True:
        window_start = datetime.now(timezone.utc)
        window_end = window_start + timedelta(seconds=WORK_WINDOW_SECONDS)

        print("=" * 60)
        print(
            f"Ventana ACTIVA de {WORK_WINDOW_SECONDS} s "
            f"(desde {window_start} hasta {window_end})"
        )

        while datetime.now(timezone.utc) < window_end:
            try:
                check_mail_once()
            except Exception as e:
                print("Error en check_mail_once():", e)

            print(f"Esperando {POLL_INTERVAL_SECONDS} s dentro de ventana activa…")
            time.sleep(POLL_INTERVAL_SECONDS)

        print(
            f"Ventana activa terminada. Durmiendo {SLEEP_BETWEEN_CYCLES_SECONDS} s "
            "antes de la próxima ventana…"
        )
        time.sleep(SLEEP_BETWEEN_CYCLES_SECONDS)


if __name__ == "__main__":
    main()
