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

GMAIL_USER = os.environ.get("GMAIL_USER", "yap32k@gmail.com")
GMAIL_PASS = os.environ.get("GMAIL_PASS", "intn rkry alig xhtl")

IMAP_HOST = "imap.gmail.com"
IMAP_FOLDER = "INBOX"

# API de tu backend Spring
API_BASE_URL = os.environ.get("ALERT_API_BASE", "http://192.168.0.248:8080")
ALERT_ENDPOINT = f"{API_BASE_URL}/api/alerts"

# Ventana activa / pausa
WORK_WINDOW_SECONDS = 60          # trabaja 1 minuto
POLL_INTERVAL_SECONDS = 10        # chequea cada 10 s dentro de ese minuto
SLEEP_BETWEEN_CYCLES_SECONDS = 120  # duerme 2 minutos

# Caché diario
CACHE_DIR = "cache"
LIMA_TZ = timezone(timedelta(hours=-5))

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


def fetch_today_unseen(mail):
    """
    Devuelve IDs de correos NO LEÍDOS cuyo día es HOY.
    Gmail interpreta SINCE por fecha (no hora), así que filtra a nivel de día.
    """
    today = datetime.now().strftime("%d-%b-%Y")  # ej "05-Dec-2025"
    status, data = mail.search(None, "UNSEEN", "SINCE", today)

    if status != "OK":
        print("Error al buscar mensajes UNSEEN SINCE hoy:", status)
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


# ---------- CACHÉ DIARIO ----------

def get_today_cache_path():
    today_lima = datetime.now(LIMA_TZ).strftime("%Y%m%d")
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"alerts_cache_{today_lima}.json")


def load_today_cache():
    path = get_today_cache_path()
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data)
    except Exception:
        return set()


def append_cache_key(cache_key: str):
    # Carga, añade y regraba. Para volumen bajo está OK.
    path = get_today_cache_path()
    keys = load_today_cache()
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
        # Primero buscamos text/plain
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

        # Luego intentamos text/html como fallback
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


def parse_subject(subject: str):
    """
    Ejemplo: 'Alarma - IMPACTO - MG069 (308FG25-3)'
    -> tipo = IMPACTO
       vehicleCode = MG069
    """
    tipo = None
    vehicle_code = None

    if not subject:
        return tipo, vehicle_code

    # Separar por '-'
    parts = [p.strip() for p in subject.split("-")]
    if len(parts) >= 3:
        # Alarma | IMPACTO | MG069 (308FG25-3)
        tipo = parts[1].strip()
        third = parts[2].strip()
        # Tomamos el primer token como código
        vehicle_code = third.split()[0]

    return tipo, vehicle_code


def parse_area(body_text: str):
    """
    Busca una línea que contenga 'Area:' o 'Área:' y devuelve el valor.
    """
    area = None
    for line in body_text.splitlines():
        line_clean = line.strip()
        m = re.search(r"Area\s*:\s*(.+)", line_clean, re.IGNORECASE)
        if m:
            area = m.group(1).strip()
            break
    return area


def parse_event_time_from_body(body_text: str, fallback_dt_utc: datetime):
    """
    Intenta leer 'Alarma Fecha: 05-Dic-2025 Hora: 09:42' o 'Fecha: 05-Dic-2025 Hora: 09:42'
    Si no puede, usa la fecha del header (fallback_dt_utc).
    """
    # Buscamos algo tipo '05-Dic-2025' y '09:42'
    # Ojo, los meses vienen en español abreviado.
    pattern = re.compile(
        r"(?:Alarma\s+Fecha|Fecha)\s*:\s*([0-9]{2}-[A-Za-z]{3}-[0-9]{4}).*?Hora\s*:\s*([0-9]{2}:[0-9]{2})",
        re.IGNORECASE | re.DOTALL,
    )

    match = pattern.search(body_text)
    if not match:
        return fallback_dt_utc

    date_str = match.group(1)  # ej "05-Dic-2025"
    time_str = match.group(2)  # ej "09:42"

    # Mapeo meses esp -> número
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
        # Construimos en horario de Lima
        dt_lima = datetime(
            year=int(y),
            month=mon,
            day=int(d),
            hour=int(hh),
            minute=int(mm),
            tzinfo=LIMA_TZ,
        )
        # Lo devolvemos en UTC (o podrías dejar Lima si tu API espera eso)
        return dt_lima.astimezone(timezone.utc)
    except Exception as e:
        print("No se pudo parsear fecha/hora desde el body:", e)
        return fallback_dt_utc


def build_alert_payload(subject: str, body_text: str, msg_dt_utc: datetime) -> dict:
    alert_type, vehicle_code = parse_subject(subject)
    area = parse_area(body_text)
    event_time = parse_event_time_from_body(body_text, msg_dt_utc)

    if not vehicle_code:
        vehicle_code = "UNKNOWN"

    if not alert_type:
        # Como fallback, tratamos de encontrar IMPACTO / EXCESO / etc en el body
        m = re.search(r"(IMPACTO|EXCESO\s+VELOCIDAD|ALARMA)", body_text, re.IGNORECASE)
        if m:
            alert_type = m.group(1).upper()
        else:
            alert_type = "DESCONOCIDO"

    short_description = f"{alert_type} - {vehicle_code}"
    if area:
        short_description += f" - Área: {area}"
    short_description = short_description[:500]

    payload = {
        "vehicleCode": vehicle_code,
        "type": alert_type,
        "subject": subject[:255] if subject else None,
        "area": area,
        "operatorName": None,  # TODO: parsear "Operador:" si lo necesitas
        "operatorId": None,    # TODO: parsear "ID Operador:"
        # Event time en ISO8601
        "eventTime": event_time.isoformat(),
        "shortDescription": short_description,
        "rawPayload": body_text[:5000],  # por si acaso limitas tamaño
    }

    return payload


def send_alert_to_api(payload: dict) -> bool:
    try:
        resp = requests.post(ALERT_ENDPOINT, json=payload, timeout=10)
        if resp.status_code >= 200 and resp.status_code < 300:
            print(f">>> API OK ({resp.status_code}) - alerta registrada")
            return True
        else:
            print(f">>> API ERROR ({resp.status_code}): {resp.text}")
            return False
    except Exception as e:
        print(">>> ERROR llamando a la API:", e)
        return False


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

    # Clave única para caché
    cache_key = message_id or f"{subject}|{msg_dt_utc.isoformat()}"

    if cache_key in processed_keys:
        print(f"Mensaje {msg_id} ya procesado previamente (cache_key={cache_key}). Saltando.")
        return

    print("=" * 60)
    print("Nuevo correo candidato a alerta:")
    print(f"ID IMAP: {msg_id}")
    print(f"Message-ID: {message_id}")
    print(f"De: {from_}")
    print(f"Asunto: {subject}")
    print(f"Fecha header (UTC): {msg_dt_utc}")

    body_text = extract_body_text(msg)

    # Aquí puedes refinar: por ahora buscamos "Alarma" en subject o body.
    text_to_search = (subject or "") + "\n" + (body_text or "")
    if "alarma" not in text_to_search.lower():
        print(">>> No parece un correo de alarma (no contiene 'alarma'). No se envía a la API.")
        return

    # Construimos payload para la API
    payload = build_alert_payload(subject, body_text, msg_dt_utc)

    print("Payload a enviar a la API:")
    print(payload)

    if send_alert_to_api(payload):
        # Solo marcamos en caché si la API respondió OK
        append_cache_key(cache_key)
        processed_keys.add(cache_key)

        # Marcamos como leído para no re-procesar por IMAP
        mail.store(msg_id, "+FLAGS", "\\Seen")
    else:
        print(">>> La API falló, NO se marca como procesado para reintentar luego.")


def check_mail_once():
    now_lima = datetime.now(LIMA_TZ)
    print(f"Chequeando correos a las {now_lima.isoformat()} (hora Lima)")

    processed_keys = load_today_cache()

    print(f"Claves ya procesadas hoy: {len(processed_keys)}")

    mail = connect()
    print("Conectado a Gmail IMAP, buscando UNSEEN de HOY…")

    try:
        unseen_ids = fetch_today_unseen(mail)
        if unseen_ids:
            print(f"Encontrados {len(unseen_ids)} correo(s) NO leídos de hoy.")
            for msg_id in unseen_ids:
                process_message(mail, msg_id, processed_keys)
        else:
            print("Sin correos nuevos (UNSEEN de hoy).")
    finally:
        mail.logout()
        print("Desconectado de IMAP.")


def main():
    while True:
        # ===== Ventana activa =====
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

        # ===== Pausa entre ventanas =====
        print(
            f"Ventana activa terminada. Durmiendo {SLEEP_BETWEEN_CYCLES_SECONDS} s "
            "antes de la próxima ventana…"
        )
        time.sleep(SLEEP_BETWEEN_CYCLES_SECONDS)


if __name__ == "__main__":
    main()
