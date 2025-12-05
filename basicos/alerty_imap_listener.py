import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import time
import os
from datetime import datetime, timedelta, timezone

# =============== CONFIG ===============

# Cambia estas variables a las de TU servidor de correo
IMAP_HOST = os.environ.get("IMAP_HOST", "correo.midominio.com")
IMAP_PORT = int(os.environ.get("IMAP_PORT", "993"))  # 993 IMAPS, 143 IMAP normal

MAIL_USER = os.environ.get("MAIL_USER", "alertas@midominio.com")
MAIL_PASS = os.environ.get("MAIL_PASS", "TU_PASSWORD_AQUI")

IMAP_FOLDER = os.environ.get("IMAP_FOLDER", "INBOX")

LOG_FILE = "alertas_detectadas_local.txt"
WINDOW_MINUTES = 20  # ventana de tiempo para considerar correos "recientes"

# Ventana activa: 1 minuto
WORK_WINDOW_SECONDS = 60

# Cada cuánto revisar dentro de la ventana activa
POLL_INTERVAL_SECONDS = 10

# Descanso entre ventanas activas: 2 minutos
SLEEP_BETWEEN_CYCLES_SECONDS = 120

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
    """
    Conecta al servidor IMAP de tu servidor de correo.
    Si tu servidor NO usa SSL en 993 sino IMAP normal en 143,
    cambia IMAP4_SSL por IMAP4.
    """
    print(f"Conectando a {IMAP_HOST}:{IMAP_PORT} como {MAIL_USER}...")
    if IMAP_PORT == 993:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    else:
        mail = imaplib.IMAP4(IMAP_HOST, IMAP_PORT)

    mail.login(MAIL_USER, MAIL_PASS)
    mail.select(IMAP_FOLDER)
    return mail


def fetch_recent_unseen_messages(mail):
    """
    Busca solo correos NO LEÍDOS (UNSEEN) de HOY en adelante,
    usando IMAP SINCE (por día, no por minutos).
    """
    today = datetime.now().strftime("%d-%b-%Y")  # ej. "05-Dec-2025"
    status, data = mail.search(None, "UNSEEN", "SINCE", today)

    if status != "OK":
        print("Error al buscar mensajes UNSEEN SINCE hoy:", status)
        return []

    ids_raw = data[0].split()
    return ids_raw


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


def append_alert_log(from_str: str, arrival_dt: datetime):
    arrival_utc = arrival_dt.astimezone(timezone.utc)

    # Zona horaria de Lima (UTC-5, sin DST)
    lima_tz = timezone(timedelta(hours=-5))
    arrival_lima = arrival_dt.astimezone(lima_tz)

    utc_str = arrival_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    lima_str = arrival_lima.strftime("%Y-%m-%d %H:%M:%S Lima")

    line = f"alerta - {from_str} - {utc_str} - {lima_str}\n"

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)

    print(f">>> Escrito en {LOG_FILE}: {line.strip()}")


def process_message(mail, msg_id, cutoff_dt: datetime):
    status, msg_data = mail.fetch(msg_id, "(RFC822)")
    if status != "OK":
        print(f"Error al descargar mensaje {msg_id}: {status}")
        return

    msg = email.message_from_bytes(msg_data[0][1])

    subject = decode_maybe(msg.get("Subject"))
    from_ = decode_maybe(msg.get("From"))

    msg_dt = get_message_datetime(msg)

    # Filtrar por ventana de tiempo (últimos WINDOW_MINUTES)
    if msg_dt < cutoff_dt:
        print("=" * 60)
        print(f"Mensaje {msg_id} es más antiguo que la ventana de {WINDOW_MINUTES} minutos. Ignorando.")
        print(f"Fecha del mensaje: {msg_dt}, cutoff: {cutoff_dt}")
        return

    print("=" * 60)
    print("Nuevo correo dentro de ventana de tiempo:")
    print(f"De: {from_}")
    print(f"Asunto: {subject}")
    print(f"Fecha (UTC): {msg_dt}")

    body_text = None
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))
            if content_type == "text/plain" and "attachment" not in content_disposition:
                try:
                    body_text = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8",
                        errors="ignore",
                    )
                    break
                except Exception as e:
                    print("Error decodificando body:", e)
    else:
        try:
            body_text = msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8",
                errors="ignore",
            )
        except Exception as e:
            print("Error decodificando body:", e)

    if body_text:
        preview = "\n".join(body_text.splitlines()[:5])
        print("Body (preview):")
        print(preview)
    else:
        print("Sin cuerpo de texto plano o no se pudo decodificar.")

    text_to_search = (subject or "") + "\n" + (body_text or "")
    if "alerta" in text_to_search.lower():
        print(">>> Palabra 'alerta' detectada. Registrando en txt…")
        append_alert_log(from_, msg_dt)
        # Si quieres marcar como leído SOLO si tiene 'alerta', podrías hacer:
        # mail.store(msg_id, "+FLAGS", "\\Seen")
    else:
        print(">>> No contiene la palabra 'alerta'. No se registra en txt.")


def check_mail_once():
    now_utc = datetime.now(timezone.utc)
    cutoff_dt = now_utc - timedelta(minutes=WINDOW_MINUTES)

    mail = connect()
    print("Conectado. Revisando correos nuevos (UNSEEN de HOY)…")
    print(f"Solo se procesarán correos recibidos después de: {cutoff_dt}")

    try:
        unseen_ids = fetch_recent_unseen_messages(mail)
        if unseen_ids:
            print(f"Encontrados {len(unseen_ids)} correo(s) NO leídos de hoy.")
            for msg_id in unseen_ids:
                process_message(mail, msg_id, cutoff_dt)
                # Aquí puedes decidir si marcarlos como leídos siempre
                mail.store(msg_id, "+FLAGS", "\\Seen")
        else:
            print("Sin correos nuevos (UNSEEN de hoy).")
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
