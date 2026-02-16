import os
import re
import threading
import requests

WA_SENDER_URL = os.environ.get("WA_SENDER_URL", "").strip()
WA_SENDER_TOKEN = os.environ.get("WA_SENDER_TOKEN", "").strip()


def normalizar_telefone_wa(phone):
    numeros = re.sub(r"\D", "", str(phone or ""))
    if numeros.startswith("+"):
        numeros = numeros[1:]
    if not numeros.isdigit():
        raise ValueError("telefone deve conter apenas números")
    if len(numeros) < 10 or len(numeros) > 15:
        raise ValueError("telefone fora do tamanho válido")
    return numeros


def send_whatsapp(phone, message, order_id):
    if not WA_SENDER_URL or not WA_SENDER_TOKEN:
        raise RuntimeError("WA_SENDER_URL/WA_SENDER_TOKEN não configurados")

    phone_norm = normalizar_telefone_wa(phone)

    headers = {
        "Authorization": f"Bearer {WA_SENDER_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "phone": phone_norm,
        "message": message,
        "order_id": order_id
    }

    response = requests.post(WA_SENDER_URL, json=payload, headers=headers, timeout=20)
    response.raise_for_status()
    return True


def schedule_whatsapp(phone, message, order_id, delay_minutes, on_success=None, on_failure=None):
    delay_seconds = max(0, int(delay_minutes or 0) * 60)

    def _job():
        try:
            send_whatsapp(phone=phone, message=message, order_id=order_id)
            print(f"📲 WhatsApp auto enviado ({order_id})", flush=True)
            if on_success:
                on_success(order_id)
        except Exception as exc:
            print(f"❌ Falha WhatsApp auto ({order_id}): {exc}", flush=True)
            if on_failure:
                on_failure(order_id, str(exc))

    timer = threading.Timer(delay_seconds, _job)
    timer.daemon = True
    timer.start()
    return timer
