import base64
import requests
import os

GOOGLE_EMAIL_WEBHOOK = "COLE_AQUI_A_URL_DO_SCRIPT"

def enviar_email(destinatario, nome_plano, arquivo, senha):
    with open(arquivo, "rb") as f:
        arquivo_base64 = base64.b64encode(f.read()).decode("utf-8")

    payload = {
        "email": destinatario,
        "assunto": f"Seu plano {nome_plano} – Acesso Liberado",
        "mensagem": f"""Olá,

Seu pagamento foi confirmado!

Plano: {nome_plano}
Senha do arquivo: {senha}

O arquivo está em anexo.
""",
        "filename": os.path.basename(arquivo),
        "file_base64": arquivo_base64
    }

    requests.post(GOOGLE_EMAIL_WEBHOOK, json=payload, timeout=60)
