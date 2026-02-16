import base64
import os

import requests

print("EMAIL_UTILS CARREGADO")

GOOGLE_EMAIL_WEBHOOK = os.environ.get(
    "GOOGLE_EMAIL_WEBHOOK",
    "https://script.google.com/macros/s/AKfycbzqsLLYy7IfyEIYAyXD7yx8K9A5ojbNeOVyTVSEqLr6Y0dp3I5RgdgYjmeT7UYItkjuXw/exec",
).strip()


def _enviar_payload_email(payload):
    if not GOOGLE_EMAIL_WEBHOOK:
        raise RuntimeError("GOOGLE_EMAIL_WEBHOOK nao configurado.")

    response = requests.post(
        GOOGLE_EMAIL_WEBHOOK,
        json=payload,
        timeout=60,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Falha no envio de email. Status={response.status_code} Body={response.text}")


def _arquivo_para_base64(caminho_arquivo):
    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo nao encontrado: {caminho_arquivo}")

    with open(caminho_arquivo, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def enviar_email_com_anexo(destinatario, assunto, mensagem, caminho_arquivo):
    arquivo_base64 = _arquivo_para_base64(caminho_arquivo)
    payload = {
        "email": destinatario,
        "assunto": assunto,
        "mensagem": mensagem,
        "filename": os.path.basename(caminho_arquivo),
        "file_base64": arquivo_base64,
    }
    _enviar_payload_email(payload)


def enviar_email_simples(destinatario, assunto, mensagem, html=None):
    payload = {
        "email": destinatario,
        "assunto": assunto,
        "mensagem": mensagem,
    }
    if html:
        payload["html"] = html
    _enviar_payload_email(payload)


def enviar_email(destinatario, nome_plano, arquivo, senha):
    mensagem = f"""Ola

Obrigado pela sua compra!

Pagamento confirmado com sucesso.

Plano adquirido: {nome_plano}
Senha do arquivo: {senha}

(ASSISTA AGORA)
Tutorial de como baixar, descompactar e instalar o robo:
https://youtu.be/u3GWhwR8bcQ?si=3mb8yraHc_KKruFF

IMPORTANTE - ENTRE NA COMUNIDADE OFICIAL
Para receber avisos, atualizacoes e suporte, entre no grupo abaixo:
https://chat.whatsapp.com/KPcaKf6OsaQHG2cUPAU1CE

O arquivo do seu plano esta em anexo neste email.

Importante:
- Guarde sua senha
- Nao compartilhe o arquivo

Suporte:
Email: trxtradingpro@gmail.com
WhatsApp: +55 11 94043-1906
WhatsApp 2: +55 11 98175-9207

Bom uso
"""
    assunto = f"Seu plano {nome_plano} - Acesso Liberado"
    enviar_email_com_anexo(
        destinatario=destinatario,
        assunto=assunto,
        mensagem=mensagem,
        caminho_arquivo=arquivo,
    )
