import base64
import requests
import os

print("🔥 EMAIL_UTILS CARREGADO")

GOOGLE_EMAIL_WEBHOOK = "https://script.google.com/macros/s/AKfycbzqsLLYy7IfyEIYAyXD7yx8K9A5ojbNeOVyTVSEqLr6Y0dp3I5RgdgYjmeT7UYItkjuXw/exec"


def enviar_email(destinatario, nome_plano, arquivo, senha):
    print("📧 INICIANDO ENVIO DE EMAIL")
    print("➡️ Destinatário:", destinatario)
    print("➡️ Plano:", nome_plano)
    print("➡️ Arquivo:", arquivo)

    if not os.path.exists(arquivo):
        raise Exception("Arquivo não encontrado para envio de email")

    with open(arquivo, "rb") as f:
        arquivo_base64 = base64.b64encode(f.read()).decode("utf-8")

    mensagem = f"""Olá

Obrigado pela sua compra!

✅ Pagamento confirmado com sucesso.

Plano adquirido: {nome_plano}
Senha do arquivo: {senha}

IMPORTANTE — ENTRE NA COMUNIDADE OFICIAL
Para receber avisos, atualizações e suporte, entre no grupo abaixo:

https://chat.whatsapp.com/KPcaKf6OsaQHG2cUPAU1CE

O arquivo do seu plano está em anexo logo abaixo neste email.

⚠️ Importante:
– Guarde sua senha
– Não compartilhe o arquivo

Suporte:
Email: trxtradingpro@gmail.com
WhatsApp: +55 11 98175-9207
WhatsApp 2: +55 11 94043-1906

Bom uso
"""

    payload = {
        "email": destinatario,
        "assunto": f"Seu plano {nome_plano} – Acesso Liberado",
        "mensagem": mensagem,
        "filename": os.path.basename(arquivo),
        "file_base64": arquivo_base64
    }

    response = requests.post(
        GOOGLE_EMAIL_WEBHOOK,
        json=payload,
        timeout=60
    )

    print("📨 RESPOSTA GOOGLE SCRIPT")
    print("Status:", response.status_code)
    print("Body:", response.text)

    if response.status_code != 200:
        raise Exception("Falha ao enviar email via Google Script")
