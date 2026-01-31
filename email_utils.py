import smtplib
import os
from email.message import EmailMessage

EMAIL_REMETENTE = os.getenv("EMAIL_REMETENTE")
SENHA_APP = os.getenv("EMAIL_SENHA_APP")

SMTP_SERVIDOR = "smtp.gmail.com"
SMTP_PORTA = 587

def enviar_email(destinatario, nome_plano, arquivo, senha):
    msg = EmailMessage()
    msg["Subject"] = f"Seu plano {nome_plano} – Acesso Liberado"
    msg["From"] = EMAIL_REMETENTE
    msg["To"] = destinatario

    msg.set_content(f"""
Olá,

Seu pagamento foi confirmado com sucesso!

Plano: {nome_plano}
Senha do arquivo: {senha}
""")

    with open(arquivo, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="octet-stream",
            filename=os.path.basename(arquivo)
        )

    with smtplib.SMTP(SMTP_SERVIDOR, SMTP_PORTA, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_REMETENTE, SENHA_APP)
        smtp.send_message(msg)
