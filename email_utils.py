import base64
import requests
import os

# URL do Google Apps Script (Web App)
GOOGLE_EMAIL_WEBHOOK = "COLE_AQUI_A_URL_DO_SCRIPT"

def enviar_email(destinatario, nome_cliente, nome_plano, arquivo, senha):
    # Lê o ZIP e converte para base64
    with open(arquivo, "rb") as f:
        arquivo_base64 = base64.b64encode(f.read()).decode("utf-8")

    mensagem = f"""Olá {nome_cliente} 👋

Obrigado pela sua compra!

✅ Pagamento confirmado com sucesso.

📦 Plano adquirido: {nome_plano}
🔐 Senha do arquivo: {senha}

📎 O arquivo do seu plano está em anexo neste email.

⚠️ Importante:
- Guarde sua senha
- Não compartilhe o arquivo

Qualquer dúvida, é só responder este email.

Bom uso 🚀
"""

    payload = {
        "email": destinatario,
        "assunto": f"Seu plano {nome_plano} – Acesso Liberado",
        "mensagem": mensagem,
        "filename": os.path.basename(arquivo),
        "file_base64": arquivo_base64
    }

    response = requests.post(GOOGLE_EMAIL_WEBHOOK, json=payload, timeout=60)
    response.raise_for_status()
