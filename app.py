from flask import Flask, request, jsonify
import os

from compactador import compactar_plano
from email_utils import enviar_email

from database import (
    init_db,
    salvar_order_email,
    buscar_email,
    transacao_ja_processada,
    marcar_processada
)

app = Flask(__name__)

# ================= INIT =================

init_db()
PASTA_SAIDA = "saida"
os.makedirs(PASTA_SAIDA, exist_ok=True)

# ================= PLANOS =================

PLANOS = {
    "trx-bronze-0001": {"nome": "TRX BRONZE", "pasta": "Licencas/TRX BRONZE"},
    "trx-prata-0001":  {"nome": "TRX PRATA",  "pasta": "Licencas/TRX PRATA"},
    "trx-gold-0001":   {"nome": "TRX GOLD",   "pasta": "Licencas/TRX GOLD"},
    "trx-black-0001":  {"nome": "TRX BLACK",  "pasta": "Licencas/TRX BLACK"},
    "trx-teste-0001":  {"nome": "TRX BLACK",  "pasta": "Licencas/TRX BLACK"}
}

# ======================================================
# ENDPOINT 1 — SALVAR EMAIL ANTES DO PAGAMENTO
# ======================================================

@app.route("/salvar-email", methods=["POST"])
def salvar_email():
    data = request.get_json(silent=True)

    if not data:
        return jsonify({"erro": "Payload vazio"}), 400

    order_nsu = data.get("order_nsu")
    email = data.get("email")

    if not order_nsu or not email:
        return jsonify({"erro": "order_nsu ou email ausente"}), 400

    salvar_order_email(order_nsu, email)

    print(f"💾 EMAIL SALVO | order_nsu={order_nsu} | email={email}")

    return jsonify({"msg": "Email salvo com sucesso"}), 200


# ======================================================
# ENDPOINT 2 — WEBHOOK INFINITEPAY (PAGAMENTO APROVADO)
# ======================================================

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)

    print("📩 WEBHOOK RECEBIDO")

    if not data:
        print("⚠️ Payload vazio")
        return jsonify({"msg": "Payload vazio"}), 200

    transaction_nsu = data.get("transaction_nsu")
    order_nsu = data.get("order_nsu")
    paid_amount = data.get("paid_amount", 0)

    if not transaction_nsu or not order_nsu:
        print("⚠️ Evento incompleto:", data)
        return jsonify({"msg": "Evento incompleto"}), 200

    if paid_amount <= 0:
        print("⚠️ Pagamento não confirmado:", paid_amount)
        return jsonify({"msg": "Pagamento não confirmado"}), 200

    if transacao_ja_processada(transaction_nsu):
        print("🔁 Transação já processada:", transaction_nsu)
        return jsonify({"msg": "Já processado"}), 200

    if order_nsu not in PLANOS:
        print("❌ Plano inválido:", order_nsu)
        return jsonify({"msg": "Plano inválido"}), 200

    email = buscar_email(order_nsu)

    if not email:
        print("❌ Email não encontrado para order_nsu:", order_nsu)
        return jsonify({"msg": "Email não encontrado"}), 200

    plano = PLANOS[order_nsu]
    arquivo = None

    try:
        # -------- GERAR ARQUIVO --------
        arquivo, senha = compactar_plano(plano["pasta"], PASTA_SAIDA)

        # -------- ENVIAR EMAIL --------
        enviar_email(
            destinatario=email,
            nome_plano=plano["nome"],
            arquivo=arquivo,
            senha=senha
        )

        # -------- MARCAR PROCESSADO --------
        marcar_processada(transaction_nsu)

        print("✅ EMAIL ENVIADO COM SUCESSO")

    except Exception as e:
        print("❌ ERRO NO PROCESSO:", str(e))
        return jsonify({"msg": "Erro interno"}), 500

    finally:
        # -------- LIMPAR ARQUIVO --------
        if arquivo and os.path.exists(arquivo):
            os.remove(arquivo)

    return jsonify({"msg": "OK"}), 200


# ================= START =================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
