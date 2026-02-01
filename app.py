from flask import Flask, request, jsonify, render_template, redirect
import os
import uuid
import json

from compactador import compactar_plano
from email_utils import enviar_email

from database import (
    init_db,
    salvar_order_email,
    buscar_email,
    transacao_ja_processada,
    marcar_processada
)

print("🚀 APP.PY INICIADO", flush=True)

app = Flask(__name__)

# ======================================================
# INIT
# ======================================================

init_db()
print("🗄️ BANCO INICIALIZADO", flush=True)

PASTA_SAIDA = "saida"
os.makedirs(PASTA_SAIDA, exist_ok=True)

# ======================================================
# PLANOS
# ======================================================

PLANOS = {
    "trx-bronze-0001": {"nome": "TRX BRONZE", "pasta": "Licencas/TRX BRONZE"},
    "trx-prata-0001":  {"nome": "TRX PRATA",  "pasta": "Licencas/TRX PRATA"},
    "trx-gold-0001":   {"nome": "TRX GOLD",   "pasta": "Licencas/TRX GOLD"},
    "trx-black-0001":  {"nome": "TRX BLACK",  "pasta": "Licencas/TRX BLACK"},
    "trx-teste-0001":  {"nome": "TRX TESTE",  "pasta": "Licencas/TRX BRONZE"},
}

# ======================================================
# LINKS CHECKOUT INFINITEPAY
# ======================================================

CHECKOUT_LINKS = {
    "trx-bronze-0001": "https://checkout.infinitepay.io/SEU_LINK_BRONZE",
    "trx-prata-0001":  "https://checkout.infinitepay.io/SEU_LINK_PRATA",
    "trx-gold-0001":   "https://checkout.infinitepay.io/SEU_LINK_GOLD",
    "trx-black-0001":  "https://checkout.infinitepay.io/SEU_LINK_BLACK",
    "trx-teste-0001":  "https://checkout.infinitepay.io/SEU_LINK_TESTE",
}

# ======================================================
# CHECKOUT
# ======================================================

@app.route("/checkout/<plano>")
def checkout(plano):
    print(f"🛒 ABRINDO CHECKOUT | plano={plano}", flush=True)

    if plano not in PLANOS:
        return "Plano inválido", 404

    return render_template("checkout.html", plano=plano)


@app.route("/comprar", methods=["POST"])
def comprar():
    print("➡️ /comprar CHAMADO", flush=True)

    email = request.form.get("email")
    telefone = request.form.get("telefone")
    plano = request.form.get("plano")

    print("📩 DADOS RECEBIDOS:", email, telefone, plano, flush=True)

    if not email or not telefone or plano not in PLANOS:
        print("❌ DADOS INVÁLIDOS", flush=True)
        return "Dados inválidos", 400

    reference = f"{plano}-{uuid.uuid4().hex[:10]}"
    print("🔑 REFERENCE GERADO:", reference, flush=True)

    salvar_order_email(reference, email)
    print("💾 SALVO NO BANCO", flush=True)

    checkout_url = f"{CHECKOUT_LINKS[plano]}&reference={reference}"
    print("➡️ REDIRECIONANDO PARA:", checkout_url, flush=True)

    return redirect(checkout_url)

# ======================================================
# WEBHOOK INFINITEPAY
# ======================================================

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    print("\n================ WEBHOOK RECEBIDO ================", flush=True)

    raw = request.data.decode("utf-8", errors="ignore")
    print("🧾 RAW BODY:", raw, flush=True)

    if not raw:
        return jsonify({"msg": "Body vazio"}), 200

    try:
        data = json.loads(raw)
    except Exception as e:
        print("❌ JSON INVÁLIDO:", e, flush=True)
        return jsonify({"msg": "JSON inválido"}), 200

    print("📦 JSON:", data, flush=True)

    transaction_nsu = data.get("transaction_nsu") or data.get("id")
    reference = (
        data.get("reference")
        or data.get("invoice_slug")
        or data.get("order_nsu")
    )
    paid_amount = data.get("paid_amount") or data.get("amount") or 0

    print("🔑 transaction_nsu:", transaction_nsu, flush=True)
    print("🔑 reference:", reference, flush=True)
    print("💰 paid_amount:", paid_amount, flush=True)

    if not transaction_nsu or not reference:
        print("❌ EVENTO INCOMPLETO", flush=True)
        return jsonify({"msg": "Evento incompleto"}), 200

    if float(paid_amount) <= 0:
        print("❌ PAGAMENTO NÃO CONFIRMADO", flush=True)
        return jsonify({"msg": "Pagamento não confirmado"}), 200

    if transacao_ja_processada(transaction_nsu):
        print("🔁 JÁ PROCESSADO", flush=True)
        return jsonify({"msg": "Já processado"}), 200

    plano_id = reference.rsplit("-", 1)[0]
    print("📦 PLANO ID:", plano_id, flush=True)

    if plano_id not in PLANOS:
        print("❌ PLANO INVÁLIDO", flush=True)
        return jsonify({"msg": "Plano inválido"}), 200

    email = buscar_email(reference)
    print("📧 EMAIL BUSCADO:", email, flush=True)

    if not email:
        print("❌ EMAIL NÃO ENCONTRADO", flush=True)
        return jsonify({"msg": "Email não encontrado"}), 200

    plano = PLANOS[plano_id]
    arquivo = None

    try:
        print("📦 GERANDO ARQUIVO", flush=True)
        arquivo, senha = compactar_plano(plano["pasta"], PASTA_SAIDA)

        print("📧 ENVIANDO EMAIL", flush=True)
        enviar_email(
            destinatario=email,
            nome_plano=plano["nome"],
            arquivo=arquivo,
            senha=senha
        )

        marcar_processada(transaction_nsu)
        print("✅ EMAIL ENVIADO COM SUCESSO", flush=True)

    except Exception as e:
        print("❌ ERRO CRÍTICO:", e, flush=True)
        return jsonify({"msg": "Erro interno"}), 500

    finally:
        if arquivo and os.path.exists(arquivo):
            os.remove(arquivo)
            print("🧹 ARQUIVO REMOVIDO", flush=True)

    print("================ FIM WEBHOOK ================\n", flush=True)
    return jsonify({"msg": "OK"}), 200

# ======================================================
# START
# ======================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"🌐 SERVIDOR RODANDO NA PORTA {port}", flush=True)
    app.run(host="0.0.0.0", port=port)
