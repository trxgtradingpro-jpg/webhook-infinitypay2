from flask import Flask, request, jsonify, render_template, redirect
import os
import uuid

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

# ======================================================
# INIT
# ======================================================

init_db()
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
# LINKS CHECKOUT INFINITEPAY (SEUS LINKS REAIS)
# ======================================================

CHECKOUT_LINKS = {
    "trx-bronze-0001": "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_4AAKwOeFPWKZHaGgwrtbnxQhkv4KiDhArJwQYcaqpRqvPtJldWd-Ka92SNWfqci-iv0AFNIEN450spoppyYt12BDblKb-w3Wh3QRBzoAF3WVbgNnyOMGIo_VHrdXTVr-4mjB9RGT4I3E0xxinog89v3-nnq9k4WI_xqbseh3gitacdu-0yKWBPNv-wrxDnS0kNlKGhD3TRjVv9hQtQ2Qt5HgE9LshJE9Ol6eTcyux5qHtNN5i57nIOaCrc8LnJfDOPcq-fE3GMYrImTAI.v1.a98160e86dffbbf6",
    "trx-prata-0001":  "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_sAYCwOeBPrIb6xgbiDxUfFa9OglERQhU451CSSlkiU6uF05sNEDozSoO1Kn7p5IcwFGOY_2psDSmBMSrjzOggEa-zTA0s55VpBZW1BcOaDHvi73X6oGy4bpOuxS32Gg9APyOr2aRrIokINIhmwq8Zvr78wCdwqdUk_T92a7BQupGOeph9JLFTzZ_4BjoelldjGuABtyS8ef8oyBDU49LMgo_WLHKTp0GXZ8-RXDw7yPK4BhnhvZKgXZmEzFmikqlbUIM3ANA.v1.7267bbf4bfa06f94",
    "trx-gold-0001":   "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_gAQIzDOCYcJweLYqmDiKQKTVzzyzRLMC0q-IFeoDfahHe-bUdgU57yCxvuCoIgDiijQANOs0A3fS5VJFb2j9Y_UVi11Sq74d23M7rpRAmCPDT--0ZDksdBWwRoVBVBh44Cu5P2bb3ukeBOtf965AUwLjXTN9XybhEP1tDmf-bvp6yC2EZAlgU5Ll9UHA1PyqdyRy-pXlPgxlWVzQBF0y80KcOkTPqldhasKpWi81gLxgE.v1.6ff188236f431871",
    "trx-black-0001":  "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_8AAJwHdsOHUA81Ig9NGxGrMCiDClfBOdaKvQgywZqeP3XzQvhlLcFp3xiTA0pgTAos77yGqSGSsu0IbMpTfmHDrjAoLLBMAg04zXIbOyOUEKN2WPo6KLrStGruuuwBaQIMWWz-e7wmiH2nTBzgZYlTgZZ1Flun8UaSmFnY_r1aQ9-ltcxR8zWZB9os_rn_T9kJiY6AYgtyZOHIQJDgLLUMBezMXZazs3NMP8uiViAq8AIBbpAG9ZAbAyyW0ZyMhBE.v1.6236e5c27ab6a662",
    "trx-teste-0001":  "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_wAACwOePNZgFM5YemHyoyWkDN24lKqphA24AAs0lSD6XKTGzm3I2QJ3qNKD3SBDKM75UgjrRWn3_X0bUdgU57yCxtuF4YcaaB13QVZbmO3H0aI0g_b70NCr1KYFWee1lJuZLkBIlXoqPPfZxWObxtpYIBWFBgZWDINbHvf5UkCA7Mx3CicV9FAymZpTqSi_1P_n7ISEh0BxRbksISrCFTTKGwN2HEwe_o-2ipDtaPI2wOCAi_QYTqhkzex0kDSi0yyIQwD.v1.da2465697b6d205b",
}

# ======================================================
# CHECKOUT (ANTES DO PAGAMENTO)
# ======================================================

@app.route("/checkout/<plano>")
def checkout(plano):
    if plano not in PLANOS:
        return "Plano inválido", 404
    return render_template("checkout.html", plano=plano)

@app.route("/comprar", methods=["POST"])
def comprar():
    email = request.form.get("email")
    telefone = request.form.get("telefone")
    plano = request.form.get("plano")

    if not email or not telefone or plano not in PLANOS:
        return "Dados inválidos", 400

    # referência interna estável (nossa)
    reference = f"{plano}-{uuid.uuid4().hex[:10]}"

    salvar_order_email(reference, email)

    print(f"🛒 CHECKOUT CRIADO | ref={reference} | email={email} | telefone={telefone}")

    checkout_base = CHECKOUT_LINKS[plano]
    checkout_url = f"{checkout_base}&reference={reference}"

    return redirect(checkout_url)

# ======================================================
# WEBHOOK INFINITEPAY (BLINDADO)
# ======================================================
@app.route("/orders")
def debug_orders():
    import sqlite3

    conn = sqlite3.connect("database.db")
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT * FROM orders ORDER BY created_at DESC")
    rows = c.fetchall()

    conn.close()

    return jsonify([dict(row) for row in rows])

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    print("\n================ WEBHOOK RECEBIDO ================")

    raw = request.data.decode("utf-8", errors="ignore")
    print("🧾 RAW BODY:", raw)

    data = request.get_json(force=True, silent=True)
    print("📦 JSON:", data)

    if not data:
        return jsonify({"msg": "Payload inválido"}), 200

    transaction_nsu = data.get("transaction_nsu") or data.get("id")
    reference = (
        data.get("reference")
        or data.get("invoice_slug")
        or data.get("order_nsu")
    )
    paid_amount = data.get("paid_amount") or data.get("amount") or 0

    print("🔑 transaction_nsu:", transaction_nsu)
    print("🔑 reference:", reference)
    print("💰 paid_amount:", paid_amount)

    if not transaction_nsu or not reference:
        return jsonify({"msg": "Evento incompleto"}), 200

    if float(paid_amount) <= 0:
        return jsonify({"msg": "Pagamento não confirmado"}), 200

    if transacao_ja_processada(transaction_nsu):
        return jsonify({"msg": "Já processado"}), 200

    plano_id = reference.rsplit("-", 1)[0]
    if plano_id not in PLANOS:
        return jsonify({"msg": "Plano inválido"}), 200

    email = buscar_email(reference)
    print("📧 EMAIL:", email)

    if not email:
        return jsonify({"msg": "Email não encontrado"}), 200

    plano = PLANOS[plano_id]
    arquivo = None

    try:
        arquivo, senha = compactar_plano(plano["pasta"], PASTA_SAIDA)

        enviar_email(
            destinatario=email,
            nome_plano=plano["nome"],
            arquivo=arquivo,
            senha=senha
        )

        marcar_processada(transaction_nsu)
        print("✅ EMAIL ENVIADO")

    except Exception as e:
        print("❌ ERRO:", str(e))
        return jsonify({"msg": "Erro interno"}), 500

    finally:
        if arquivo and os.path.exists(arquivo):
            os.remove(arquivo)

    print("================ FIM WEBHOOK ================\n")
    return jsonify({"msg": "OK"}), 200

# ======================================================
# START
# ======================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)


