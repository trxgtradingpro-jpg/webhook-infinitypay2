from flask import Flask, request, jsonify, render_template, redirect
import os, json

from compactador import compactar_plano
from email_utils import enviar_email
from database import (
    init_db,
    salvar_order,
    buscar_email_pendente,
    listar_orders,
    transacao_ja_processada,
    marcar_processada
)

print("🚀 APP INICIADO", flush=True)

app = Flask(__name__)
init_db()

PASTA_SAIDA = "saida"
os.makedirs(PASTA_SAIDA, exist_ok=True)

# ================= PLANOS =================

PLANOS = {
    "trx-bronze-0001": {"nome": "TRX BRONZE", "pasta": "Licencas/TRX BRONZE"},
    "trx-prata-0001":  {"nome": "TRX PRATA",  "pasta": "Licencas/TRX PRATA"},
    "trx-gold-0001":   {"nome": "TRX GOLD",   "pasta": "Licencas/TRX GOLD"},
    "trx-black-0001":  {"nome": "TRX BLACK",  "pasta": "Licencas/TRX BLACK"},
    "trx-teste-0001":  {"nome": "TRX BLACK",  "pasta": "Licencas/TRX BLACK"},
}

CHECKOUT_LINKS = {
    "trx-bronze-0001": "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_4AAKwOeFPWKZHaGgwrtbnxQhkv4KiDhArJwQYcaqpRqvPtJldWd-Ka92SNWfqci-iv0AFNIEN450spoppyYt12BDblKb-w3Wh3QRBzoAF3WVbgNnyOMGIo_VHrdXTVr-4mjB9RGT4I3E0xxinog89v3-nnq9k4WI_xqbseh3gitacdu-0yKWBPNv-wrxDnS0kNlKGhD3TRjVv9hQtQ2Qt5HgE9LshJE9Ol6eTcyux5qHtNN5i57nIOaCrc8LnJfDOPcq-fE3GMYrImTAI.v1.a98160e86dffbbf6",
    "trx-prata-0001":  "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_sAYCwOeBPrIb6xgbiDxUfFa9OglERQhU451CSSlkiU6uF05sNEDozSoO1Kn7p5IcwFGOY_2psDSmBMSrjzOggEa-zTA0s55VpBZW1BcOaDHvi73X6oGy4bpOuxS32Gg9APyOr2aRrIokINIhmwq8Zvr78wCdwqdUk_T92a7BQupGOeph9JLFTzZ_4BjoelldjGuABtyS8ef8oyBDU49LMgo_WLHKTp0GXZ8-RXDw7yPK4BhnhvZKgXZmEzFmikqlbUIM3ANA.v1.7267bbf4bfa06f94",
    "trx-gold-0001":   "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_gAQIzDOCYcJweLYqmDiKQKTVzzyzRLMC0q-IFeoDfahHe-bUdgU57yCxvuCoIgDiijQANOs0A3fS5VJFb2j9Y_UVi11Sq74d23M7rpRAmCPDT--0ZDksdBWwRoVBVBh44Cu5P2bb3ukeBOtf965AUwLjXTN9XybhEP1tDmf-bvp6yC2EZAlgU5Ll9UHA1PyqdyRy-pXlPgxlWVzQBF0y80KcOkTPqldhasKpWi81gLxgE.v1.6ff188236f431871",
    "trx-black-0001":  "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_8AAJwHdsOHUA81Ig9NGxGrMCiDClfBOdaKvQgywZqeP3XzQvhlLcFp3xiTA0pgTAos77yGqSGSsu0IbMpTfmHDrjAoLLBMAg04zXIbOyOUEKN2WPo6KLrStGruuuwBaQIMWWz-e7wmiH2nTBzgZYlTgZZ1Flun8UaSmFnY_r1aQ9-ltcxR8zWZB9os_rn_T9kJiY6AYgtyZOHIQJDgLLUMBezMXZazs3NMP8uiViAq8AIBbpAG9ZAbAyyW0ZyMhBE.v1.6236e5c27ab6a662",
    "trx-teste-0001":  "https://checkout.infinitepay.io/guilherme-gomes-v85?lenc=G_wAACwOePNZgFM5YemHyoyWkDN24lKqphA24AAs0lSD6XKTGzm3I2QJ3qNKD3SBDKM75UgjrRWn3_X0bUdgU57yCxtuF4YcaaB13QVZbmO3H0aI0g_b70NCr1KYFWee1lJuZLkBIlXoqPPfZxWObxtpYIBWFBgZWDINbHvf5UkCA7Mx3CicV9FAymZpTqSi_1P_n7ISEh0BxRbksISrCFTTKGwN2HEwe_o-2ipDtaPI2wOCAi_QYTqhkzex0kDSi0yyIQwD.v1.da2465697b6d205b",
}

# ================= CHECKOUT =================

@app.route("/checkout/<plano>")
def checkout(plano):
    print(f"🛒 CHECKOUT ABERTO | plano={plano}", flush=True)
    if plano not in PLANOS:
        return "Plano inválido", 404
    return render_template("checkout.html", plano=plano)


@app.route("/comprar", methods=["POST"])
def comprar():
    print("➡️ /comprar CHAMADO", flush=True)

    email = request.form.get("email")
    telefone = request.form.get("telefone")
    plano = request.form.get("plano")

    if not email or not telefone or plano not in PLANOS:
        return "Dados inválidos", 400

    salvar_order(plano, email, telefone)

    print(f"💾 SALVO | plano={plano} email={email} telefone={telefone}", flush=True)
    return redirect(CHECKOUT_LINKS[plano])

# ================= WEBHOOK =================

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    print("\n================ WEBHOOK ================", flush=True)

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"msg": "Payload vazio"}), 200

    transaction_nsu = data.get("transaction_nsu")
    plano = data.get("order_nsu")
    paid_amount = data.get("paid_amount", 0)

    print("📦 plano:", plano, flush=True)

    if not transaction_nsu or not plano or paid_amount <= 0:
        return jsonify({"msg": "Evento ignorado"}), 200

    if transacao_ja_processada(transaction_nsu):
        return jsonify({"msg": "Já processado"}), 200

    if plano not in PLANOS:
        print("🚫 PLANO NÃO CADASTRADO", flush=True)
        return jsonify({"msg": "Plano inválido"}), 200

    email = buscar_email_pendente(plano)
    if not email:
        return jsonify({"msg": "Email não encontrado"}), 200

    plano_info = PLANOS[plano]
    arquivo = None

    try:
        arquivo, senha = compactar_plano(plano_info["pasta"], PASTA_SAIDA)

        enviar_email(
            destinatario=email,
            nome_plano=plano_info["nome"],
            arquivo=arquivo,
            senha=senha
        )

        marcar_processada(transaction_nsu)
        print("✅ EMAIL ENVIADO", flush=True)

    finally:
        if arquivo and os.path.exists(arquivo):
            os.remove(arquivo)

    print("================ FIM WEBHOOK ================\n", flush=True)
    return jsonify({"msg": "OK"}), 200

# ================= DASHBOARD =================

from datetime import datetime
@app.route("/pagamentos")
def pagamentos():
    transacoes = listar_pagamentos()  # vindo do banco
    return render_template("pagamentos.html", transacoes=transacoes)

@app.route("/relatorios")
def relatorios():
    faturamento_mensal = "5.430,00"
    crescimento = 18
    plano_top = "TRX BRONZE"
    conversao = 92
    melhor_dia = "Sexta-feira"

    dias = [
        {"dia":"Segunda","vendas":5,"valor":"985,00"},
        {"dia":"Terça","vendas":7,"valor":"1.379,00"},
        {"dia":"Quarta","vendas":4,"valor":"788,00"},
        {"dia":"Quinta","vendas":6,"valor":"1.182,00"},
        {"dia":"Sexta","vendas":9,"valor":"1.096,00"},
    ]

    chart_labels = ["Seg","Ter","Qua","Qui","Sex"]
    chart_values = [985,1379,788,1182,1096]

    planos_labels = ["TRX BRONZE","TRX PRATA","TRX GOLD","TRX BLACK"]
    planos_values = [55,20,15,10]

    return render_template(
        "relatorios.html",
        faturamento_mensal=faturamento_mensal,
        crescimento=crescimento,
        plano_top=plano_top,
        conversao=conversao,
        melhor_dia=melhor_dia,
        dias=dias,
        chart_labels=chart_labels,
        chart_values=chart_values,
        planos_labels=planos_labels,
        planos_values=planos_values
    )

@app.route("/orders")
def orders():
    pedidos = listar_orders(200)
    return render_template(
        "orders.html",
        orders=pedidos,
        year=datetime.now().year
    )


# ================= START =================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)







