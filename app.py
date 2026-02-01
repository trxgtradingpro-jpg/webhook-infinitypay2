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
}

CHECKOUT_LINKS = {
    "trx-bronze-0001": "SEU_LINK_BRONZE",
    "trx-prata-0001":  "SEU_LINK_PRATA",
    "trx-gold-0001":   "SEU_LINK_GOLD",
    "trx-black-0001":  "SEU_LINK_BLACK",
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
    # DADOS MOCK (por enquanto)
    pagamentos = [
        {
            "plano": "TRX BRONZE",
            "email": "cliente@email.com",
            "valor": "197,00",
            "metodo": "PIX",
            "data": "01/02/2026"
        }
    ]

    faturamento_total = "197,00"
    pagamentos_hoje = 1
    ticket_medio = "197,00"

    chart_labels = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"]
    chart_values = [197, 394, 197, 591, 394, 788, 591]

    return render_template(
        "pagamentos.html",
        pagamentos=pagamentos,
        faturamento_total=faturamento_total,
        pagamentos_hoje=pagamentos_hoje,
        ticket_medio=ticket_medio,
        chart_labels=chart_labels,
        chart_values=chart_values
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


