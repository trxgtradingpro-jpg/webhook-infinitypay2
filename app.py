from flask import (
    Flask, request, jsonify, render_template,
    redirect, session
)
import os
import json
import uuid
import requests
import time

from compactador import compactar_plano
from email_utils import enviar_email

from database import (
    init_db,
    salvar_order,
    buscar_order_por_id,
    marcar_order_processada,
    registrar_falha_email,
    transacao_ja_processada,
    marcar_transacao_processada,
    listar_pedidos,
    buscar_pedido_detalhado
)

print("🚀 APP INICIADO", flush=True)

# ======================================================
# APP
# ======================================================

app = Flask(__name__)

# ======================================================
# CONFIG ADMIN (SOMENTE VARIÁVEIS DE AMBIENTE)
# ======================================================
# ⚠️ Se essas variáveis não existirem, o app NÃO SOBE (correto em produção)

app.secret_key = os.environ["ADMIN_SECRET"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]

# ======================================================
# INIT
# ======================================================

init_db()
PASTA_SAIDA = "saida"
os.makedirs(PASTA_SAIDA, exist_ok=True)

# ======================================================
# INFINITEPAY CONFIG
# ======================================================

INFINITEPAY_URL = "https://api.infinitepay.io/invoices/public/checkout/links"
HANDLE = "guilherme-gomes-v85"
WEBHOOK_URL = "https://webhook-infinitypay.onrender.com/webhook/infinitypay"

# ======================================================
# PLANOS
# ======================================================

PLANOS = {
    "trx-bronze": {
        "nome": "TRX BRONZE",
        "pasta": "Licencas/TRX BRONZE",
        "preco": 19700,
        "redirect_url": "https://sites.google.com/view/plano-bronze/in%C3%ADcio"
    },
    "trx-prata": {
        "nome": "TRX PRATA",
        "pasta": "Licencas/TRX PRATA",
        "preco": 25000,
        "redirect_url": "https://sites.google.com/view/plano-prata/in%C3%ADcio"
    },
    "trx-gold": {
        "nome": "TRX GOLD",
        "pasta": "Licencas/TRX GOLD",
        "preco": 49900,
        "redirect_url": "https://sites.google.com/view/plano-gold/in%C3%ADcio"
    },
    "trx-black": {
        "nome": "TRX BLACK",
        "pasta": "Licencas/TRX BLACK",
        "preco": 70000,
        "redirect_url": "https://sites.google.com/view/plano-ilimitado/in%C3%ADcio"
    },
    "trx-teste": {
        "nome": "TRX TESTE",
        "pasta": "Licencas/TRX TESTE",
        "preco": 100,
        "redirect_url": "https://sites.google.com/view/plano-bronze/in%C3%ADcio"
    }
}

# ======================================================
# CHECKOUT DINÂMICO
# ======================================================

def criar_checkout_dinamico(plano_id, order_id):
    plano = PLANOS[plano_id]

    payload = {
        "handle": HANDLE,
        "webhook_url": WEBHOOK_URL,
        "redirect_url": plano["redirect_url"],
        "order_nsu": order_id,
        "items": [
            {
                "description": plano["nome"],
                "quantity": 1,
                "price": plano["preco"]
            }
        ]
    }

    r = requests.post(INFINITEPAY_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["url"]

# ======================================================
# EMAIL COM RETRY AUTOMÁTICO
# ======================================================

MAX_TENTATIVAS_EMAIL = 3

def enviar_email_com_retry(order, plano_info, arquivo, senha):
    tentativas = order["email_tentativas"]

    while tentativas < MAX_TENTATIVAS_EMAIL:
        try:
            enviar_email(
                destinatario=order["email"],
                nome_plano=plano_info["nome"],
                arquivo=arquivo,
                senha=senha
            )
            return True

        except Exception as e:
            tentativas += 1
            print(f"❌ Falha email tentativa {tentativas}: {e}", flush=True)

            registrar_falha_email(
                order_id=order["order_id"],
                tentativas=tentativas,
                erro=str(e)
            )

            time.sleep(5)

    return False

# ======================================================
# ROTAS PÚBLICAS
# ======================================================

@app.route("/")
def home():
    return redirect("/checkout/trx-bronze")


@app.route("/checkout/<plano>")
def checkout(plano):
    if plano not in PLANOS:
        return "Plano inválido", 404
    return render_template("checkout.html", plano=plano)


@app.route("/comprar", methods=["POST"])
def comprar():
    email = request.form.get("email")
    plano_id = request.form.get("plano")

    if not email or plano_id not in PLANOS:
        return "Dados inválidos", 400

    order_id = str(uuid.uuid4())
    salvar_order(order_id, plano_id, email)

    checkout_url = criar_checkout_dinamico(plano_id, order_id)
    print(f"🧾 PEDIDO {order_id} criado para {email}", flush=True)

    return redirect(checkout_url)

# ======================================================
# WEBHOOK INFINITEPAY
# ======================================================

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    raw = request.data.decode("utf-8", errors="ignore")
    print("🧾 WEBHOOK:", raw, flush=True)

    if not raw:
        return jsonify({"msg": "Body vazio"}), 200

    data = json.loads(raw)

    transaction_nsu = data.get("transaction_nsu")
    order_id = data.get("order_nsu")
    paid_amount = data.get("paid_amount", 0)

    if not transaction_nsu or not order_id:
        return jsonify({"msg": "Evento incompleto"}), 200

    if paid_amount <= 0:
        return jsonify({"msg": "Pagamento não confirmado"}), 200

    if transacao_ja_processada(transaction_nsu):
        return jsonify({"msg": "Já processado"}), 200

    order = buscar_order_por_id(order_id)
    if not order or order["status"] != "PENDENTE":
        return jsonify({"msg": "Pedido inválido"}), 200

    plano_info = PLANOS[order["plano"]]
    arquivo = None

    try:
        arquivo, senha = compactar_plano(plano_info["pasta"], PASTA_SAIDA)

        sucesso = enviar_email_com_retry(
            order=order,
            plano_info=plano_info,
            arquivo=arquivo,
            senha=senha
        )

        if sucesso:
            marcar_order_processada(order_id)
            marcar_transacao_processada(transaction_nsu)
            print("✅ EMAIL ENVIADO COM SUCESSO", flush=True)
        else:
            print("🚨 EMAIL FALHOU APÓS TODAS AS TENTATIVAS", flush=True)

    finally:
        if arquivo and os.path.exists(arquivo):
            os.remove(arquivo)

    return jsonify({"msg": "OK"}), 200

# ======================================================
# DASHBOARD ADMIN (PROTEGIDO)
# ======================================================

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        senha = request.form.get("senha")
        if senha == ADMIN_PASSWORD:
            session["admin"] = True
            return redirect("/admin/dashboard")
        return "Senha inválida", 403

    return """
    <h2>Login Admin</h2>
    <form method="post">
        <input type="password" name="senha" placeholder="Senha">
        <button>Entrar</button>
    </form>
    """


@app.route("/admin/dashboard")
def admin_dashboard():
    if not session.get("admin"):
        return redirect("/admin/login")

    pedidos = listar_pedidos()

    html = "<h2>Pedidos</h2><table border=1 cellpadding=6>"
    html += "<tr><th>Order</th><th>Email</th><th>Plano</th><th>Status</th><th>Tentativas</th><th>Data</th></tr>"

    for p in pedidos:
        html += f"""
        <tr>
            <td><a href="/admin/pedido/{p['order_id']}">{p['order_id']}</a></td>
            <td>{p['email']}</td>
            <td>{p['plano']}</td>
            <td>{p['status']}</td>
            <td>{p['email_tentativas']}</td>
            <td>{p['created_at']}</td>
        </tr>
        """

    html += "</table>"
    return html


@app.route("/admin/pedido/<order_id>")
def admin_pedido(order_id):
    if not session.get("admin"):
        return redirect("/admin/login")

    pedido = buscar_pedido_detalhado(order_id)
    if not pedido:
        return "Pedido não encontrado", 404

    return f"""
    <h2>Pedido {pedido['order_id']}</h2>
    <p><b>Email:</b> {pedido['email']}</p>
    <p><b>Plano:</b> {pedido['plano']}</p>
    <p><b>Status:</b> {pedido['status']}</p>
    <p><b>Tentativas Email:</b> {pedido['email_tentativas']}</p>
    <p><b>Último erro:</b> {pedido['ultimo_erro']}</p>
    <p><b>Data:</b> {pedido['created_at']}</p>
    <a href="/admin/dashboard">← Voltar</a>
    """

# ======================================================
# START
# ======================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
