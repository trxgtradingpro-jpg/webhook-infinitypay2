from flask import (
    Flask, request, jsonify, render_template,
    redirect, session
)
import os
import json
import uuid
import requests
import time
import re

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
    buscar_pedido_detalhado,
    obter_estatisticas
)

print("🚀 APP INICIADO", flush=True)

# ======================================================
# APP
# ======================================================

app = Flask(__name__)

# ======================================================
# SEGURANÇA (ENV)
# ======================================================

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
    "trx-gratis": {
    "nome": "TRX GRATIS",
    "pasta": "Licencas/TRX GRATIS",
    "preco": 0,
    "gratis": True,
    "redirect_url": "https://sites.google.com/view/planogratuito/in%C3%ADcio"
},

    "trx-teste": {
        "nome": "TRX TESTE",
        "pasta": "Licencas/TRX TESTE",
        "preco": 100,
        "redirect_url": "https://sites.google.com/view/planogratuito/in%C3%ADcio"
    }
}

# ======================================================
# UTIL
# ======================================================

def formatar_telefone_infinitepay(telefone):
    """
    Converte qualquer entrada para:
    +5511999999999
    """
    numeros = re.sub(r"\D", "", telefone)

    if numeros.startswith("55") and len(numeros) > 11:
        numeros = numeros[2:]

    if len(numeros) != 11:
        raise ValueError("Telefone inválido")

    return f"+55{numeros}"

# ======================================================
# CHECKOUT DINÂMICO (INFINITEPAY)
# ======================================================

def criar_checkout_dinamico(plano_id, order_id, nome, email, telefone):
    plano = PLANOS[plano_id]

    payload = {
        "handle": HANDLE,
        "webhook_url": WEBHOOK_URL,
        "redirect_url": plano["redirect_url"],
        "order_nsu": order_id,

        # 🔥 FORMATO OFICIAL DA DOC INFINITEPAY
        "customer": {
            "name": nome,
            "email": email,
            "phone_number": formatar_telefone_infinitepay(telefone)
        },

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
# EMAIL COM RETRY
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
            registrar_falha_email(
                order["order_id"],
                tentativas,
                str(e)
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

    return render_template(
        "checkout.html",
        plano=plano,
        nome=session.get("nome", ""),
        email=session.get("email", ""),
        telefone=session.get("telefone", "")
    )


@app.route("/comprar", methods=["POST"])
def comprar():
    nome = request.form.get("nome")
    email = request.form.get("email")
    telefone_raw = request.form.get("telefone")
    plano_id = request.form.get("plano")

    if not nome or not email or not telefone_raw or plano_id not in PLANOS:
        return "Dados inválidos", 400

    try:
        telefone_formatado = formatar_telefone_infinitepay(telefone_raw)
    except ValueError:
        return "Telefone inválido. Use DDD + número.", 400

    # salva para autofill
    session["nome"] = nome
    session["email"] = email
    session["telefone"] = telefone_raw

    order_id = str(uuid.uuid4())
    salvar_order(
    order_id=order_id,
    plano=plano_id,
    nome=nome,
    email=email,
    telefone=telefone_raw
)


    plano = PLANOS[plano_id]

    # ======================================================
    # 🚀 FLUXO PLANO GRÁTIS (SEM CHECKOUT)
    # ======================================================
    if plano.get("gratis") is True or plano["preco"] <= 0:
        print(f"🎁 PLANO GRÁTIS | {order_id}", flush=True)

        arquivo = None
        try:
            arquivo, senha = compactar_plano(
                plano["pasta"],
                PASTA_SAIDA
            )

            enviar_email(
                destinatario=email,
                nome_plano=plano["nome"],
                arquivo=arquivo,
                senha=senha
            )

            marcar_order_processada(order_id)

        finally:
            if arquivo and os.path.exists(arquivo):
                os.remove(arquivo)

        # redireciona direto para a página final
        return redirect(plano["redirect_url"])

    # ======================================================
    # 💳 FLUXO PLANO PAGO (INFINITEPAY)
    # ======================================================
    checkout_url = criar_checkout_dinamico(
        plano_id=plano_id,
        order_id=order_id,
        nome=nome,
        email=email,
        telefone=telefone_raw
    )

    print(f"🧾 PEDIDO {order_id} criado para {email}", flush=True)
    return redirect(checkout_url)


# ======================================================
# WEBHOOK INFINITEPAY
# ======================================================

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    data = json.loads(request.data.decode("utf-8", errors="ignore"))

    transaction_nsu = data.get("transaction_nsu")
    order_id = data.get("order_nsu")
    paid_amount = data.get("paid_amount", 0)

    if not transaction_nsu or not order_id or paid_amount <= 0:
        return jsonify({"msg": "Ignorado"}), 200

    if transacao_ja_processada(transaction_nsu):
        return jsonify({"msg": "Já processado"}), 200

    order = buscar_order_por_id(order_id)
    if not order or order["status"] != "PENDENTE":
        return jsonify({"msg": "Pedido inválido"}), 200

    plano_info = PLANOS[order["plano"]]
    arquivo = None

    try:
        arquivo, senha = compactar_plano(
            plano_info["pasta"],
            PASTA_SAIDA
        )

        sucesso = enviar_email_com_retry(
            order,
            plano_info,
            arquivo,
            senha
        )

        if sucesso:
            marcar_order_processada(order_id)
            marcar_transacao_processada(transaction_nsu)

    finally:
        if arquivo and os.path.exists(arquivo):
            os.remove(arquivo)

    return jsonify({"msg": "OK"}), 200

# ======================================================
# ADMIN / DASHBOARD
# ======================================================

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        if request.form.get("senha") == ADMIN_PASSWORD:
            session["admin"] = True
            return redirect("/admin/dashboard")
        return "Senha inválida", 403

    return render_template("admin_login.html")


@app.route("/admin/dashboard")
def admin_dashboard():
    if not session.get("admin"):
        return redirect("/admin/login")

    pedidos = listar_pedidos()
    stats = obter_estatisticas()

    return render_template(
        "admin_dashboard.html",
        pedidos=pedidos,
        stats=stats
    )


@app.route("/admin/pedido/<order_id>")
def admin_pedido(order_id):
    if not session.get("admin"):
        return redirect("/admin/login")

    pedido = buscar_pedido_detalhado(order_id)
    if not pedido:
        return "Pedido não encontrado", 404

    return render_template("admin_pedido.html", pedido=pedido)

# ======================================================
# START
# ======================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)




