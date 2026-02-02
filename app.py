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
# CONFIG ADMIN (SEGURANÇA VIA ENV)
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
    "trx-teste": {
        "nome": "TRX TESTE",
        "pasta": "Licencas/TRX TESTE",
        "preco": 100,
        "redirect_url": "https://sites.google.com/view/plano-bronze/in%C3%ADcio"
    }
}
import re

def limpar_telefone(telefone):
    # remove tudo que não for número
    numeros = re.sub(r"\D", "", telefone)

    # se vier com 55 no começo, remove
    if numeros.startswith("55") and len(numeros) > 11:
        numeros = numeros[2:]

    return numeros

# ======================================================
# CHECKOUT DINÂMICO
# ======================================================

def criar_checkout_dinamico(plano_id, order_id, email, telefone, nome):
    plano = PLANOS[plano_id]

    payload = {
        "handle": HANDLE,
        "webhook_url": WEBHOOK_URL,
        "redirect_url": plano["redirect_url"],
        "order_nsu": order_id,

        # 👇 DADOS DO CLIENTE (AUTOFILL INFINITEPAY)
        "customer": {
            "name": nome,
            "email": email,
            "country_code": "55",
            "phone": telefone
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

    email = session.get("email", "")
    telefone = session.get("telefone", "")

    return render_template(
        "checkout.html",
        plano=plano,
        email=email,
        telefone=telefone
    )


@app.route("/comprar", methods=["GET"])
def comprar_get():
    return redirect("/")

@app.route("/comprar", methods=["POST"])
def comprar():
    email = request.form.get("email")
    telefone = request.form.get("telefone")
    plano_id = request.form.get("plano")

    if not email or not telefone or plano_id not in PLANOS:
        return "Dados inválidos", 400

    # salva na sessão (UX)
    session["email"] = email
    session["telefone"] = telefone

    order_id = str(uuid.uuid4())
    salvar_order(order_id, plano_id, email)

    checkout_url = criar_checkout_dinamico(
    plano_id=plano_id,
    order_id=order_id,
    email=email,
    telefone=telefone
)

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
# DASHBOARD ADMIN (TEMPLATES)
# ======================================================

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        senha = request.form.get("senha")
        if senha == ADMIN_PASSWORD:
            session["admin"] = True
            return redirect("/admin/dashboard")
        return "Senha inválida", 403

    return render_template("admin_login.html")


from database import obter_estatisticas

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





