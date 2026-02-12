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
import threading
from collections import defaultdict

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
    obter_estatisticas,
    agendar_whatsapp,
    listar_whatsapp_pendentes,
    registrar_falha_whatsapp,
    marcar_whatsapp_enviado
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
# WHATSAPP FOLLOW-UP (PLANO GRÁTIS)
# ======================================================

WHATSAPP_API_URL = os.environ.get("WHATSAPP_API_URL", "").strip()
WHATSAPP_API_TOKEN = os.environ.get("WHATSAPP_API_TOKEN", "").strip()
WHATSAPP_DELAY_MINUTES = int(os.environ.get("WHATSAPP_DELAY_MINUTES", "5"))
WHATSAPP_TEMPLATE = os.environ.get(
    "WHATSAPP_TEMPLATE",
    "Olá {nome}, tudo bem? Vi que você baixou o plano {plano}. "
    "Se quiser ajuda para começar, posso te orientar por aqui."
)

# ======================================================
# PLANOS (COM TESTE + GRÁTIS)
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
        "preco": 110,
        "redirect_url": "https://sites.google.com/view/planogratuito/in%C3%ADcio"
    },
    "trx-gratis": {
        "nome": "TRX GRÁTIS",
        "pasta": "Licencas/TRX GRATIS",
        "preco": 0,
        "gratis": True,
        "redirect_url": "https://sites.google.com/view/planogratuito/in%C3%ADcio"
    }
}

# ======================================================
# UTIL
# ======================================================

def formatar_telefone_infinitepay(telefone):
    numeros = re.sub(r"\D", "", telefone)

    if numeros.startswith("55") and len(numeros) > 11:
        numeros = numeros[2:]

    if len(numeros) != 11:
        raise ValueError("Telefone inválido")

    return f"+55{numeros}"


def formatar_telefone_whatsapp(telefone):
    numeros = re.sub(r"\D", "", telefone or "")

    if not numeros:
        raise ValueError("Telefone vazio")

    if numeros.startswith("55"):
        return numeros

    if len(numeros) in (10, 11):
        return f"55{numeros}"

    raise ValueError("Telefone inválido para WhatsApp")


def enviar_whatsapp_followup(order):
    if not WHATSAPP_API_URL:
        raise RuntimeError("WHATSAPP_API_URL não configurada")

    telefone = formatar_telefone_whatsapp(order.get("telefone"))
    mensagem = WHATSAPP_TEMPLATE.format(
        nome=order.get("nome") or "",
        plano=PLANOS.get(order.get("plano"), {}).get("nome", order.get("plano", ""))
    )

    headers = {"Content-Type": "application/json"}
    if WHATSAPP_API_TOKEN:
        headers["Authorization"] = f"Bearer {WHATSAPP_API_TOKEN}"

    payload = {
        "phone": telefone,
        "message": mensagem,
        "order_id": order["order_id"]
    }

    response = requests.post(WHATSAPP_API_URL, json=payload, headers=headers, timeout=30)
    response.raise_for_status()


def processar_fila_whatsapp():
    pendentes = listar_whatsapp_pendentes(limite=30)

    for order in pendentes:
        tentativas = int(order.get("whatsapp_tentativas") or 0)
        try:
            enviar_whatsapp_followup(order)
            marcar_whatsapp_enviado(order["order_id"])
            print(f"📲 WhatsApp enviado: {order['order_id']}", flush=True)
        except Exception as e:
            registrar_falha_whatsapp(order["order_id"], tentativas + 1, str(e))
            print(f"❌ Falha WhatsApp {order['order_id']}: {e}", flush=True)


def iniciar_worker_whatsapp():
    def worker_loop():
        while True:
            try:
                processar_fila_whatsapp()
            except Exception as e:
                print(f"⚠️ Worker WhatsApp com erro: {e}", flush=True)
            time.sleep(20)

    thread = threading.Thread(target=worker_loop, daemon=True)
    thread.start()


iniciar_worker_whatsapp()

# ======================================================
# CHECKOUT INFINITEPAY
# ======================================================

def criar_checkout_dinamico(plano_id, order_id, nome, email, telefone):
    plano = PLANOS[plano_id]

    payload = {
        "handle": HANDLE,
        "webhook_url": WEBHOOK_URL,
        "redirect_url": plano["redirect_url"],
        "order_nsu": order_id,
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
            registrar_falha_email(order["order_id"], tentativas, str(e))
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

# 🚫 nunca permitir GET em /comprar
@app.route("/comprar", methods=["GET"])
def comprar_get():
    return redirect("/")

# ✅ POST real
@app.route("/comprar", methods=["POST"])
def comprar():
    nome = request.form.get("nome")
    email = request.form.get("email")
    telefone = request.form.get("telefone")
    plano_id = request.form.get("plano")

    if not nome or not email or not telefone or plano_id not in PLANOS:
        return "Dados inválidos", 400

    order_id = str(uuid.uuid4())

    salvar_order(
        order_id=order_id,
        plano=plano_id,
        nome=nome,
        email=email,
        telefone=telefone
    )

    plano_info = PLANOS[plano_id]

    # 🔹 PLANO GRÁTIS → pula checkout
    if plano_info["preco"] <= 0:
        arquivo, senha = compactar_plano(plano_info["pasta"], PASTA_SAIDA)

        enviar_email(
            destinatario=email,
            nome_plano=plano_info["nome"],
            arquivo=arquivo,
            senha=senha
        )

        marcar_order_processada(order_id)
        agendar_whatsapp(order_id, minutos=WHATSAPP_DELAY_MINUTES)
        return redirect(plano_info["redirect_url"])

    checkout_url = criar_checkout_dinamico(
        plano_id=plano_id,
        order_id=order_id,
        nome=nome,
        email=email,
        telefone=telefone
    )

    return redirect(checkout_url)


# ======================================================
# WEBHOOK
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
        arquivo, senha = compactar_plano(plano_info["pasta"], PASTA_SAIDA)
        sucesso = enviar_email_com_retry(order, plano_info, arquivo, senha)
        if sucesso:
            marcar_order_processada(order_id)
            marcar_transacao_processada(transaction_nsu)
    finally:
        if arquivo and os.path.exists(arquivo):
            os.remove(arquivo)

    return jsonify({"msg": "OK"}), 200

# ======================================================
# ADMIN
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


def montar_dashboard_stats(pedidos):
    total_vendas = len(pedidos)
    pagos = [
        pedido
        for pedido in pedidos
        if (pedido.get("status") or "").strip().upper() == "PAGO"
    ]
    pendentes = total_vendas - len(pagos)
    total_faturado = sum(
        PLANOS[pedido["plano"]]["preco"]
        for pedido in pagos
        if pedido["plano"] in PLANOS
    )

    faturamento_por_dia = defaultdict(int)
    for pedido in pagos:
        criado_em = pedido["created_at"]
        if criado_em:
            dia = criado_em.date().isoformat()
            faturamento_por_dia[dia] += PLANOS[pedido["plano"]]["preco"]

    faturamento_labels = sorted(faturamento_por_dia.keys())
    faturamento_values = [faturamento_por_dia[label] for label in faturamento_labels]

    planos_labels = [info["nome"] for info in PLANOS.values()]
    planos_values = [
        sum(
            1
            for pedido in pagos
            if pedido["plano"] == plano_id
        )
        for plano_id in PLANOS.keys()
    ]

    return {
        "total_vendas": total_vendas,
        "total_faturado": total_faturado,
        "pagos": len(pagos),
        "pendentes": pendentes,
        "faturamento_labels": json.dumps(faturamento_labels),
        "faturamento_values": json.dumps(faturamento_values),
        "planos_labels": json.dumps(planos_labels),
        "planos_values": json.dumps(planos_values)
    }


@app.route("/dashboard")
def dashboard():
    if not session.get("admin"):
        return redirect("/admin/login")

    pedidos = listar_pedidos()
    stats = montar_dashboard_stats(pedidos)

    return render_template("dashboard.html", stats=stats)


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



