from flask import (
    Flask, request, jsonify, render_template,
    redirect, session
)
import os
import json
import uuid
import requests
import time
from datetime import datetime, timedelta, timezone
import math
from urllib.parse import quote
import re
import threading
from zoneinfo import ZoneInfo
from collections import defaultdict

from compactador import compactar_plano
from email_utils import enviar_email
from whatsapp_sender import schedule_whatsapp

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
    incrementar_whatsapp_enviado,
    excluir_order,
    excluir_duplicados_por_dados,
    registrar_evento_compra_analytics,
    buscar_user_plan_stats,
    listar_eventos_analytics,
    backfill_analytics_from_orders,
    registrar_whatsapp_auto_agendamento,
    marcar_whatsapp_auto_enviado,
    registrar_falha_whatsapp_auto
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

WHATSAPP_MENSAGEM = os.environ.get(
    "WHATSAPP_MENSAGEM",
    (
        "Olá {nome}\n\n"
        "Seu TRX {plano} foi liberado com sucesso ✅\n\n"
        "Quero confirmar se conseguiu instalar corretamente.\n"
        "Caso tenha qualquer dúvida ou dificuldade, é só me chamar que te dou suporte imediato 🤝\n\n"
        "Lembre-se de entrar na nossa comunidade para receber atualizações do nosso robô:\n"
        "https://chat.whatsapp.com/KPcaKf6OsaQHG2cUPAU1CE\n\n"
        "Estou à disposição."
    )
)
WHATSAPP_TEMPLATE = os.environ.get(
    "WHATSAPP_TEMPLATE",
    "✅ {nome}, seu pagamento do {plano} foi confirmado. Qualquer dúvida pode me chamar!"
)
WA_SENDER_URL = os.environ.get("WA_SENDER_URL", "").strip()
WA_SENDER_TOKEN = os.environ.get("WA_SENDER_TOKEN", "").strip()
WHATSAPP_DELAY_MINUTES = int(os.environ.get("WHATSAPP_DELAY_MINUTES", "5"))
WHATSAPP_AUTO_SEND = os.environ.get("WHATSAPP_AUTO_SEND", "true").strip().lower() == "true"
WHATSAPP_GRAPH_VERSION = os.environ.get("WHATSAPP_GRAPH_VERSION", "v21.0").strip()
WHATSAPP_PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID", "").strip()
WHATSAPP_ACCESS_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN", "").strip()

ADMIN_TIMEZONE = os.environ.get("ADMIN_TIMEZONE", "America/Sao_Paulo").strip()
ONLINE_TTL_SECONDS = int(os.environ.get("ONLINE_TTL_SECONDS", "90"))
_online_sessions = {}
_online_lock = threading.Lock()

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
        "preco": 24700,
        "redirect_url": "https://sites.google.com/view/plano-prata/in%C3%ADcio"
    },
    "trx-gold": {
        "nome": "TRX GOLD",
        "pasta": "Licencas/TRX GOLD",
        "preco": 49700,
        "redirect_url": "https://sites.google.com/view/plano-gold/in%C3%ADcio"
    },
    "trx-black": {
        "nome": "TRX BLACK",
        "pasta": "Licencas/TRX BLACK",
        "preco": 69700,
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

backfill_analytics_from_orders({
    plano_id: int(info.get("preco") or 0)
    for plano_id, info in PLANOS.items()
})

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


def gerar_link_whatsapp(order):
    telefone_usuario = order.get("telefone")
    if not telefone_usuario:
        return None

    try:
        numero = formatar_telefone_whatsapp(telefone_usuario)
    except ValueError:
        return None

    mensagem = WHATSAPP_MENSAGEM.format(
        nome=order.get("nome") or "",
        plano=PLANOS.get(order.get("plano"), {}).get("nome", order.get("plano", ""))
    )
    return f"https://wa.me/{numero}?text={quote(mensagem)}"


def enviar_whatsapp_automatico(order):
    if not WHATSAPP_AUTO_SEND:
        raise RuntimeError("WHATSAPP_AUTO_SEND=false")

    if not WHATSAPP_PHONE_NUMBER_ID or not WHATSAPP_ACCESS_TOKEN:
        raise RuntimeError(
            "Configure WHATSAPP_PHONE_NUMBER_ID e WHATSAPP_ACCESS_TOKEN para envio automático"
        )

    numero_destino = formatar_telefone_whatsapp(order.get("telefone"))
    mensagem = WHATSAPP_MENSAGEM.format(
        nome=order.get("nome") or "",
        plano=PLANOS.get(order.get("plano"), {}).get("nome", order.get("plano", ""))
    )

    url = (
        f"https://graph.facebook.com/{WHATSAPP_GRAPH_VERSION}/"
        f"{WHATSAPP_PHONE_NUMBER_ID}/messages"
    )
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": numero_destino,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": mensagem
        }
    }

    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()


MAX_TENTATIVAS_WHATSAPP = 3


def processar_fila_whatsapp():
    pedidos = listar_whatsapp_pendentes(limite=30)

    for pedido in pedidos:
        tentativas = int(pedido.get("whatsapp_tentativas") or 0)
        if tentativas >= MAX_TENTATIVAS_WHATSAPP:
            continue

        try:
            enviar_whatsapp_automatico(pedido)
            incrementar_whatsapp_enviado(pedido["order_id"])
            print(f"📲 WhatsApp automático enviado: {pedido['order_id']}", flush=True)
        except Exception as e:
            registrar_falha_whatsapp(
                pedido["order_id"],
                tentativas + 1,
                str(e)
            )
            print(f"❌ Falha WhatsApp automático {pedido['order_id']}: {e}", flush=True)


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


def pedido_liberado_para_whatsapp(order):
    if order.get("plano") != "trx-gratis" or order.get("status") != "PAGO":
        return False

    agendado = order.get("whatsapp_agendado_para")
    if agendado is None:
        criado_em = order.get("created_at")
        if criado_em is None:
            return True
        agendado = criado_em + timedelta(minutes=WHATSAPP_DELAY_MINUTES)

    agora = datetime.now(agendado.tzinfo) if getattr(agendado, "tzinfo", None) else datetime.now()
    return agendado <= agora


iniciar_worker_whatsapp()


def chave_duplicidade_pedido(order):
    nome = (order.get("nome") or "").strip().lower()
    email = (order.get("email") or "").strip().lower()
    telefone = re.sub(r"\D", "", order.get("telefone") or "")
    return (nome, email, telefone)


def calcular_contagem_regressiva_30_dias(order):
    criado_em = order.get("created_at")
    if not criado_em:
        return {
            "dias_restantes_30": None,
            "alerta_30_dias": ""
        }

    agora = datetime.now(criado_em.tzinfo) if getattr(criado_em, "tzinfo", None) else datetime.now()
    limite = criado_em + timedelta(days=30)
    segundos = (limite - agora).total_seconds()
    dias_restantes = math.ceil(segundos / 86400)

    alerta = ""
    if dias_restantes in (5, 3):
        alerta = f"⚠ Faltam {dias_restantes} dias para completar 30 dias"

    return {
        "dias_restantes_30": dias_restantes,
        "alerta_30_dias": alerta
    }


def montar_mensagem_whatsapp_pos_pago(order):
    nome = (order.get("nome") or "cliente").strip() or "cliente"
    plano_nome = PLANOS.get(order.get("plano"), {}).get("nome", order.get("plano", "plano"))
    return WHATSAPP_TEMPLATE.format(nome=nome, plano=plano_nome)


def agendar_whatsapp_pos_pago(order):
    order_id = order.get("order_id")
    telefone = order.get("telefone")

    if not order_id or not telefone:
        return

    if not WA_SENDER_URL or not WA_SENDER_TOKEN:
        print(f"⚠️ WhatsApp sender não configurado; ignorando pedido {order_id}", flush=True)
        return

    agendado = registrar_whatsapp_auto_agendamento(order_id, delay_minutes=WHATSAPP_DELAY_MINUTES)
    if not agendado:
        print(f"ℹ️ WhatsApp já agendado/enviado para {order_id}", flush=True)
        return

    mensagem = montar_mensagem_whatsapp_pos_pago(order)
    print(f"✅ Pagamento confirmado; agendando WhatsApp para {order_id} em {WHATSAPP_DELAY_MINUTES} min", flush=True)

    schedule_whatsapp(
        phone=telefone,
        message=mensagem,
        order_id=order_id,
        delay_minutes=WHATSAPP_DELAY_MINUTES,
        on_success=marcar_whatsapp_auto_enviado,
        on_failure=registrar_falha_whatsapp_auto
    )


def converter_data_para_timezone_admin(dt):
    if not dt:
        return None

    try:
        tz_admin = ZoneInfo(ADMIN_TIMEZONE)
    except Exception:
        tz_admin = ZoneInfo("America/Sao_Paulo")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(tz_admin)


def obter_user_key(order):
    email = (order.get("email") or "").strip().lower()
    if email:
        return email

    telefone = re.sub(r"\D", "", order.get("telefone") or "")
    if telefone:
        return telefone

    return (order.get("order_id") or "").strip()


def registrar_compra_analytics(order, transaction_nsu=None):
    if not order:
        return False

    status_pago = (order.get("status") or "").upper() == "PAGO"
    if not status_pago:
        return False

    plano = order.get("plano")
    if plano not in PLANOS:
        return False

    user_key = obter_user_key(order)
    if not user_key:
        return False

    amount_centavos = int(PLANOS.get(plano, {}).get("preco") or 0)
    return registrar_evento_compra_analytics(
        order_id=order.get("order_id"),
        user_key=user_key,
        plano=plano,
        is_paid=amount_centavos > 0,
        amount_centavos=amount_centavos,
        transaction_nsu=transaction_nsu,
        created_at=order.get("created_at")
    )


def parse_iso_date(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def fmt_brl_from_centavos(valor):
    return f"R$ {valor / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def agrupar_periodo(data_obj, group_by):
    if group_by == "week":
        iso = data_obj.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    if group_by == "month":
        return data_obj.strftime("%Y-%m")
    return data_obj.isoformat()


def carregar_eventos_analytics_filtrados(start=None, end=None, plano='all'):
    start_dt = datetime.combine(start, datetime.min.time()) if start else None
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time()) if end else None
    return listar_eventos_analytics(start_date=start_dt, end_date=end_dt, plano=plano)


def identificador_online_request():
    return f"{request.remote_addr}:{request.headers.get('User-Agent', '')[:40]}"


def registrar_usuario_online():
    identificador = identificador_online_request()
    agora = time.time()

    with _online_lock:
        _online_sessions[identificador] = agora
        limite = agora - ONLINE_TTL_SECONDS
        expirados = [k for k, ts in _online_sessions.items() if ts < limite]
        for k in expirados:
            _online_sessions.pop(k, None)


def total_usuarios_online(excluir_request_atual=False):
    agora = time.time()
    with _online_lock:
        limite = agora - ONLINE_TTL_SECONDS
        expirados = [k for k, ts in _online_sessions.items() if ts < limite]
        for k in expirados:
            _online_sessions.pop(k, None)

        total = len(_online_sessions)
        if excluir_request_atual:
            atual = identificador_online_request()
            if atual in _online_sessions:
                total -= 1

        return max(0, total)


@app.route('/online/ping', methods=['POST'])
def online_ping():
    registrar_usuario_online()
    return jsonify({"ok": True})


@app.route('/admin/online-count')
def admin_online_count():
    if not session.get("admin"):
        return jsonify({"online": 0}), 403

    return jsonify({"online": total_usuarios_online(excluir_request_atual=True)})


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
    registrar_usuario_online()
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
        order_pago = buscar_order_por_id(order_id)
        registrar_compra_analytics(order_pago)
        agendar_whatsapp(order_id, minutos=WHATSAPP_DELAY_MINUTES)
        agendar_whatsapp_pos_pago(order_pago)
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
            order_pago = buscar_order_por_id(order_id)
            registrar_compra_analytics(order_pago, transaction_nsu=transaction_nsu)
            agendar_whatsapp_pos_pago(order_pago)
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

    busca = (request.args.get("q") or "").strip().lower()
    filtro_plano = (request.args.get("plano") or "todos").strip().lower()

    pedidos = listar_pedidos()

    grupos_duplicados = defaultdict(list)
    for pedido in pedidos:
        grupos_duplicados[chave_duplicidade_pedido(pedido)].append(pedido["order_id"])

    duplicados_grupos_count = 0
    duplicados_registros_count = 0
    for ids in grupos_duplicados.values():
        if len(ids) > 1:
            duplicados_grupos_count += 1
            duplicados_registros_count += len(ids)

    total_pedidos = len(pedidos)
    total_pagos = sum(
        1 for p in pedidos
        if (p.get("status") or "").upper() == "PAGO"
        and p.get("plano") in PLANOS
        and PLANOS[p.get("plano")]["preco"] > 0
    )
    total_gratis = sum(
        1 for p in pedidos
        if (p.get("status") or "").upper() == "PAGO" and p.get("plano") == "trx-gratis"
    )
    total_faturado_centavos = sum(
        PLANOS[p.get("plano", "")]["preco"]
        for p in pedidos
        if (p.get("status") or "").upper() == "PAGO"
        and p.get("plano") in PLANOS
        and PLANOS[p.get("plano")]["preco"] > 0
    )

    stats = {
        "total_pedidos": total_pedidos,
        "processados": total_pagos,
        "pendentes": sum(1 for p in pedidos if (p.get("status") or "").upper() != "PAGO"),
        "pagos": total_pagos,
        "total_gratis": total_gratis,
        "total_faturado": f"R$ {total_faturado_centavos / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "online": total_usuarios_online(excluir_request_atual=True)
    }

    pedidos_processados = []
    for pedido in pedidos:
        pedido["whatsapp_link"] = None
        pedido["whatsapp_status"] = ""

        mensagens_enviadas = int(pedido.get("whatsapp_mensagens_enviadas") or 0)

        pedido["whatsapp_link"] = gerar_link_whatsapp(pedido)
        if not pedido["whatsapp_link"] and (pedido.get("telefone") or ""):
            pedido["whatsapp_status"] = "telefone inválido"

        if mensagens_enviadas > 0:
            pedido["whatsapp_status"] = f"{mensagens_enviadas} mensagem(ns) enviada(s)"

        data_local = converter_data_para_timezone_admin(pedido.get("created_at"))
        pedido["created_at_local"] = data_local.strftime("%d/%m/%Y %H:%M") if data_local else "-"

        info_30_dias = calcular_contagem_regressiva_30_dias(pedido)
        pedido.update(info_30_dias)

        ids_mesmos_dados = grupos_duplicados[chave_duplicidade_pedido(pedido)]
        pedido["duplicados_total"] = max(0, len(ids_mesmos_dados) - 1)
        pedido["tem_duplicados"] = pedido["duplicados_total"] > 0

        if filtro_plano != "todos":
            if filtro_plano == "pagos":
                if (pedido.get("status") or "").upper() != "PAGO":
                    continue
                if pedido.get("plano") not in PLANOS or PLANOS[pedido.get("plano")]["preco"] <= 0:
                    continue
            elif filtro_plano == "gratis" and pedido.get("plano") != "trx-gratis":
                continue
            elif filtro_plano == "pendentes" and (pedido.get("status") or "").upper() == "PAGO":
                continue
            elif filtro_plano not in ("pagos", "gratis", "pendentes") and pedido.get("plano") != filtro_plano:
                continue

        pedido["data_formatada_busca"] = pedido["created_at_local"]

        if busca:
            campos_busca = [
                pedido.get("nome") or "",
                pedido.get("email") or "",
                pedido.get("telefone") or "",
                pedido.get("plano") or "",
                pedido.get("status") or "",
                pedido.get("data_formatada_busca") or "",
                str(pedido.get("dias_restantes_30") if pedido.get("dias_restantes_30") is not None else ""),
                str(pedido.get("whatsapp_mensagens_enviadas") or 0)
            ]
            texto = " ".join(campos_busca).lower()
            if busca not in texto:
                continue

        pedidos_processados.append(pedido)

    return render_template(
        "admin_dashboard.html",
        pedidos=pedidos_processados,
        stats=stats,
        duplicados_grupos_count=duplicados_grupos_count,
        duplicados_registros_count=duplicados_registros_count,
        busca=busca,
        filtro_plano=filtro_plano,
        planos=list(PLANOS.keys())
    )


@app.route("/admin/relatorios")
def admin_relatorios():
    if not session.get("admin"):
        return redirect("/admin/login")

    pedidos = listar_pedidos()

    total_pedidos = len(pedidos)
    total_pagos = [
        p for p in pedidos
        if (p.get("status") or "").upper() == "PAGO"
        and p.get("plano") in PLANOS
        and PLANOS[p.get("plano")]["preco"] > 0
    ]

    faturado_centavos = sum(PLANOS[p["plano"]]["preco"] for p in total_pagos)

    por_plano = []
    for plano_id, info in PLANOS.items():
        pagos_plano = [p for p in pedidos if (p.get("status") or "").upper() == "PAGO" and p.get("plano") == plano_id]
        quantidade = len(pagos_plano)
        faturado = (info["preco"] * quantidade) / 100
        por_plano.append({
            "plano_id": plano_id,
            "nome": info["nome"],
            "quantidade": quantidade,
            "faturado": f"R$ {faturado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        })

    return render_template(
        "admin_relatorios.html",
        stats={
            "total_pedidos": total_pedidos,
            "pagos_reais": len(total_pagos),
            "pendentes": sum(1 for p in pedidos if (p.get("status") or "").upper() != "PAGO"),
            "faturado_total": f"R$ {faturado_centavos / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            "online": total_usuarios_online(excluir_request_atual=True)
        },
        por_plano=por_plano
    )


@app.route("/admin/analytics")
def admin_analytics():
    if not session.get("admin"):
        return redirect("/admin/login")

    hoje = datetime.now().date()
    inicio_padrao = (hoje - timedelta(days=30)).isoformat()
    return render_template("admin_analytics.html", planos=list(PLANOS.keys()), inicio_padrao=inicio_padrao, hoje=hoje.isoformat())


@app.route("/api/analytics/users/<path:user_key>/plan-stats")
def api_analytics_user_plan_stats(user_key):
    if not session.get("admin"):
        return jsonify({"error": "unauthorized"}), 403

    registro = buscar_user_plan_stats(user_key)
    if not registro:
        return jsonify({
            "userKey": user_key,
            "free_count": 0,
            "paid_count": 0,
            "by_plan": {
                "trx-gratis": 0,
                "trx-bronze": 0,
                "trx-prata": 0,
                "trx-gold": 0,
                "trx-black": 0
            },
            "updated_at": None
        })

    return jsonify({
        "userKey": registro["user_key"],
        "free_count": int(registro.get("free_count") or 0),
        "paid_count": int(registro.get("paid_count") or 0),
        "by_plan": {
            "trx-gratis": int(registro.get("plan_trx_gratis_count") or 0),
            "trx-bronze": int(registro.get("plan_trx_bronze_count") or 0),
            "trx-prata": int(registro.get("plan_trx_prata_count") or 0),
            "trx-gold": int(registro.get("plan_trx_gold_count") or 0),
            "trx-black": int(registro.get("plan_trx_black_count") or 0)
        },
        "updated_at": registro.get("updated_at").isoformat() if registro.get("updated_at") else None
    })


@app.route("/api/analytics/summary")
def api_analytics_summary():
    if not session.get("admin"):
        return jsonify({"error": "unauthorized"}), 403

    try:
        start = parse_iso_date(request.args.get("start"))
    except Exception:
        return jsonify({"error": "start inválido (YYYY-MM-DD)"}), 400

    try:
        end = parse_iso_date(request.args.get("end"))
    except Exception:
        return jsonify({"error": "end inválido (YYYY-MM-DD)"}), 400

    if start and end and end < start:
        return jsonify({"error": "end deve ser maior/igual a start"}), 400

    eventos = carregar_eventos_analytics_filtrados(start=start, end=end, plano='all')
    totals_by_plan = {plano: 0 for plano in PLANOS.keys()}
    users = set()
    daily_revenue = defaultdict(int)
    daily_orders = defaultdict(int)
    daily_paid_orders = defaultdict(int)
    daily_free_orders = defaultdict(int)
    daily_by_plan = {plano: defaultdict(int) for plano in PLANOS.keys()}

    total_free = 0
    total_paid = 0
    revenue_total = 0

    for evento in eventos:
        plano = evento.get("plano")
        if plano not in totals_by_plan:
            continue

        user_key = evento.get("user_key")
        if user_key:
            users.add(user_key)

        date_key = evento["created_at"].date().isoformat()
        totals_by_plan[plano] += 1
        daily_orders[date_key] += 1
        daily_by_plan[plano][date_key] += 1

        amount = int(evento.get("amount_centavos") or 0)
        revenue_total += amount
        daily_revenue[date_key] += amount

        if amount > 0:
            total_paid += 1
            daily_paid_orders[date_key] += 1
        else:
            total_free += 1
            daily_free_orders[date_key] += 1

    def para_lista(d):
        return [{"date": k, "value": d[k]} for k in sorted(d.keys())]

    return jsonify({
        "total_users": len(users),
        "total_free": total_free,
        "total_paid": total_paid,
        "totals_by_plan": totals_by_plan,
        "revenue_total": revenue_total,
        "daily_revenue": para_lista(daily_revenue),
        "daily_orders": para_lista(daily_orders),
        "daily_paid_orders": para_lista(daily_paid_orders),
        "daily_free_orders": para_lista(daily_free_orders),
        "daily_by_plan": {plano: para_lista(serie) for plano, serie in daily_by_plan.items()}
    })


@app.route("/api/analytics/chart")
def api_analytics_chart():
    if not session.get("admin"):
        return jsonify({"error": "unauthorized"}), 403

    metric = (request.args.get("metric") or "revenue").strip()
    group_by = (request.args.get("groupBy") or "day").strip()
    plan = (request.args.get("plan") or "all").strip()
    chart_type = (request.args.get("chartType") or "line").strip()

    metric_validas = {"revenue", "orders_total", "orders_paid", "orders_free", "orders_by_plan"}
    group_validos = {"day", "week", "month"}

    if metric not in metric_validas:
        return jsonify({"error": "metric inválida"}), 400
    if group_by not in group_validos:
        return jsonify({"error": "groupBy inválido"}), 400
    if plan != "all" and plan not in PLANOS:
        return jsonify({"error": "plan inválido"}), 400

    try:
        start = parse_iso_date(request.args.get("start"))
    except Exception:
        return jsonify({"error": "start inválido (YYYY-MM-DD)"}), 400

    try:
        end = parse_iso_date(request.args.get("end"))
    except Exception:
        return jsonify({"error": "end inválido (YYYY-MM-DD)"}), 400

    if start and end and end < start:
        return jsonify({"error": "end deve ser maior/igual a start"}), 400

    filtro_plano = plan if metric == "orders_by_plan" and plan != "all" else "all"
    eventos = carregar_eventos_analytics_filtrados(start=start, end=end, plano=filtro_plano)

    series = []

    if metric == "orders_by_plan" and plan == "all":
        for plano_id in PLANOS.keys():
            agrupado = defaultdict(int)
            for evento in eventos:
                if evento.get("plano") != plano_id:
                    continue
                bucket = agrupar_periodo(evento["created_at"].date(), group_by)
                agrupado[bucket] += 1
            series.append({
                "name": plano_id,
                "data": [{"x": k, "y": agrupado[k]} for k in sorted(agrupado.keys())]
            })
    else:
        agrupado = defaultdict(int)
        for evento in eventos:
            amount = int(evento.get("amount_centavos") or 0)
            if metric == "orders_paid" and amount <= 0:
                continue
            if metric == "orders_free" and amount > 0:
                continue
            if metric == "orders_by_plan" and plan != "all" and evento.get("plano") != plan:
                continue

            bucket = agrupar_periodo(evento["created_at"].date(), group_by)
            if metric == "revenue":
                agrupado[bucket] += amount
            else:
                agrupado[bucket] += 1

        nome = "Total" if metric != "orders_by_plan" else plan
        series.append({
            "name": nome,
            "data": [{"x": k, "y": agrupado[k]} for k in sorted(agrupado.keys())]
        })

    return jsonify({
        "metric": metric,
        "groupBy": group_by,
        "plan": plan,
        "chartType": chart_type,
        "series": series
    })


@app.route("/admin/whatsapp/<order_id>")
def admin_whatsapp(order_id):
    if not session.get("admin"):
        return redirect("/admin/login")

    pedido = buscar_order_por_id(order_id)
    if not pedido:
        return "Pedido não encontrado", 404

    link = gerar_link_whatsapp(pedido)
    if not link:
        return "Telefone do usuário não encontrado/inválido", 400

    incrementar_whatsapp_enviado(order_id)
    return redirect(link)


@app.route("/admin/pedido/<order_id>/excluir", methods=["POST"])
def admin_excluir_pedido(order_id):
    if not session.get("admin"):
        return redirect("/admin/login")

    excluir_order(order_id)
    return redirect("/admin/dashboard")


@app.route("/admin/pedido/<order_id>/excluir-duplicados", methods=["POST"])
def admin_excluir_duplicados(order_id):
    if not session.get("admin"):
        return redirect("/admin/login")

    nome = request.form.get("nome", "")
    email = request.form.get("email", "")
    telefone = request.form.get("telefone", "")
    excluir_duplicados_por_dados(order_id, nome, email, telefone)

    return redirect("/admin/dashboard")


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



