from flask import (
    Flask, request, jsonify, render_template,
    g, redirect, session, send_from_directory
)
import os
import json
import csv
import uuid
import requests
import time
import base64
from datetime import datetime, timedelta, timezone
import math
from urllib.parse import quote
import re
import threading
import hmac
import hashlib
import secrets
from collections import defaultdict, deque
from zoneinfo import ZoneInfo
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash
from cryptography.fernet import Fernet, InvalidToken

from compactador import compactar_plano
from email_utils import enviar_email, enviar_email_com_anexo, enviar_email_simples
from whatsapp_sender import schedule_whatsapp
from backup_utils import criar_backup_criptografado, remover_backups_antigos

from database import (
    init_db,
    salvar_order,
    buscar_order_por_id,
    marcar_order_processada,
    registrar_falha_email,
    atualizar_order_afiliado,
    transacao_ja_processada,
    marcar_transacao_processada,
    listar_pedidos,
    buscar_pedido_detalhado,
    obter_estatisticas,
    contar_pedidos_pagos_por_plano,
    agendar_whatsapp,
    listar_whatsapp_pendentes,
    registrar_falha_whatsapp,
    incrementar_whatsapp_enviado,
    excluir_order,
    excluir_duplicados_por_dados,
    excluir_duplicados_gratis_mesmo_dia,
    registrar_evento_compra_analytics,
    buscar_user_plan_stats,
    listar_eventos_analytics,
    backfill_analytics_from_orders,
    registrar_lead_upgrade_cliente,
    registrar_whatsapp_auto_agendamento,
    marcar_whatsapp_auto_enviado,
    registrar_falha_whatsapp_auto,
    registrar_quiz_submission,
    existe_quiz_submission,
    listar_afiliados,
    buscar_afiliado_por_slug,
    buscar_afiliado_por_email,
    buscar_indicacao_afiliado_por_email,
    criar_afiliado,
    atualizar_afiliado,
    excluir_afiliado,
    registrar_primeira_indicacao_afiliado,
    registrar_comissao_afiliado,
    registrar_backup_execucao,
    listar_backups_execucao,
    adquirir_lock_backup_distribuido,
    liberar_lock_backup_distribuido,
    buscar_conta_cliente_por_email,
    criar_ou_atualizar_conta_cliente,
    registrar_codigo_primeiro_acesso,
    incrementar_tentativa_codigo_cliente,
    limpar_codigo_cliente,
    confirmar_senha_conta_cliente,
    forcar_reset_senha_conta_cliente,
    atualizar_ultimo_login_conta_cliente,
    listar_pedidos_pagos_por_email,
    listar_pedidos_acesso_por_email,
    buscar_ultimo_pedido_pago_por_email,
    salvar_remember_token_cliente,
    limpar_remember_token_cliente,
    buscar_conta_cliente_por_remember_hash,
    conceder_bonus_indicacao_mes_gratis
)

print("Ã°Å¸Å¡â‚¬ APP INICIADO", flush=True)

# ======================================================
# APP
# ======================================================

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# ======================================================
# SEGURANÃƒâ€¡A (ENV)
# ======================================================

ADMIN_SECRET = os.environ["ADMIN_SECRET"]
app.secret_key = ADMIN_SECRET
ADMIN_PASSWORD = (os.environ.get("ADMIN_PASSWORD") or "").strip()
ADMIN_PASSWORD_HASH = (os.environ.get("ADMIN_PASSWORD_HASH") or "").strip()
if not ADMIN_PASSWORD and not ADMIN_PASSWORD_HASH:
    raise RuntimeError("Configure ADMIN_PASSWORD ou ADMIN_PASSWORD_HASH.")

# ======================================================
# INIT
# ======================================================

init_db()

PASTA_SAIDA = "saida"
os.makedirs(PASTA_SAIDA, exist_ok=True)
CAPITAL_CURVE_CSV_PATH = (os.environ.get("CAPITAL_CURVE_CSV_PATH") or os.path.join("assets", "capital_curve.csv")).strip()
CAPITAL_CURVE_AXIS_PADDING = float(os.environ.get("CAPITAL_CURVE_AXIS_PADDING", "100"))
CAPITAL_CURVE_VALUE_MODE = (os.environ.get("CAPITAL_CURVE_VALUE_MODE") or "points").strip().lower()
if CAPITAL_CURVE_VALUE_MODE not in {"points", "brl"}:
    CAPITAL_CURVE_VALUE_MODE = "points"
try:
    CAPITAL_CURVE_BRL_PER_POINT = float(os.environ.get("CAPITAL_CURVE_BRL_PER_POINT", "0.2"))
except (TypeError, ValueError):
    CAPITAL_CURVE_BRL_PER_POINT = 0.2
CAPITAL_CURVE_BRL_PER_POINT = max(0.0, CAPITAL_CURVE_BRL_PER_POINT)
CLIENT_INSTALL_VIDEO_ID = (os.environ.get("CLIENT_INSTALL_VIDEO_ID") or "19bR-OLADRU").strip()
if not re.fullmatch(r"[A-Za-z0-9_-]{6,20}", CLIENT_INSTALL_VIDEO_ID):
    CLIENT_INSTALL_VIDEO_ID = "19bR-OLADRU"

# ======================================================
# INFINITEPAY CONFIG
# ======================================================

INFINITEPAY_URL = "https://api.infinitepay.io/invoices/public/checkout/links"
HANDLE = "guilherme-gomes-v85"
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://www.trxpro.com.br").strip().rstrip("/")
INFINITEPAY_WEBHOOK_TOKEN = (os.environ.get("INFINITEPAY_WEBHOOK_TOKEN") or "").strip()
if not INFINITEPAY_WEBHOOK_TOKEN:
    INFINITEPAY_WEBHOOK_TOKEN = hashlib.sha256(f"{ADMIN_SECRET}:{HANDLE}:webhook".encode("utf-8")).hexdigest()[:48]
WEBHOOK_URL = f"{PUBLIC_BASE_URL}/webhook/infinitypay?token={INFINITEPAY_WEBHOOK_TOKEN}"
ALLOW_LEGACY_UNSIGNED_WEBHOOK = (os.environ.get("ALLOW_LEGACY_UNSIGNED_WEBHOOK", "false").strip().lower() == "true")

DEFAULT_SECURE_COOKIE = PUBLIC_BASE_URL.startswith("https://")
SESSION_COOKIE_SECURE = (os.environ.get("SESSION_COOKIE_SECURE") or ("true" if DEFAULT_SECURE_COOKIE else "false")).strip().lower() == "true"
SESSION_COOKIE_SAMESITE = (os.environ.get("SESSION_COOKIE_SAMESITE") or "Lax").strip()
SESSION_TTL_HOURS = int(os.environ.get("SESSION_TTL_HOURS", "8"))
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=SESSION_COOKIE_SECURE,
    SESSION_COOKIE_SAMESITE=SESSION_COOKIE_SAMESITE,
    PERMANENT_SESSION_LIFETIME=timedelta(hours=max(1, SESSION_TTL_HOURS)),
    MAX_CONTENT_LENGTH=int(os.environ.get("MAX_CONTENT_LENGTH_BYTES", str(2 * 1024 * 1024))),
)

# ======================================================
# WHATSAPP FOLLOW-UP (PLANO GRÃƒÂTIS)
# ======================================================

def corrigir_texto_quebrado(texto):
    valor = (texto or "").strip()
    if not valor:
        return ""
    for _ in range(3):
        if "Ã" not in valor and "Â" not in valor and "â" not in valor:
            break
        try:
            valor = valor.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            break
    return valor


WHATSAPP_MENSAGEM = corrigir_texto_quebrado(os.environ.get(
    "WHATSAPP_MENSAGEM",
    (
        "Ola {nome}\n\n"
        "Seu {plano} foi liberado com sucesso.\n\n"
        "Quero confirmar se conseguiu instalar corretamente.\n"
        "Se tiver qualquer duvida ou dificuldade, me chame e eu te dou suporte imediato.\n\n"
        "Lembre-se de entrar na nossa comunidade para receber atualizacoes do robo:\n"
        "https://chat.whatsapp.com/KPcaKf6OsaQHG2cUPAU1CE\n\n"
        "Estou a disposicao."
    )
))
WHATSAPP_TEMPLATE = corrigir_texto_quebrado(os.environ.get(
    "WHATSAPP_TEMPLATE",
    "{nome}, seu pagamento do {plano} foi confirmado. Qualquer duvida pode me chamar!"
))
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

BACKUP_ENABLED = (os.environ.get("BACKUP_ENABLED", "true").strip().lower() == "true")
BACKUP_TIMEZONE = os.environ.get("BACKUP_TIMEZONE", ADMIN_TIMEZONE).strip()
BACKUP_HOUR = int(os.environ.get("BACKUP_HOUR", "23"))
BACKUP_MINUTE = int(os.environ.get("BACKUP_MINUTE", "59"))
BACKUP_OUTPUT_DIR = os.environ.get("BACKUP_OUTPUT_DIR", "backups").strip() or "backups"
BACKUP_EMAIL_TO = os.environ.get("BACKUP_EMAIL_TO", "trxtradingpro@gmail.com").strip()
BACKUP_ENCRYPTION_PASSWORD = (os.environ.get("BACKUP_ENCRYPTION_PASSWORD") or ADMIN_SECRET).strip()
BACKUP_RETENTION_DAYS = int(os.environ.get("BACKUP_RETENTION_DAYS", "15"))
BACKUP_WORKER_ENABLED = (os.environ.get("BACKUP_WORKER_ENABLED", "true").strip().lower() == "true")
_backup_lock = threading.Lock()

CSRF_HEADER_NAME = "X-CSRF-Token"
FAILED_LOGIN_LIMIT = int(os.environ.get("FAILED_LOGIN_LIMIT", "5"))
FAILED_LOGIN_WINDOW_SECONDS = int(os.environ.get("FAILED_LOGIN_WINDOW_SECONDS", str(15 * 60)))
FAILED_LOGIN_LOCK_SECONDS = int(os.environ.get("FAILED_LOGIN_LOCK_SECONDS", str(15 * 60)))
_failed_login_attempts = {}
_failed_login_lock = threading.Lock()

_request_rate_limit = {}
_request_rate_lock = threading.Lock()

CLIENT_SESSION_EMAIL_KEY = "cliente_email"
CLIENT_PENDING_EMAIL_KEY = "cliente_pending_email"
CLIENT_VERIFY_EMAIL_KEY = "cliente_verify_email"
CLIENT_PENDING_REMEMBER_KEY = "cliente_pending_remember"
CLIENT_REMEMBER_COOKIE = "trx_client_remember"
CLIENT_REMEMBER_DAYS = int(os.environ.get("CLIENT_REMEMBER_DAYS", "30"))
CLIENT_CODE_TTL_SECONDS = int(os.environ.get("CLIENT_CODE_TTL_SECONDS", "120"))
CLIENT_CODE_MAX_ATTEMPTS = int(os.environ.get("CLIENT_CODE_MAX_ATTEMPTS", "5"))

CLIENT_PLAN_EXPIRY_DAYS = {
    "trx-gratis": int(os.environ.get("EXP_DIAS_TRX_GRATIS", "30")),
    "trx-teste": int(os.environ.get("EXP_DIAS_TRX_TESTE", "30")),
    "trx-bronze": int(os.environ.get("EXP_DIAS_TRX_BRONZE", "30")),
    "trx-prata": int(os.environ.get("EXP_DIAS_TRX_PRATA", "30")),
    "trx-gold": int(os.environ.get("EXP_DIAS_TRX_GOLD", "30")),
    "trx-black": int(os.environ.get("EXP_DIAS_TRX_BLACK", "30")),
}

CLIENT_PLAN_CONTRACT_LIMITS = {
    "trx-gratis": 300,
    "trx-teste": 1,
    "trx-bronze": 1,
    "trx-prata": 5,
    "trx-gold": 20,
    "trx-black": 300,
}

_CLIENT_DATA_FERNET = None

# ======================================================
# PLANOS (COM TESTE + GRÃƒÂTIS)
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
        "nome": "TRX GRATIS",
        "pasta": "Licencas/TRX GRATIS",
        "preco": 0,
        "gratis": True,
        "redirect_url": "https://sites.google.com/view/planogratuito/in%C3%ADcio"
    }
}

AFFILIATE_SLUG_RE = re.compile(r"^[a-z0-9-]{2,60}$")
AFFILIATE_COMMISSION_PERCENT = 50.0
AFFILIATE_COMMISSION_RATE = AFFILIATE_COMMISSION_PERCENT / 100.0
AFFILIATE_TERMS_VERSION = (os.environ.get("AFFILIATE_TERMS_VERSION") or "2026-02-17").strip()
RESERVED_AFFILIATE_SLUGS = {
    "admin",
    "api",
    "assets",
    "checkout",
    "comprar",
    "dashboard",
    "login",
    "logout",
    "minha-conta",
    "primeiro-acesso",
    "confirmar-codigo",
    "diagnostico-de-perfil-trx",
    "quiz",
    "termos",
    "privacidade",
    "contato",
    "sucesso",
    "webhook",
    "online",
    "favicon",
    "favicon-ico",
    "static"
}


def normalizar_slug_afiliado(valor):
    slug = re.sub(r"[^a-z0-9-]", "-", (valor or "").strip().lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug[:60]


def slug_afiliado_valido(slug):
    if not slug:
        return False
    return AFFILIATE_SLUG_RE.fullmatch(slug) is not None and slug not in RESERVED_AFFILIATE_SLUGS


def montar_plano_checkout(plano_base, affiliate_slug=None):
    if affiliate_slug:
        return f"{plano_base}-{affiliate_slug}"
    return plano_base


def decompor_plano_checkout(plano_checkout):
    plano_checkout = (plano_checkout or "").strip().lower()
    if plano_checkout in PLANOS:
        return plano_checkout, None

    for plano_base in sorted(PLANOS.keys(), key=len, reverse=True):
        prefixo = f"{plano_base}-"
        if not plano_checkout.startswith(prefixo):
            continue
        affiliate_slug = normalizar_slug_afiliado(plano_checkout[len(prefixo):])
        if not slug_afiliado_valido(affiliate_slug):
            return None, None
        return plano_base, affiliate_slug

    return None, None


def obter_afiliado_ativo(slug):
    slug = normalizar_slug_afiliado(slug)
    if not slug_afiliado_valido(slug):
        return None
    return buscar_afiliado_por_slug(slug, apenas_ativos=True)


def carregar_afiliado_contexto():
    slug_query = normalizar_slug_afiliado(request.args.get("aff"))
    if slug_query:
        afiliado_query = obter_afiliado_ativo(slug_query)
        if afiliado_query:
            session["affiliate_slug"] = afiliado_query["slug"]
            return afiliado_query

    session_slug = normalizar_slug_afiliado(session.get("affiliate_slug"))
    if session_slug:
        afiliado_sessao = obter_afiliado_ativo(session_slug)
        if afiliado_sessao:
            return afiliado_sessao
        session.pop("affiliate_slug", None)

    return None


def montar_checkout_suffix(affiliate):
    if not affiliate:
        return ""
    return f"-{affiliate['slug']}"


def gerar_slug_afiliado_unico(nome, email):
    nome_norm = normalizar_nome(nome)
    email_norm = normalizar_email(email)
    local_email = email_norm.split("@", 1)[0] if "@" in email_norm else ""
    primeiro_nome = nome_norm.split(" ", 1)[0] if nome_norm else ""

    candidatos = []
    if primeiro_nome:
        candidatos.append(primeiro_nome)
    if local_email:
        candidatos.append(local_email)
    if primeiro_nome and local_email:
        candidatos.append(f"{primeiro_nome}-{local_email}")

    for candidato in candidatos:
        slug = normalizar_slug_afiliado(candidato)
        if not slug_afiliado_valido(slug):
            continue
        if not buscar_afiliado_por_slug(slug, apenas_ativos=False):
            return slug

    base = normalizar_slug_afiliado(local_email or primeiro_nome or "afiliado")
    if not slug_afiliado_valido(base):
        base = "afiliado"
    base = base[:52].rstrip("-")

    for _ in range(40):
        sufixo = secrets.token_hex(2)
        slug = normalizar_slug_afiliado(f"{base}-{sufixo}")
        if not slug_afiliado_valido(slug):
            continue
        if not buscar_afiliado_por_slug(slug, apenas_ativos=False):
            return slug

    return ""


def montar_dados_afiliado_cliente(afiliado):
    if not afiliado:
        return None

    slug = normalizar_slug_afiliado(afiliado.get("slug"))
    if not slug_afiliado_valido(slug):
        return None

    nome = normalizar_nome(afiliado.get("nome") or "")
    link_publico = montar_url_absoluta(f"/{slug}")
    terms_accepted_at = afiliado.get("terms_accepted_at")
    link_saved_at = afiliado.get("link_saved_at")
    return {
        "slug": slug,
        "nome": nome or slug,
        "email": normalizar_email(afiliado.get("email") or ""),
        "telefone": normalizar_telefone(afiliado.get("telefone") or ""),
        "ativo": bool(afiliado.get("ativo")),
        "terms_accepted": bool(terms_accepted_at),
        "terms_accepted_at": terms_accepted_at,
        "link_saved": bool(link_saved_at),
        "link_saved_at": link_saved_at,
        "link_publico": link_publico,
    }


def affiliate_eh_autoindicacao(referred_email, affiliate_slug=None, affiliate_email=None):
    referred_email_norm = normalizar_email(referred_email)
    if not EMAIL_RE.fullmatch(referred_email_norm):
        return False

    affiliate_email_norm = normalizar_email(affiliate_email or "")
    if not EMAIL_RE.fullmatch(affiliate_email_norm):
        slug_norm = normalizar_slug_afiliado(affiliate_slug or "")
        if slug_afiliado_valido(slug_norm):
            afiliado = buscar_afiliado_por_slug(slug_norm, apenas_ativos=False)
            affiliate_email_norm = normalizar_email((afiliado or {}).get("email") or "")

    if not EMAIL_RE.fullmatch(affiliate_email_norm):
        return False

    return referred_email_norm == affiliate_email_norm


def resolver_afiliado_para_compra(email, affiliate_slug_checkout, order_id=None, checkout_slug=None, forcar_direto=False):
    email_norm = normalizar_email(email)
    if not EMAIL_RE.fullmatch(email_norm):
        return None, None

    if forcar_direto:
        return None, None

    referral = buscar_indicacao_afiliado_por_email(email_norm)
    if referral:
        slug_referral = normalizar_slug_afiliado(referral.get("affiliate_slug") or "")
        if slug_afiliado_valido(slug_referral):
            if affiliate_eh_autoindicacao(
                referred_email=email_norm,
                affiliate_slug=slug_referral,
                affiliate_email=referral.get("affiliate_email")
            ):
                referral = None
            else:
                return {
                    "slug": slug_referral,
                    "nome": normalizar_nome(referral.get("affiliate_nome") or ""),
                    "email": normalizar_email(referral.get("affiliate_email") or ""),
                    "telefone": normalizar_telefone(referral.get("affiliate_telefone") or ""),
                }, referral

    slug_checkout = normalizar_slug_afiliado(affiliate_slug_checkout or "")
    if not slug_afiliado_valido(slug_checkout):
        return None, referral

    afiliado = buscar_afiliado_por_slug(slug_checkout, apenas_ativos=True)
    if not afiliado:
        return None, referral

    if affiliate_eh_autoindicacao(
        referred_email=email_norm,
        affiliate_slug=slug_checkout,
        affiliate_email=afiliado.get("email")
    ):
        return None, referral

    snap = {
        "slug": afiliado.get("slug") or slug_checkout,
        "nome": normalizar_nome(afiliado.get("nome") or ""),
        "email": normalizar_email(afiliado.get("email") or ""),
        "telefone": normalizar_telefone(afiliado.get("telefone") or ""),
    }

    registrar_primeira_indicacao_afiliado(
        referred_email=email_norm,
        affiliate_slug=snap["slug"],
        affiliate_nome=snap["nome"],
        affiliate_email=snap["email"],
        affiliate_telefone=snap["telefone"],
        first_order_id=order_id,
        first_checkout_slug=checkout_slug,
        first_source="checkout"
    )

    referral = buscar_indicacao_afiliado_por_email(email_norm)
    if referral:
        slug_referral = normalizar_slug_afiliado(referral.get("affiliate_slug") or "")
        if slug_afiliado_valido(slug_referral):
            if affiliate_eh_autoindicacao(
                referred_email=email_norm,
                affiliate_slug=slug_referral,
                affiliate_email=referral.get("affiliate_email")
            ):
                return None, None
            return {
                "slug": slug_referral,
                "nome": normalizar_nome(referral.get("affiliate_nome") or ""),
                "email": normalizar_email(referral.get("affiliate_email") or ""),
                "telefone": normalizar_telefone(referral.get("affiliate_telefone") or ""),
            }, referral

    return snap, referral


def registrar_comissao_pedido_afiliado(order, transaction_nsu=None):
    if not order:
        return False

    if (order.get("status") or "").upper() != "PAGO":
        return False

    plano = (order.get("plano") or "").strip().lower()
    if plano not in PLANOS:
        return False

    referred_email = normalizar_email(order.get("email") or "")
    if not EMAIL_RE.fullmatch(referred_email):
        return False

    affiliate_slug = normalizar_slug_afiliado(order.get("affiliate_slug") or "")
    affiliate_email_norm = normalizar_email(order.get("affiliate_email") or "")
    if not slug_afiliado_valido(affiliate_slug):
        referral = buscar_indicacao_afiliado_por_email(referred_email)
        fallback_slug = normalizar_slug_afiliado((referral or {}).get("affiliate_slug") or "")
        if not slug_afiliado_valido(fallback_slug):
            return False
        if affiliate_eh_autoindicacao(
            referred_email=referred_email,
            affiliate_slug=fallback_slug,
            affiliate_email=(referral or {}).get("affiliate_email")
        ):
            return False
        affiliate_slug = fallback_slug
        order["affiliate_slug"] = affiliate_slug
        order["affiliate_nome"] = (referral or {}).get("affiliate_nome") or order.get("affiliate_nome")
        order["affiliate_email"] = (referral or {}).get("affiliate_email") or order.get("affiliate_email")
        order["affiliate_telefone"] = (referral or {}).get("affiliate_telefone") or order.get("affiliate_telefone")
        affiliate_email_norm = normalizar_email(order.get("affiliate_email") or "")
        try:
            atualizar_order_afiliado(
                order_id=order.get("order_id"),
                affiliate_slug=order.get("affiliate_slug"),
                affiliate_nome=order.get("affiliate_nome"),
                affiliate_email=order.get("affiliate_email"),
                affiliate_telefone=order.get("affiliate_telefone"),
            )
        except Exception:
            pass
    elif affiliate_eh_autoindicacao(
        referred_email=referred_email,
        affiliate_slug=affiliate_slug,
        affiliate_email=affiliate_email_norm
    ):
        return False

    if not EMAIL_RE.fullmatch(affiliate_email_norm):
        afiliado_por_slug = buscar_afiliado_por_slug(affiliate_slug, apenas_ativos=False)
        affiliate_email_norm = normalizar_email((afiliado_por_slug or {}).get("email") or "")
        if afiliado_por_slug:
            order["affiliate_nome"] = order.get("affiliate_nome") or normalizar_nome(afiliado_por_slug.get("nome") or "")
            order["affiliate_telefone"] = order.get("affiliate_telefone") or normalizar_telefone(afiliado_por_slug.get("telefone") or "")
            order["affiliate_email"] = affiliate_email_norm or order.get("affiliate_email")

    if affiliate_eh_autoindicacao(
        referred_email=referred_email,
        affiliate_slug=affiliate_slug,
        affiliate_email=affiliate_email_norm
    ):
        return False

    amount_centavos = int(PLANOS.get(plano, {}).get("preco") or 0)
    commission_centavos = int(round(float(amount_centavos) * AFFILIATE_COMMISSION_RATE))

    return registrar_comissao_afiliado(
        order_id=order.get("order_id"),
        transaction_nsu=transaction_nsu,
        referred_email=referred_email,
        affiliate_slug=affiliate_slug,
        affiliate_nome=order.get("affiliate_nome"),
        affiliate_email=affiliate_email_norm or order.get("affiliate_email"),
        affiliate_telefone=order.get("affiliate_telefone"),
        plano=plano,
        checkout_slug=order.get("checkout_slug"),
        order_amount_centavos=amount_centavos,
        commission_percent=AFFILIATE_COMMISSION_PERCENT,
        commission_centavos=commission_centavos,
        status="PENDENTE"
    )


def conceder_bonus_indicacao_pedido(order):
    if not order:
        return False

    if (order.get("status") or "").upper() != "PAGO":
        return False

    plano = (order.get("plano") or "").strip().lower()
    if plano not in PLANOS:
        return False

    # Regra comercial: bonus de 1 mes apenas quando o indicado compra plano pago.
    if int(PLANOS.get(plano, {}).get("preco") or 0) <= 0:
        return False

    referred_email = normalizar_email(order.get("email") or "")
    if not EMAIL_RE.fullmatch(referred_email):
        return False

    affiliate_slug = normalizar_slug_afiliado(order.get("affiliate_slug") or "")
    affiliate_email = normalizar_email(order.get("affiliate_email") or "")
    affiliate_nome = normalizar_nome(order.get("affiliate_nome") or "")
    affiliate_telefone = normalizar_telefone(order.get("affiliate_telefone") or "")

    if not slug_afiliado_valido(affiliate_slug):
        referral = buscar_indicacao_afiliado_por_email(referred_email)
        fallback_slug = normalizar_slug_afiliado((referral or {}).get("affiliate_slug") or "")
        if not slug_afiliado_valido(fallback_slug):
            return False
        affiliate_slug = fallback_slug
        affiliate_nome = affiliate_nome or normalizar_nome((referral or {}).get("affiliate_nome") or "")
        affiliate_email = affiliate_email or normalizar_email((referral or {}).get("affiliate_email") or "")
        affiliate_telefone = affiliate_telefone or normalizar_telefone((referral or {}).get("affiliate_telefone") or "")

    if not EMAIL_RE.fullmatch(affiliate_email):
        afiliado = buscar_afiliado_por_slug(affiliate_slug, apenas_ativos=False)
        affiliate_email = normalizar_email((afiliado or {}).get("email") or "")
        affiliate_nome = affiliate_nome or normalizar_nome((afiliado or {}).get("nome") or "")
        affiliate_telefone = affiliate_telefone or normalizar_telefone((afiliado or {}).get("telefone") or "")

    if not EMAIL_RE.fullmatch(affiliate_email):
        return False

    if affiliate_eh_autoindicacao(
        referred_email=referred_email,
        affiliate_slug=affiliate_slug,
        affiliate_email=affiliate_email
    ):
        return False

    bonus_nome = affiliate_nome or f"Afiliado {affiliate_email.split('@', 1)[0]}"
    inserido, bonus_order_id = conceder_bonus_indicacao_mes_gratis(
        source_order_id=order.get("order_id"),
        plano=plano,
        email=affiliate_email,
        nome=bonus_nome,
        telefone=affiliate_telefone or None,
        checkout_slug=f"bonus-indicacao-{plano}"
    )
    if inserido:
        print(
            f"[AFILIADOS] Bonus de 1 mes concedido: order={order.get('order_id')} "
            f"-> afiliado={affiliate_email} plano={plano} bonus_order={bonus_order_id}",
            flush=True
        )
    return bool(inserido)


MESES_ROTULO = {
    "jan": "JAN",
    "fev": "FEV",
    "mar": "MAR",
    "abr": "ABR",
    "mai": "MAI",
    "jun": "JUN",
    "jul": "JUL",
    "ago": "AGO",
    "set": "SET",
    "out": "OUT",
    "nov": "NOV",
    "dez": "DEZ"
}

# Ordem configurada para exibir ciclo iniciando em fevereiro e encerrando em janeiro.
MESES_ORDEM_CARROSSEL = {
    "fev": 1,
    "mar": 2,
    "abr": 3,
    "mai": 4,
    "jun": 5,
    "jul": 6,
    "ago": 7,
    "set": 8,
    "out": 9,
    "nov": 10,
    "dez": 11,
    "jan": 12
}

REGEX_RELATORIO_MENSAL = re.compile(
    r"^(?P<mes>jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)_(?P<inicio>\d{2})_(?P<fim>\d{2})_(?P<valor>[+-]?\d[\d.,]*)\.png$",
    flags=re.IGNORECASE
)

# Valor oficial do acumulado final do ciclo (fev -> jan), alinhado ÃƒÂ  curva anual.
ACUMULADO_FINAL_CICLO = 78210.00


def formatar_valor_brl_com_sinal(valor):
    valor_abs = abs(float(valor))
    valor_fmt = f"{valor_abs:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    if valor > 0:
        return f"R$ +{valor_fmt}", "pos"
    if valor < 0:
        return f"R$ -{valor_fmt}", "neg"
    return f"R$ {valor_fmt}", "neutral"

backfill_analytics_from_orders({
    plano_id: int(info.get("preco") or 0)
    for plano_id, info in PLANOS.items()
})

# ======================================================
# UTIL
# ======================================================

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PASSWORD_HAS_UPPER_RE = re.compile(r"[A-Z]")
PASSWORD_HAS_LOWER_RE = re.compile(r"[a-z]")
PASSWORD_HAS_DIGIT_RE = re.compile(r"\d")
PASSWORD_HAS_SPECIAL_RE = re.compile(r"[^A-Za-z0-9]")


def agora_utc():
    return datetime.now(timezone.utc)


def obter_fernet_cliente():
    global _CLIENT_DATA_FERNET
    if _CLIENT_DATA_FERNET is not None:
        return _CLIENT_DATA_FERNET

    chave_raw = hashlib.sha256(f"{ADMIN_SECRET}:client-data:v1".encode("utf-8")).digest()
    chave = base64.urlsafe_b64encode(chave_raw)
    _CLIENT_DATA_FERNET = Fernet(chave)
    return _CLIENT_DATA_FERNET


def criptografar_texto_cliente(valor):
    texto = (valor or "").strip()
    if not texto:
        return None
    token = obter_fernet_cliente().encrypt(texto.encode("utf-8"))
    return token.decode("utf-8")


def descriptografar_texto_cliente(valor):
    texto = (valor or "").strip()
    if not texto:
        return ""
    try:
        return obter_fernet_cliente().decrypt(texto.encode("utf-8")).decode("utf-8")
    except (InvalidToken, ValueError, TypeError):
        return texto


def gerar_csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def validar_csrf_token(token_recebido):
    token_sessao = (session.get("_csrf_token") or "").strip()
    token_recebido = (token_recebido or "").strip()
    if not token_sessao or not token_recebido:
        return False
    return hmac.compare_digest(token_sessao, token_recebido)


def verificar_senha_admin(senha_digitada):
    senha_digitada = senha_digitada or ""

    if ADMIN_PASSWORD_HASH:
        try:
            return check_password_hash(ADMIN_PASSWORD_HASH, senha_digitada)
        except Exception:
            return False

    return hmac.compare_digest(senha_digitada, ADMIN_PASSWORD)


def _limpar_janela_timestamps(timestamps, agora_ts, janela):
    while timestamps and (agora_ts - timestamps[0] > janela):
        timestamps.popleft()


def registrar_tentativa_login(ip, sucesso):
    agora_ts = time.time()
    with _failed_login_lock:
        estado = _failed_login_attempts.setdefault(ip, {"fails": deque(), "locked_until": 0})
        _limpar_janela_timestamps(estado["fails"], agora_ts, FAILED_LOGIN_WINDOW_SECONDS)

        if sucesso:
            estado["fails"].clear()
            estado["locked_until"] = 0
            return

        estado["fails"].append(agora_ts)
        if len(estado["fails"]) >= FAILED_LOGIN_LIMIT:
            estado["locked_until"] = agora_ts + FAILED_LOGIN_LOCK_SECONDS
            estado["fails"].clear()


def login_bloqueado(ip):
    agora_ts = time.time()
    with _failed_login_lock:
        estado = _failed_login_attempts.get(ip)
        if not estado:
            return False, 0

        locked_until = float(estado.get("locked_until") or 0)
        if locked_until <= agora_ts:
            estado["locked_until"] = 0
            return False, 0

        return True, int(max(1, locked_until - agora_ts))


def excedeu_rate_limit(chave, limite, janela_segundos):
    agora_ts = time.time()
    with _request_rate_lock:
        fila = _request_rate_limit.setdefault(chave, deque())
        _limpar_janela_timestamps(fila, agora_ts, janela_segundos)
        if len(fila) >= limite:
            return True
        fila.append(agora_ts)
        return False


def normalizar_nome(nome):
    nome = re.sub(r"\s+", " ", (nome or "").strip())
    return nome[:120]


def normalizar_email(email):
    return (email or "").strip().lower()[:190]


def normalizar_telefone(telefone):
    return re.sub(r"\D", "", telefone or "")[:20]


def mascarar_nome(nome):
    nome = (nome or "").strip()
    if not nome:
        return "-"
    partes = [p for p in nome.split(" ") if p]
    if not partes:
        return "-"
    saida = []
    for p in partes:
        if len(p) <= 2:
            saida.append(p[0] + "*")
        else:
            saida.append(p[0] + "*" * (len(p) - 2) + p[-1])
    return " ".join(saida)


def mascarar_email(email):
    email = (email or "").strip().lower()
    if not email or "@" not in email:
        return "-"
    user, domain = email.split("@", 1)
    if len(user) <= 2:
        user_mask = user[:1] + "*"
    else:
        user_mask = user[:2] + "*" * max(2, len(user) - 2)
    return f"{user_mask}@{domain}"


def mascarar_telefone(telefone):
    nums = re.sub(r"\D", "", telefone or "")
    if not nums:
        return "-"
    if len(nums) >= 4:
        return "*" * max(0, len(nums) - 4) + nums[-4:]
    return "*" * len(nums)


def mascarar_nome_compacto(nome):
    nome = normalizar_nome(nome)
    if not nome:
        return "-"
    partes = [p for p in nome.split(" ") if p]
    if not partes:
        return "-"

    def token_curto(token):
        if len(token) <= 2:
            return token[0] + "*"
        return token[0] + "*" + token[-1]

    curtos = [token_curto(p) for p in partes[:2]]
    saida = " ".join(curtos)
    if len(partes) > 2:
        saida += " +"
    return saida


def mascarar_email_compacto(email):
    email = normalizar_email(email)
    if not email or "@" not in email:
        return "-"
    user, domain = email.split("@", 1)
    base, _, tld = domain.partition(".")

    if len(user) <= 2:
        user_hint = user[0] + "*"
    elif len(user) == 3:
        user_hint = user[:2] + "*"
    else:
        user_hint = user[:2] + "*" + user[-1]

    base_hint = (base[:2] + "*") if base else "**"
    tld_hint = ("." + tld[:2]) if tld else ""
    return f"{user_hint}@{base_hint}{tld_hint}"


def resolver_link_caixa_email(email):
    email = normalizar_email(email)
    if "@" not in email:
        return {
            "provider": "caixa de e-mail",
            "label": "Abrir caixa de e-mail",
            "url": "https://mail.google.com/"
        }

    domain = email.split("@", 1)[1]
    providers = {
        "gmail.com": ("Gmail", "https://mail.google.com/"),
        "googlemail.com": ("Gmail", "https://mail.google.com/"),
        "outlook.com": ("Outlook", "https://outlook.live.com/mail/0/"),
        "hotmail.com": ("Outlook", "https://outlook.live.com/mail/0/"),
        "live.com": ("Outlook", "https://outlook.live.com/mail/0/"),
        "msn.com": ("Outlook", "https://outlook.live.com/mail/0/"),
        "yahoo.com": ("Yahoo Mail", "https://mail.yahoo.com/"),
        "yahoo.com.br": ("Yahoo Mail", "https://mail.yahoo.com/"),
        "icloud.com": ("iCloud Mail", "https://www.icloud.com/mail"),
        "me.com": ("iCloud Mail", "https://www.icloud.com/mail"),
        "mac.com": ("iCloud Mail", "https://www.icloud.com/mail"),
        "proton.me": ("Proton Mail", "https://mail.proton.me/"),
        "protonmail.com": ("Proton Mail", "https://mail.proton.me/"),
        "protonmail.ch": ("Proton Mail", "https://mail.proton.me/"),
        "uol.com.br": ("UOL Mail", "https://email.uol.com.br/"),
        "bol.com.br": ("BOL Mail", "https://email.bol.uol.com.br/"),
        "terra.com.br": ("Terra Mail", "https://webmail.terra.com.br/"),
        "aol.com": ("AOL Mail", "https://mail.aol.com/"),
        "gmx.com": ("GMX Mail", "https://www.gmx.com/#.1559516-header-navlogin2-1"),
        "zoho.com": ("Zoho Mail", "https://mail.zoho.com/")
    }

    provider, url = providers.get(domain, ("Webmail", f"https://{domain}"))
    return {
        "provider": provider,
        "label": f"Abrir {provider}",
        "url": url
    }


def mascarar_telefone_compacto(telefone):
    nums = re.sub(r"\D", "", telefone or "")
    if not nums:
        return "-"
    if len(nums) <= 4:
        return "*" * len(nums)
    return "***" + nums[-4:]


def validar_cadastro_cliente(nome, email, telefone):
    nome = normalizar_nome(nome)
    email = normalizar_email(email)
    telefone_num = normalizar_telefone(telefone)
    if telefone_num.startswith("55") and len(telefone_num) > 11:
        telefone_num = telefone_num[2:]

    if len(nome) < 3:
        return False, "Nome invalido."
    if not EMAIL_RE.fullmatch(email):
        return False, "Email invalido."
    if len(telefone_num) != 11:
        return False, "Telefone invalido."

    return True, {
        "nome": nome,
        "email": email,
        "telefone": telefone_num
    }


def senha_forte_valida(senha):
    senha = senha or ""
    if len(senha) < 9:
        return False, "A senha precisa ter pelo menos 9 caracteres."
    if not PASSWORD_HAS_UPPER_RE.search(senha):
        return False, "A senha precisa ter pelo menos uma letra maiuscula."
    if not PASSWORD_HAS_LOWER_RE.search(senha):
        return False, "A senha precisa ter pelo menos uma letra minuscula."
    if not PASSWORD_HAS_DIGIT_RE.search(senha):
        return False, "A senha precisa ter pelo menos um numero."
    if not PASSWORD_HAS_SPECIAL_RE.search(senha):
        return False, "A senha precisa ter pelo menos um caractere especial."
    return True, ""


def gerar_senha_temporaria():
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%&*"
    while True:
        senha = "".join(secrets.choice(chars) for _ in range(12))
        ok, _ = senha_forte_valida(senha)
        if ok:
            return senha


def hash_codigo_validacao(email, codigo):
    base = f"{normalizar_email(email)}:{codigo}:{ADMIN_SECRET}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def gerar_codigo_seis_digitos():
    return f"{secrets.randbelow(1_000_000):06d}"


def conta_cliente_requer_configuracao_senha(conta):
    if not conta:
        return False
    senha_hash = (conta.get("password_hash") or "").strip()
    if not senha_hash:
        return True
    return bool(conta.get("first_access_required"))


def iniciar_fluxo_codigo_primeiro_acesso(email, conta, remember=False):
    email = normalizar_email(email)
    if not EMAIL_RE.fullmatch(email):
        return False, "E-mail invalido."
    if not conta:
        return False, "Conta nao encontrada para este e-mail."

    codigo = gerar_codigo_seis_digitos()
    code_hash = hash_codigo_validacao(email, codigo)
    ttl_seconds = max(30, int(CLIENT_CODE_TTL_SECONDS))
    expires_at = agora_utc() + timedelta(seconds=ttl_seconds)

    pending_password_hash = (conta.get("pending_password_hash") or "").strip()
    if not pending_password_hash:
        pending_password_hash = (conta.get("password_hash") or "").strip()

    ok_registro = registrar_codigo_primeiro_acesso(
        email=email,
        pending_password_hash=pending_password_hash,
        code_hash=code_hash,
        expires_at=expires_at
    )
    if not ok_registro:
        return False, "Nao foi possivel gerar o codigo agora."

    try:
        nome = descriptografar_texto_cliente(conta.get("nome"))
        enviar_email_codigo_cliente(
            destinatario=email,
            nome=nome,
            codigo=codigo,
            ttl_seconds=ttl_seconds
        )
    except Exception:
        limpar_codigo_cliente(email)
        raise

    session[CLIENT_VERIFY_EMAIL_KEY] = email
    session.pop(CLIENT_PENDING_EMAIL_KEY, None)
    session[CLIENT_PENDING_REMEMBER_KEY] = "1" if bool(remember) else "0"
    return True, ""


def cliente_logado():
    return bool((session.get(CLIENT_SESSION_EMAIL_KEY) or "").strip())


def obter_email_cliente_logado():
    return normalizar_email(session.get(CLIENT_SESSION_EMAIL_KEY))


def limpar_sessao_cliente():
    session.pop(CLIENT_SESSION_EMAIL_KEY, None)
    session.pop(CLIENT_PENDING_EMAIL_KEY, None)
    session.pop(CLIENT_VERIFY_EMAIL_KEY, None)
    session.pop(CLIENT_PENDING_REMEMBER_KEY, None)


def montar_url_absoluta(path):
    base = (PUBLIC_BASE_URL or request.host_url.rstrip("/")).rstrip("/")
    path = "/" + (path or "").lstrip("/")
    return f"{base}{path}"


def gerar_token_sucesso_order(order_id):
    order_norm = (order_id or "").strip()
    if not order_norm:
        return ""
    base = f"{order_norm}:{ADMIN_SECRET}:checkout-success:v1"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def validar_token_sucesso_order(order_id, token):
    token = (token or "").strip()
    esperado = gerar_token_sucesso_order(order_id)
    if not token or not esperado:
        return False
    return hmac.compare_digest(token, esperado)


def montar_url_sucesso_order(order_id):
    order_norm = (order_id or "").strip()
    token = gerar_token_sucesso_order(order_norm)
    return montar_url_absoluta(f"/sucesso/{order_norm}?t={token}")


def _remember_cookie_samesite():
    valor = (SESSION_COOKIE_SAMESITE or "Lax").strip()
    if valor.lower() in {"lax", "strict", "none"}:
        return valor.capitalize()
    return "Lax"


def hash_token_remember_cliente(token):
    token = (token or "").strip()
    base = f"{token}:{ADMIN_SECRET}:client-remember:v1"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def gerar_token_remember_cliente():
    return secrets.token_urlsafe(48)


def autenticar_cliente_resposta(email, remember=False, redirect_path="/minha-conta"):
    email = normalizar_email(email)
    remember = bool(remember)
    destino = (redirect_path or "/minha-conta").strip()
    if not destino.startswith("/"):
        destino = "/minha-conta"

    session.pop(CLIENT_PENDING_EMAIL_KEY, None)
    session.pop(CLIENT_VERIFY_EMAIL_KEY, None)
    session.pop(CLIENT_PENDING_REMEMBER_KEY, None)
    session[CLIENT_SESSION_EMAIL_KEY] = email
    session.permanent = remember
    atualizar_ultimo_login_conta_cliente(email)

    response = redirect(destino)
    if remember:
        token = gerar_token_remember_cliente()
        token_hash = hash_token_remember_cliente(token)
        expira_em = agora_utc() + timedelta(days=max(1, CLIENT_REMEMBER_DAYS))
        salvar_remember_token_cliente(email, token_hash, expira_em)
        response.set_cookie(
            CLIENT_REMEMBER_COOKIE,
            token,
            max_age=max(1, CLIENT_REMEMBER_DAYS) * 86400,
            httponly=True,
            secure=SESSION_COOKIE_SECURE,
            samesite=_remember_cookie_samesite(),
            path="/"
        )
    else:
        limpar_remember_token_cliente(email)
        response.delete_cookie(
            CLIENT_REMEMBER_COOKIE,
            path="/",
            secure=SESSION_COOKIE_SECURE,
            samesite=_remember_cookie_samesite()
        )
    return response


def tentar_login_automatico_cliente():
    if cliente_logado():
        return None

    token = (request.cookies.get(CLIENT_REMEMBER_COOKIE) or "").strip()
    if not token:
        return None

    token_hash = hash_token_remember_cliente(token)
    conta = buscar_conta_cliente_por_remember_hash(token_hash)
    if not conta:
        return "clear_cookie"

    if conta.get("first_access_required"):
        return "clear_cookie"

    expira_em = conta.get("remember_expires_at")
    if not expira_em:
        return "clear_cookie"

    agora = datetime.now(expira_em.tzinfo) if getattr(expira_em, "tzinfo", None) else datetime.now()
    if expira_em < agora:
        limpar_remember_token_cliente(conta.get("email"))
        return "clear_cookie"

    session[CLIENT_SESSION_EMAIL_KEY] = conta.get("email")
    session.permanent = True
    atualizar_ultimo_login_conta_cliente(conta.get("email"))
    return "ok"


def enviar_email_primeiro_acesso_cliente(destinatario, nome, senha_temporaria):
    nome_exibicao = (nome or "Cliente").strip() or "Cliente"
    link_login = montar_url_absoluta("/login")
    assunto = "Seu acesso a Area do Cliente TRX PRO"
    mensagem = (
        f"Ola, {nome_exibicao}!\n\n"
        "Sua compra foi confirmada e sua Area do Cliente ja esta liberada.\n\n"
        f"Login: {normalizar_email(destinatario)}\n"
        f"Senha temporaria: {senha_temporaria}\n\n"
        f"Acesse agora: {link_login}\n\n"
        "No primeiro acesso voce vai criar sua nova senha segura.\n"
        "Se nao foi voce, responda este email imediatamente.\n\n"
        "Equipe TRX PRO"
    )
    html = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;background:#060b16;padding:24px;">
      <div style="max-width:620px;margin:0 auto;background:#0d1629;border:1px solid #203354;border-radius:14px;overflow:hidden;">
        <div style="padding:18px 20px;background:linear-gradient(90deg,#16a34a,#0ea5e9);color:#04111d;font-weight:800;font-size:18px;">
          Acesso Area do Cliente TRX PRO
        </div>
        <div style="padding:22px 20px;color:#eaf2ff;line-height:1.55;">
          <p style="margin:0 0 12px;">Ola, <strong>{nome_exibicao}</strong>!</p>
          <p style="margin:0 0 12px;">Sua compra foi confirmada e sua Area do Cliente ja esta liberada.</p>
          <div style="margin:14px 0;padding:14px;border:1px solid #27436c;border-radius:12px;background:#0a1323;">
            <div style="margin-bottom:8px;"><strong>Login:</strong> {normalizar_email(destinatario)}</div>
            <div><strong>Senha temporaria:</strong> {senha_temporaria}</div>
          </div>
          <p style="margin:0 0 14px;">No primeiro acesso voce vai criar sua nova senha segura.</p>
          <a href="{link_login}" style="display:inline-block;padding:11px 18px;border-radius:10px;background:#22c55e;color:#04120a;text-decoration:none;font-weight:800;">
            Entrar na Area do Cliente
          </a>
          <p style="margin:16px 0 0;color:#9eb2d4;font-size:12px;">
            Se nao foi voce, responda este e-mail imediatamente.
          </p>
        </div>
      </div>
    </div>
    """
    enviar_email_simples(destinatario=destinatario, assunto=assunto, mensagem=mensagem, html=html)


def enviar_email_codigo_cliente(destinatario, nome, codigo, ttl_seconds):
    nome_exibicao = (nome or "Cliente").strip() or "Cliente"
    minutos = max(1, int(math.ceil(float(ttl_seconds) / 60.0)))
    email_norm = normalizar_email(destinatario)
    link_confirmar = montar_url_absoluta(f"/login/confirmar-codigo?email={quote(email_norm, safe='')}")
    assunto = "Codigo de confirmacao - TRX PRO"
    mensagem = (
        f"Ola, {nome_exibicao}!\n\n"
        f"Seu codigo de confirmacao e: {codigo}\n"
        f"Validade: {minutos} minuto(s).\n\n"
        f"Abra o link para confirmar e criar sua senha:\n{link_confirmar}\n\n"
        "Se voce nao solicitou essa alteracao, ignore este e-mail."
    )
    html = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;background:#050912;padding:24px;">
      <div style="max-width:620px;margin:0 auto;background:#0d1629;border:1px solid #203354;border-radius:14px;overflow:hidden;">
        <div style="padding:18px 20px;background:linear-gradient(90deg,#f59e0b,#ef4444);color:#1c0902;font-weight:800;font-size:18px;">
          Confirmacao de Seguranca TRX PRO
        </div>
        <div style="padding:22px 20px;color:#eaf2ff;line-height:1.55;">
          <p style="margin:0 0 12px;">Ola, <strong>{nome_exibicao}</strong>.</p>
          <p style="margin:0 0 10px;">Use o codigo abaixo para confirmar sua nova senha:</p>
          <div style="margin:14px 0;padding:16px;border:1px dashed #43689b;border-radius:12px;background:#0a1323;text-align:center;">
            <div style="font-size:34px;letter-spacing:8px;font-weight:900;color:#f8fafc;">{codigo}</div>
          </div>
          <p style="margin:0 0 12px;">Este codigo expira em <strong>{minutos} minuto(s)</strong>.</p>
          <a href="{link_confirmar}" style="display:inline-block;padding:11px 18px;border-radius:10px;background:#22c55e;color:#04120a;text-decoration:none;font-weight:800;">
            Confirmar codigo e criar senha
          </a>
          <p style="margin:0;color:#9eb2d4;font-size:12px;">Se voce nao solicitou essa alteracao, ignore este e-mail.</p>
        </div>
      </div>
    </div>
    """
    enviar_email_simples(destinatario=destinatario, assunto=assunto, mensagem=mensagem, html=html)


def garantir_conta_cliente_para_order(order, enviar_email_credenciais=False):
    if not order:
        return False, None

    email = normalizar_email(order.get("email"))
    if not EMAIL_RE.fullmatch(email):
        return False, None

    nome = normalizar_nome(order.get("nome") or "")
    telefone = normalizar_telefone(order.get("telefone") or "")
    nome_enc = criptografar_texto_cliente(nome)
    telefone_enc = criptografar_texto_cliente(telefone)
    conta = buscar_conta_cliente_por_email(email)
    if conta:
        criar_ou_atualizar_conta_cliente(email=email, nome=nome_enc, telefone=telefone_enc)
        return False, None

    senha_temporaria = gerar_senha_temporaria()
    senha_hash = generate_password_hash(senha_temporaria)
    resultado = criar_ou_atualizar_conta_cliente(
        email=email,
        nome=nome_enc,
        telefone=telefone_enc,
        password_hash=senha_hash,
        first_access_required=True
    )
    if not resultado.get("created"):
        return False, None

    if enviar_email_credenciais:
        try:
            enviar_email_primeiro_acesso_cliente(
                destinatario=email,
                nome=nome,
                senha_temporaria=senha_temporaria
            )
        except Exception as exc:
            print(f"[CLIENTE] Falha ao enviar credenciais para {email}: {exc}", flush=True)

    return True, senha_temporaria


def provisionar_conta_cliente_por_email(email, enviar_email_credenciais=True):
    email = normalizar_email(email)
    if not EMAIL_RE.fullmatch(email):
        return False

    if buscar_conta_cliente_por_email(email):
        return False

    ultimo = buscar_ultimo_pedido_pago_por_email(email)
    if not ultimo:
        return False

    criado, _ = garantir_conta_cliente_para_order(
        ultimo,
        enviar_email_credenciais=bool(enviar_email_credenciais)
    )
    return bool(criado)


def verificar_status_email_cliente(email):
    email = normalizar_email(email)
    if not EMAIL_RE.fullmatch(email):
        return {
            "valid": False,
            "exists": False,
            "status": "invalid",
            "requires_password_setup": False,
        }

    conta = buscar_conta_cliente_por_email(email)
    if conta:
        needs_setup = conta_cliente_requer_configuracao_senha(conta)
        return {
            "valid": True,
            "exists": True,
            "status": "setup_required" if needs_setup else "account",
            "requires_password_setup": needs_setup,
        }

    ultimo = buscar_ultimo_pedido_pago_por_email(email)
    if ultimo:
        return {
            "valid": True,
            "exists": True,
            "status": "paid_order",
            "requires_password_setup": True,
        }

    return {
        "valid": True,
        "exists": False,
        "status": "not_found",
        "requires_password_setup": False,
    }


def iniciar_recuperacao_senha_cliente(email):
    email = normalizar_email(email)
    if not EMAIL_RE.fullmatch(email):
        return False

    conta = buscar_conta_cliente_por_email(email)
    if not conta:
        return False

    senha_temporaria = gerar_senha_temporaria()
    senha_hash = generate_password_hash(senha_temporaria)
    ok_reset = forcar_reset_senha_conta_cliente(email, senha_hash)
    if not ok_reset:
        return False

    nome = descriptografar_texto_cliente(conta.get("nome"))
    enviar_email_primeiro_acesso_cliente(
        destinatario=email,
        nome=nome,
        senha_temporaria=senha_temporaria
    )
    return True


def montar_expiracao_pedido(order):
    criado = order.get("created_at")
    plano = (order.get("plano") or "").strip().lower()
    dias = int(CLIENT_PLAN_EXPIRY_DAYS.get(plano, 30))
    if not criado:
        return None

    expira_em = criado + timedelta(days=max(1, dias))
    agora = datetime.now(expira_em.tzinfo) if getattr(expira_em, "tzinfo", None) else datetime.now()
    ativo = expira_em >= agora
    return {
        "expira_em": expira_em,
        "ativo": ativo,
        "dias_restantes": int(math.ceil((expira_em - agora).total_seconds() / 86400.0))
    }


def verificar_token_webhook():
    token_qs = (request.args.get("token") or "").strip()
    token_header = (request.headers.get("X-Webhook-Token") or "").strip()
    recebido = token_qs or token_header
    if recebido:
        return hmac.compare_digest(recebido, INFINITEPAY_WEBHOOK_TOKEN)
    return ALLOW_LEGACY_UNSIGNED_WEBHOOK


def _cspr_headers():
    return "; ".join([
        "default-src 'self'",
        "base-uri 'self'",
        "form-action 'self'",
        "object-src 'none'",
        "frame-ancestors 'none'",
        "frame-src 'self' https://www.youtube.com https://www.youtube-nocookie.com",
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
        "font-src 'self' https://fonts.gstatic.com data:",
        "img-src 'self' data: https:",
        "connect-src 'self' https: wss:",
    ])


THEME_HEAD_INJECTION = (
    '\n<link rel="stylesheet" href="/assets/theme-toggle.css?v=20260216">'
    '\n<script>(function(){try{var t=localStorage.getItem("trx_theme");'
    'if(t!=="light"&&t!=="dark"){t=(window.matchMedia&&window.matchMedia("(prefers-color-scheme: dark)").matches)?"dark":"light";}'
    'document.documentElement.setAttribute("data-theme",t);'
    'document.documentElement.style.colorScheme=t;'
    '}catch(_){document.documentElement.setAttribute("data-theme","dark");document.documentElement.style.colorScheme="dark";}})();</script>\n'
)

THEME_BODY_INJECTION = '\n<script src="/assets/theme-toggle.js?v=20260216"></script>\n'


def _injetar_theme_global(response):
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "text/html" not in content_type or response.direct_passthrough:
        return response

    html = response.get_data(as_text=True)
    if not html:
        return response

    inicio = html.lstrip()[:200].lower()
    if not (inicio.startswith("<!doctype html") or inicio.startswith("<html")):
        return response

    alterado = False
    if "/assets/theme-toggle.css" not in html and "</head>" in html:
        html = html.replace("</head>", f"{THEME_HEAD_INJECTION}</head>", 1)
        alterado = True

    if "/assets/theme-toggle.js" not in html and "</body>" in html:
        html = html.replace("</body>", f"{THEME_BODY_INJECTION}</body>", 1)
        alterado = True

    if alterado:
        response.set_data(html)
        response.headers.pop("Content-Length", None)

    return response

def formatar_telefone_infinitepay(telefone):
    numeros = re.sub(r"\D", "", telefone)

    if numeros.startswith("55") and len(numeros) > 11:
        numeros = numeros[2:]

    if len(numeros) != 11:
        raise ValueError("Telefone invÃƒÂ¡lido")

    return f"+55{numeros}"


def formatar_telefone_whatsapp(telefone):
    numeros = re.sub(r"\D", "", telefone or "")

    if not numeros:
        raise ValueError("Telefone vazio")

    if numeros.startswith("55"):
        return numeros

    if len(numeros) in (10, 11):
        return f"55{numeros}"

    raise ValueError("Telefone invÃƒÂ¡lido para WhatsApp")


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
            "Configure WHATSAPP_PHONE_NUMBER_ID e WHATSAPP_ACCESS_TOKEN para envio automÃƒÂ¡tico"
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
            print(f"Ã°Å¸â€œÂ² WhatsApp automÃƒÂ¡tico enviado: {pedido['order_id']}", flush=True)
        except Exception as e:
            registrar_falha_whatsapp(
                pedido["order_id"],
                tentativas + 1,
                str(e)
            )
            print(f"Ã¢ÂÅ’ Falha WhatsApp automÃƒÂ¡tico {pedido['order_id']}: {e}", flush=True)


def iniciar_worker_whatsapp():
    def worker_loop():
        while True:
            try:
                processar_fila_whatsapp()
            except Exception as e:
                print(f"Ã¢Å¡Â Ã¯Â¸Â Worker WhatsApp com erro: {e}", flush=True)
            time.sleep(20)

    thread = threading.Thread(target=worker_loop, daemon=True)
    thread.start()


def _backup_timezone():
    try:
        return ZoneInfo(BACKUP_TIMEZONE)
    except Exception:
        return ZoneInfo("America/Sao_Paulo")


def _registrar_backup_execucao_seguro(**kwargs):
    try:
        registrar_backup_execucao(**kwargs)
    except Exception as exc:
        print(f"[BACKUP] falha ao registrar log: {exc}", flush=True)


def _segundos_ate_proximo_backup():
    tz = _backup_timezone()
    now = datetime.now(tz)
    alvo = now.replace(
        hour=max(0, min(23, BACKUP_HOUR)),
        minute=max(0, min(59, BACKUP_MINUTE)),
        second=0,
        microsecond=0,
    )
    if now >= alvo:
        alvo += timedelta(days=1)
    return max(1, int((alvo - now).total_seconds()))


def executar_backup_criptografado(trigger_type="auto"):
    inicio = agora_utc()

    if not BACKUP_ENABLED:
        return False, "Backup desativado por configuracao."

    if not BACKUP_ENCRYPTION_PASSWORD:
        return False, "Senha de criptografia do backup nao configurada."

    if not BACKUP_EMAIL_TO:
        return False, "Email de destino do backup nao configurado."

    os.makedirs(BACKUP_OUTPUT_DIR, exist_ok=True)

    with _backup_lock:
        lock_conn = None
        try:
            lock_conn = adquirir_lock_backup_distribuido()
        except Exception as exc:
            _registrar_backup_execucao_seguro(
                trigger_type=trigger_type,
                status="FAILED",
                message=f"Falha ao adquirir lock distribuido: {exc}",
                started_at=inicio,
                finished_at=agora_utc(),
            )
            return False, f"Falha ao iniciar backup: {exc}"

        if not lock_conn:
            _registrar_backup_execucao_seguro(
                trigger_type=trigger_type,
                status="SKIPPED",
                message="Outro processo ja esta executando backup.",
                started_at=inicio,
                finished_at=agora_utc(),
            )
            return False, "Backup em andamento em outro processo."

        try:
            info = criar_backup_criptografado(
                project_root=os.getcwd(),
                output_dir=BACKUP_OUTPUT_DIR,
                password=BACKUP_ENCRYPTION_PASSWORD,
            )

            assunto = f"Backup diario TRX PRO ({info['created_at_utc']})"
            mensagem = (
                "Backup criptografado gerado e enviado com sucesso.\n\n"
                f"Arquivo: {info['filename']}\n"
                f"Tamanho: {info['size_bytes']} bytes\n"
                f"SHA256: {info['sha256']}\n"
                f"Trigger: {trigger_type}\n"
            )
            enviar_email_com_anexo(
                destinatario=BACKUP_EMAIL_TO,
                assunto=assunto,
                mensagem=mensagem,
                caminho_arquivo=info["path"],
            )

            remover_backups_antigos(
                output_dir=BACKUP_OUTPUT_DIR,
                keep_days=BACKUP_RETENTION_DAYS,
            )

            fim = agora_utc()
            _registrar_backup_execucao_seguro(
                trigger_type=trigger_type,
                status="SUCCESS",
                filename=info["filename"],
                size_bytes=info["size_bytes"],
                sha256=info["sha256"],
                message="Backup enviado por email com sucesso.",
                started_at=inicio,
                finished_at=fim,
            )
            return True, "Backup enviado com sucesso."
        except Exception as exc:
            fim = agora_utc()
            _registrar_backup_execucao_seguro(
                trigger_type=trigger_type,
                status="FAILED",
                message=str(exc),
                started_at=inicio,
                finished_at=fim,
            )
            return False, f"Falha no backup: {exc}"
        finally:
            liberar_lock_backup_distribuido(lock_conn)


def iniciar_worker_backup_diario():
    if not BACKUP_ENABLED or not BACKUP_WORKER_ENABLED:
        print("Worker de backup diario desativado.", flush=True)
        return

    def worker_loop():
        while True:
            try:
                esperar = _segundos_ate_proximo_backup()
                time.sleep(esperar)
                ok, msg = executar_backup_criptografado(trigger_type="auto")
                print(f"[BACKUP] {msg}", flush=True)
                if not ok and "em andamento em outro processo" not in msg.lower():
                    time.sleep(60)
            except Exception as exc:
                print(f"[BACKUP] erro no worker: {exc}", flush=True)
                time.sleep(60)

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
iniciar_worker_backup_diario()


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
        alerta = f"Ã¢Å¡Â  Faltam {dias_restantes} dias para completar 30 dias"

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
        print(f"Ã¢Å¡Â Ã¯Â¸Â WhatsApp sender nÃƒÂ£o configurado; ignorando pedido {order_id}", flush=True)
        return

    agendado = registrar_whatsapp_auto_agendamento(order_id, delay_minutes=WHATSAPP_DELAY_MINUTES)
    if not agendado:
        print(f"Ã¢â€žÂ¹Ã¯Â¸Â WhatsApp jÃƒÂ¡ agendado/enviado para {order_id}", flush=True)
        return

    mensagem = montar_mensagem_whatsapp_pos_pago(order)
    print(f"Ã¢Å“â€¦ Pagamento confirmado; agendando WhatsApp para {order_id} em {WHATSAPP_DELAY_MINUTES} min", flush=True)

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


def parse_numero_curva_csv(valor):
    texto = re.sub(r"[^\d,.\-]", "", (str(valor or "")).strip())
    if not texto:
        return None

    if "," in texto and "." in texto:
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    else:
        texto = texto.replace(",", "")

    try:
        return float(texto)
    except ValueError:
        return None


def converter_valor_curva_para_brl(valor):
    valor_float = float(valor or 0.0)
    if CAPITAL_CURVE_VALUE_MODE == "points":
        return round(valor_float * CAPITAL_CURVE_BRL_PER_POINT, 2)
    return valor_float


def valor_curva_tem_marcador_atualizacao(valor):
    texto = (str(valor or "")).strip().lower()
    if not texto:
        return False
    return texto.endswith("g")


def parse_dia_curva_csv(valor):
    texto = (str(valor or "")).strip()
    if not re.fullmatch(r"\d{1,3}", texto):
        return None
    dia = int(texto)
    if 1 <= dia <= 366:
        return dia
    return None


def carregar_curva_capital_csv(path_csv=None):
    caminho = (path_csv or CAPITAL_CURVE_CSV_PATH or "").strip()
    if not caminho:
        return {"has_data": False, "by_day": {}, "sequence": [], "path": caminho}

    if not os.path.exists(caminho):
        return {"has_data": False, "by_day": {}, "sequence": [], "path": caminho}

    try:
        with open(caminho, "r", encoding="utf-8-sig", errors="ignore") as f:
            linhas = f.read().splitlines()
    except Exception:
        return {"has_data": False, "by_day": {}, "sequence": [], "path": caminho}

    sample = ""
    for linha in linhas:
        linha_limpa = (linha or "").strip()
        if linha_limpa and not linha_limpa.startswith("#"):
            sample = linha_limpa
            break

    if not sample:
        return {"has_data": False, "by_day": {}, "sequence": [], "path": caminho}

    delimitador = ";" if sample.count(";") >= sample.count(",") else ","
    reader = csv.reader(linhas, delimiter=delimitador)

    valores_por_dia = {}
    valores_sequencia = []
    dia_marcador = None

    for row in reader:
        cols = [(c or "").strip() for c in row]
        if not any(cols):
            continue
        if cols[0].startswith("#"):
            continue

        if len(cols) == 1:
            marcador = valor_curva_tem_marcador_atualizacao(cols[0])
            valor = parse_numero_curva_csv(cols[0])
            if valor is not None:
                valores_sequencia.append(converter_valor_curva_para_brl(valor))
                if marcador:
                    dia_seq = len(valores_sequencia)
                    dia_marcador = max(int(dia_marcador or 0), dia_seq)
            continue

        dia = parse_dia_curva_csv(cols[0])
        valor = None
        marcador = False
        for item in cols[1:]:
            candidato = parse_numero_curva_csv(item)
            if candidato is not None:
                valor = converter_valor_curva_para_brl(candidato)
                marcador = valor_curva_tem_marcador_atualizacao(item)
                break

        if valor is None:
            continue

        if dia is not None:
            valores_por_dia[dia] = valor
            if marcador:
                dia_marcador = max(int(dia_marcador or 0), int(dia))
        else:
            valores_sequencia.append(valor)
            if marcador:
                dia_seq = len(valores_sequencia)
                dia_marcador = max(int(dia_marcador or 0), dia_seq)

    # Se houver marcador "g", ignora qualquer dado posterior para evitar
    # exibir dias alem do ultimo dia atualizado.
    if int(dia_marcador or 0) > 0:
        cutoff = int(dia_marcador)
        if valores_por_dia:
            valores_por_dia = {
                int(dia): float(valor)
                for dia, valor in valores_por_dia.items()
                if int(dia) <= cutoff
            }
        if valores_sequencia:
            valores_sequencia = [float(v) for v in valores_sequencia[:cutoff]]

    has_data = bool(valores_por_dia or valores_sequencia)
    return {
        "has_data": has_data,
        "by_day": valores_por_dia,
        "sequence": valores_sequencia,
        "marker_day": int(dia_marcador or 0),
        "value_mode": CAPITAL_CURVE_VALUE_MODE,
        "brl_per_point": float(CAPITAL_CURVE_BRL_PER_POINT),
        "path": caminho,
    }


def montar_curva_capital_plano(order):
    if not order:
        return {"available": False, "message": "Nenhum plano pago/gratis encontrado para gerar a curva."}

    plano_id = (order.get("plano") or "").strip().lower()
    dias_plano = int(CLIENT_PLAN_EXPIRY_DAYS.get(plano_id, 30) or 30)
    dias_plano = max(1, min(365, dias_plano))
    limite_contratos = int(CLIENT_PLAN_CONTRACT_LIMITS.get(plano_id, 1) or 1)
    limite_contratos = max(1, min(300, limite_contratos))
    contrato_padrao = 1

    created_at_local = converter_data_para_timezone_admin(order.get("created_at"))
    if not created_at_local:
        return {"available": False, "message": "Data de inicio do plano nao encontrada."}

    curva_csv = carregar_curva_capital_csv()
    if not curva_csv.get("has_data"):
        caminho = curva_csv.get("path") or os.path.join("assets", "capital_curve.csv")
        return {
            "available": False,
            "message": f"CSV da curva nao encontrado ou sem dados ({caminho}).",
        }

    dia_inicio = created_at_local.date()
    hoje_local = converter_data_para_timezone_admin(agora_utc()).date()
    dia_atual_idx = (hoje_local - dia_inicio).days + 1
    dia_atual_idx = max(1, min(dias_plano, dia_atual_idx))

    by_day = curva_csv.get("by_day") or {}
    sequence = curva_csv.get("sequence") or []
    marker_day_raw = int(curva_csv.get("marker_day") or 0)
    dias_csv = len(sequence)
    if by_day:
        dias_csv = max(dias_csv, max(by_day.keys()))
    dias_csv = max(1, min(366, int(dias_csv)))

    def obter_delta_dia(dia):
        delta = by_day.get(dia)
        if delta is None and (dia - 1) < len(sequence):
            delta = sequence[dia - 1]
        if delta is None:
            delta = 0.0
        return float(delta)

    deltas = []
    for dia in range(1, dias_plano + 1):
        deltas.append(obter_delta_dia(dia))

    valores_acumulados = []
    saldo = 0.0
    for delta in deltas:
        saldo += float(delta)
        valores_acumulados.append(round(saldo, 2))

    dia_ultimo_atualizado = dia_atual_idx
    if marker_day_raw > 0:
        dia_ultimo_atualizado = min(dias_plano, dias_csv, marker_day_raw)
    else:
        dia_ultimo_atualizado = min(dias_plano, dia_atual_idx)
    dia_ultimo_atualizado = max(1, int(dia_ultimo_atualizado))

    valor_atual = valores_acumulados[dia_ultimo_atualizado - 1]
    saldo_total_csv = 0.0
    for dia in range(1, dia_ultimo_atualizado + 1):
        saldo_total_csv += obter_delta_dia(dia)
    saldo_total_csv = round(saldo_total_csv, 2)
    axis_padding_base = max(0.0, float(CAPITAL_CURVE_AXIS_PADDING or 100.0))

    plano_nome = PLANOS.get(plano_id, {}).get("nome", plano_id or "Plano")
    dia_fim = dia_inicio + timedelta(days=dias_plano - 1)
    dia_fim_csv = dia_inicio + timedelta(days=dia_ultimo_atualizado - 1)

    def construir_janela(inicio_dia, fim_dia, ocultar_futuro=False):
        inicio_dia = int(max(1, min(dias_plano, inicio_dia)))
        fim_dia = int(max(inicio_dia, min(dias_plano, fim_dia)))

        labels = []
        date_labels = []
        valores = []
        resultados_dia = []
        resultados_dia_pontos = []
        valores_visiveis = []

        for dia in range(inicio_dia, fim_dia + 1):
            data_ref = dia_inicio + timedelta(days=dia - 1)
            labels.append(str(dia))
            date_labels.append(data_ref.strftime("%d/%m/%Y"))

            if ocultar_futuro and dia > dia_ultimo_atualizado:
                valores.append(None)
                resultados_dia.append(None)
                resultados_dia_pontos.append(None)
                continue

            valor_dia = float(valores_acumulados[dia - 1])
            valor_dia_round = round(valor_dia, 2)
            delta_dia_brl = round(float(deltas[dia - 1]), 2)
            if CAPITAL_CURVE_VALUE_MODE == "points" and CAPITAL_CURVE_BRL_PER_POINT > 0:
                delta_dia_pontos = round(delta_dia_brl / CAPITAL_CURVE_BRL_PER_POINT, 2)
                if math.isclose(delta_dia_pontos, round(delta_dia_pontos), abs_tol=1e-9):
                    delta_dia_pontos = int(round(delta_dia_pontos))
            else:
                delta_dia_pontos = None

            valores.append(valor_dia_round)
            resultados_dia.append(delta_dia_brl)
            resultados_dia_pontos.append(delta_dia_pontos)
            valores_visiveis.append(valor_dia_round)

        if not valores_visiveis:
            valores_visiveis = [round(valor_atual, 2)]

        minimo = min(valores_visiveis)
        maximo = max(valores_visiveis)
        padding = axis_padding_base
        y_min = minimo - padding
        y_max = maximo + padding
        if math.isclose(y_min, y_max):
            y_min -= 100.0
            y_max += 100.0

        janela_inicio_data = dia_inicio + timedelta(days=inicio_dia - 1)
        janela_fim_data = dia_inicio + timedelta(days=fim_dia - 1)

        return {
            "window_start_day": inicio_dia,
            "window_end_day": fim_dia,
            "window_start_date": janela_inicio_data.strftime("%d/%m/%Y"),
            "window_end_date": janela_fim_data.strftime("%d/%m/%Y"),
            "labels": labels,
            "date_labels": date_labels,
            "values": valores,
            "daily_values": resultados_dia,
            "daily_points": resultados_dia_pontos,
            "y_min": round(y_min, 2),
            "y_max": round(y_max, 2),
        }

    janela_30_posteriores = construir_janela(
        inicio_dia=1,
        fim_dia=dia_ultimo_atualizado,
        ocultar_futuro=False
    )

    inicio_back30 = max(1, dia_ultimo_atualizado - 29)
    janela_30_anteriores = construir_janela(
        inicio_dia=inicio_back30,
        fim_dia=dia_ultimo_atualizado,
        ocultar_futuro=False
    )

    modos_janela = {
        "forward30": {
            "id": "forward30",
            "title": "30 dias posteriores",
            "description": "Mostra do inicio do plano ate o ultimo dia atualizado (marcado com g no CSV).",
            **janela_30_posteriores
        },
        "back30": {
            "id": "back30",
            "title": "30 dias anteriores",
            "description": "Mostra os ultimos 30 dias e posiciona o ultimo dia atualizado no fim (se houver menos de 30 dias, inicia no dia 1).",
            **janela_30_anteriores
        }
    }

    return {
        "available": True,
        "plan_name": plano_nome,
        "plan_days": dias_plano,
        "start_date": dia_inicio.strftime("%d/%m/%Y"),
        "end_date": dia_fim.strftime("%d/%m/%Y"),
        "csv_last_day": dia_ultimo_atualizado,
        "csv_end_date": dia_fim_csv.strftime("%d/%m/%Y"),
        "csv_total_value": saldo_total_csv,
        "csv_value_mode": curva_csv.get("value_mode") or "brl",
        "csv_brl_per_point": float(curva_csv.get("brl_per_point") or 0.0),
        "marker_day": int(marker_day_raw or 0),
        "marker_detected": bool(marker_day_raw > 0),
        "default_window_mode": "forward30",
        "window_modes": modos_janela,
        "window_start_day": janela_30_posteriores["window_start_day"],
        "window_end_day": janela_30_posteriores["window_end_day"],
        "window_start_date": janela_30_posteriores["window_start_date"],
        "window_end_date": janela_30_posteriores["window_end_date"],
        "labels": janela_30_posteriores["labels"],
        "date_labels": janela_30_posteriores["date_labels"],
        "values": janela_30_posteriores["values"],
        "daily_values": janela_30_posteriores["daily_values"],
        "daily_points": janela_30_posteriores["daily_points"],
        "y_min": janela_30_posteriores["y_min"],
        "y_max": janela_30_posteriores["y_max"],
        "current_day": dia_ultimo_atualizado,
        "current_value": round(valor_atual, 2),
        "contract_limit": limite_contratos,
        "contract_default": contrato_padrao,
        "axis_padding_base": round(axis_padding_base, 2),
        "source_file": os.path.basename(curva_csv.get("path") or ""),
    }


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


def obter_quiz_user_key():
    key = (session.get("quiz_user_key") or "").strip()
    if key:
        return key

    key = str(uuid.uuid4())
    session["quiz_user_key"] = key
    return key


def obter_ip_request():
    return (request.remote_addr or "").strip()


def parse_relatorio_mensal_nome(arquivo):
    match = REGEX_RELATORIO_MENSAL.match((arquivo or "").strip())
    if not match:
        return None

    mes = match.group("mes").lower()
    dia_inicio = match.group("inicio")
    dia_fim = match.group("fim")
    valor_raw = match.group("valor")

    valor_normalizado = valor_raw.strip()
    if "," in valor_normalizado:
        valor_normalizado = valor_normalizado.replace(".", "").replace(",", ".")

    try:
        valor_float = float(valor_normalizado)
    except ValueError:
        return None

    ganho_fmt, status = formatar_valor_brl_com_sinal(valor_float)

    return {
        "month": MESES_ROTULO.get(mes, mes.upper()),
        "start_day": dia_inicio,
        "end_day": dia_fim,
        "gain": ganho_fmt,
        "status": status,
        "image_url": f"/assets/meses/{arquivo}",
        "_value": valor_float,
        "_sort": (
            MESES_ORDEM_CARROSSEL.get(mes, 99),
            int(dia_inicio),
            int(dia_fim),
            arquivo.lower()
        )
    }


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


@app.context_processor
def injetar_contexto_global():
    token = ""
    if (request.path or "").startswith("/admin"):
        token = gerar_csrf_token()
    return {
        "csrf_token": token,
    }


@app.before_request
def aplicar_protecoes_request():
    path = request.path or ""
    method = request.method.upper()
    ip = obter_ip_request() or (request.remote_addr or "0.0.0.0")

    if not cliente_logado():
        caminho_publico = not path.startswith("/admin") and not path.startswith("/api")
        caminho_publico = caminho_publico and path != "/webhook/infinitypay"
        caminho_publico = caminho_publico and not path.startswith("/assets") and not path.startswith("/static")
        if caminho_publico:
            auto = tentar_login_automatico_cliente()
            if auto == "clear_cookie":
                g.clear_client_remember_cookie = True

    if method == "POST" and path == "/comprar":
        if excedeu_rate_limit(f"post_comprar:{ip}", limite=18, janela_segundos=60):
            return "Muitas tentativas. Aguarde alguns segundos e tente novamente.", 429

    if method == "POST" and path == "/api/quiz/submit":
        if excedeu_rate_limit(f"post_quiz:{ip}", limite=40, janela_segundos=60):
            return jsonify({"ok": False, "error": "rate_limited"}), 429

    if method == "POST" and path == "/webhook/infinitypay":
        if excedeu_rate_limit(f"post_webhook:{ip}", limite=180, janela_segundos=60):
            return jsonify({"msg": "Rate limited"}), 429

    if method == "POST" and path == "/admin/login":
        if excedeu_rate_limit(f"post_admin_login:{ip}", limite=12, janela_segundos=60):
            return "Muitas tentativas de login. Aguarde alguns segundos.", 429

    if method == "POST" and path == "/login":
        if excedeu_rate_limit(f"post_cliente_login:{ip}", limite=12, janela_segundos=60):
            return "Muitas tentativas de login. Aguarde alguns segundos.", 429

    if method == "GET" and path == "/api/client/email-status":
        if excedeu_rate_limit(f"get_cliente_email_status:{ip}", limite=80, janela_segundos=60):
            return jsonify({"ok": False, "error": "rate_limited"}), 429

    if method == "POST" and path == "/api/client/lead-upgrade-click":
        if excedeu_rate_limit(f"post_cliente_lead_upgrade:{ip}", limite=50, janela_segundos=60):
            return jsonify({"ok": False, "error": "rate_limited"}), 429

    if method == "POST" and path == "/minha-conta/afiliados/ativar":
        if excedeu_rate_limit(f"post_cliente_afiliado_ativar:{ip}", limite=10, janela_segundos=60):
            return "Muitas tentativas. Aguarde alguns segundos.", 429

    if method == "POST" and path == "/minha-conta/afiliados/editar-link":
        if excedeu_rate_limit(f"post_cliente_afiliado_editar_link:{ip}", limite=12, janela_segundos=60):
            return "Muitas tentativas. Aguarde alguns segundos.", 429

    if method == "POST" and path == "/login/recuperar-senha":
        if excedeu_rate_limit(f"post_cliente_recover:{ip}", limite=8, janela_segundos=60):
            return "Muitas tentativas. Aguarde alguns segundos.", 429

    if method == "POST" and path == "/login/primeiro-acesso":
        if excedeu_rate_limit(f"post_cliente_primeiro_acesso:{ip}", limite=10, janela_segundos=60):
            return "Muitas tentativas. Aguarde alguns segundos.", 429

    if method == "POST" and path == "/login/confirmar-codigo":
        if excedeu_rate_limit(f"post_cliente_confirmar_codigo:{ip}", limite=14, janela_segundos=60):
            return "Muitas tentativas. Aguarde alguns segundos.", 429

    if path.startswith("/admin") and method in {"POST", "PUT", "PATCH", "DELETE"}:
        token = (request.form.get("csrf_token") or request.headers.get(CSRF_HEADER_NAME) or "").strip()
        if not validar_csrf_token(token):
            return "Falha de validacao CSRF.", 403


@app.after_request
def aplicar_headers_seguranca(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=()")
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")

    content_type = (response.headers.get("Content-Type") or "").lower()
    if "text/html" in content_type:
        response.headers.setdefault("Content-Security-Policy", _cspr_headers())

    if request.is_secure:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains; preload")

    if request.path.startswith("/admin"):
        response.headers.setdefault("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        response.headers.setdefault("Pragma", "no-cache")
    if (
        request.path.startswith("/login")
        or request.path.startswith("/minha-conta")
        or request.path.startswith("/sucesso")
    ):
        response.headers.setdefault("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        response.headers.setdefault("Pragma", "no-cache")

    if getattr(g, "clear_client_remember_cookie", False):
        response.delete_cookie(
            CLIENT_REMEMBER_COOKIE,
            path="/",
            secure=SESSION_COOKIE_SECURE,
            samesite=_remember_cookie_samesite()
        )

    try:
        response = _injetar_theme_global(response)
    except Exception:
        pass

    return response


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
    redirect_sucesso = montar_url_sucesso_order(order_id)

    payload = {
        "handle": HANDLE,
        "webhook_url": WEBHOOK_URL,
        "redirect_url": redirect_sucesso,
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
                senha=senha,
                nome_cliente=order.get("nome")
            )
            return True
        except Exception as e:
            tentativas += 1
            registrar_falha_email(order["order_id"], tentativas, str(e))
            time.sleep(5)

    return False

# ======================================================
# ROTAS PÃƒÅ¡BLICAS
# ======================================================

@app.route("/assets/<path:filename>")
def serve_assets(filename):
    return send_from_directory("assets", filename)


@app.route("/favicon.ico")
def favicon():
    return send_from_directory("assets/favicons", "trx_bull_exact_favicon.ico")


@app.route("/")
def home():
    afiliado = carregar_afiliado_contexto()
    return render_template(
        "index.html",
        affiliate=afiliado,
        checkout_suffix=montar_checkout_suffix(afiliado)
    )


@app.route("/diagnostico-de-perfil-trx")
def diagnostico_perfil_trx():
    obter_quiz_user_key()
    afiliado = carregar_afiliado_contexto()
    return render_template(
        "quiz.html",
        affiliate=afiliado,
        checkout_suffix=montar_checkout_suffix(afiliado)
    )


@app.route("/quiz")
def quiz():
    destino = "/diagnostico-de-perfil-trx"
    if request.query_string:
        destino = f"{destino}?{request.query_string.decode('utf-8', errors='ignore')}"
    return redirect(destino, code=302)


@app.route("/termos")
def termos():
    return render_template("termos.html")


@app.route("/privacidade")
def privacidade():
    return render_template("privacidade.html")


@app.route("/contato")
def contato():
    return render_template("contato.html")


@app.route("/sucesso/<order_id>")
def checkout_sucesso(order_id):
    order_id = (order_id or "").strip()
    token = (request.args.get("t") or "").strip()
    if not validar_token_sucesso_order(order_id, token):
        return "Link de sucesso invalido.", 403

    order = buscar_order_por_id(order_id)
    if not order:
        return "Pedido nao encontrado.", 404

    status_pago = (order.get("status") or "").strip().upper() == "PAGO"
    plano_id = (order.get("plano") or "").strip().lower()
    plano_info = PLANOS.get(plano_id, {})
    plano_nome = plano_info.get("nome", plano_id or "Plano")
    nome_cliente = normalizar_nome(order.get("nome") or "")

    if status_pago:
        email = normalizar_email(order.get("email") or "")
        if EMAIL_RE.fullmatch(email):
            try:
                garantir_conta_cliente_para_order(order, enviar_email_credenciais=False)
            except Exception as exc:
                print(f"[CLIENTE] Falha ao garantir conta no sucesso {order_id}: {exc}", flush=True)

            limpar_sessao_cliente()
            session[CLIENT_SESSION_EMAIL_KEY] = email
            session["nome"] = nome_cliente
            session["email"] = email
            session["telefone"] = normalizar_telefone(order.get("telefone") or "")
            session.permanent = True
            atualizar_ultimo_login_conta_cliente(email)

    refresh_url = f"/sucesso/{order_id}?t={token}"
    return render_template(
        "purchase_success.html",
        order_id=order_id,
        plano_nome=plano_nome,
        nome_cliente=nome_cliente,
        status_pago=status_pago,
        redirect_seconds=3,
        refresh_seconds=2,
        redirect_url="/minha-conta",
        refresh_url=refresh_url,
    )


@app.route("/api/client/email-status")
def api_cliente_email_status():
    email = normalizar_email(request.args.get("email") or "")
    status = verificar_status_email_cliente(email)
    if not status["valid"]:
        return jsonify({
            "ok": True,
            "valid": False,
            "exists": False,
            "status": "invalid",
            "requires_password_setup": False,
            "message": "Digite um e-mail valido."
        })

    if status["exists"]:
        if status["status"] == "setup_required":
            mensagem = "Primeiro acesso detectado. Clique em Entrar para receber o codigo de 6 digitos no e-mail."
        elif status["status"] == "paid_order":
            mensagem = "E-mail encontrado em compra aprovada. Clique em Entrar para preparar seu primeiro acesso."
        else:
            mensagem = "E-mail encontrado no banco."
    else:
        mensagem = "Este e-mail nao existe no banco."

    return jsonify({
        "ok": True,
        "valid": True,
        "exists": bool(status["exists"]),
        "status": status["status"],
        "requires_password_setup": bool(status.get("requires_password_setup")),
        "message": mensagem,
    })


@app.route("/api/client/lead-upgrade-click", methods=["POST"])
def api_cliente_lead_upgrade_click():
    email = obter_email_cliente_logado()
    if not EMAIL_RE.fullmatch(email):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    token = (
        request.headers.get(CSRF_HEADER_NAME)
        or payload.get("csrf_token")
        or request.form.get("csrf_token")
        or ""
    ).strip()
    if not validar_csrf_token(token):
        return jsonify({"ok": False, "error": "csrf_invalid"}), 403

    target_plan = (payload.get("target_plan") or "").strip().lower()
    source = (payload.get("source") or "client_area_upsell").strip()[:80]
    if target_plan not in PLANOS or int(PLANOS[target_plan].get("preco") or 0) <= 0:
        return jsonify({"ok": False, "error": "invalid_target_plan"}), 400

    pedidos = listar_pedidos_pagos_por_email(email, limite=30)
    plano_origem = None
    order_id_origem = None
    affiliate_slug = ""
    checkout_slug = target_plan

    for pedido in pedidos:
        plano_id = (pedido.get("plano") or "").strip().lower()
        if plano_id not in PLANOS:
            continue
        if int(PLANOS[plano_id].get("preco") or 0) > 0:
            continue
        exp = montar_expiracao_pedido(pedido)
        if not exp or not exp.get("ativo"):
            continue

        plano_origem = plano_id
        order_id_origem = pedido.get("order_id")
        affiliate_candidato = normalizar_slug_afiliado(pedido.get("affiliate_slug") or "")
        if slug_afiliado_valido(affiliate_candidato):
            affiliate_slug = affiliate_candidato
        checkout_slug = montar_plano_checkout(target_plan, affiliate_slug or None)
        break

    if not plano_origem:
        return jsonify({
            "ok": False,
            "error": "free_plan_not_active",
            "message": "Upsell disponivel apenas para conta com plano gratuito ativo."
        }), 400

    if not affiliate_slug:
        referral = buscar_indicacao_afiliado_por_email(email)
        referral_slug = normalizar_slug_afiliado((referral or {}).get("affiliate_slug") or "")
        if slug_afiliado_valido(referral_slug):
            affiliate_slug = referral_slug
            checkout_slug = montar_plano_checkout(target_plan, affiliate_slug)

    registrar_lead_upgrade_cliente(
        email=email,
        order_id=order_id_origem,
        current_plan=plano_origem,
        target_plan=target_plan,
        source=source or "client_area_upsell",
        affiliate_slug=affiliate_slug,
        checkout_slug=checkout_slug,
        ip_address=obter_ip_request() or (request.remote_addr or ""),
        user_agent=(request.headers.get("User-Agent") or "")[:300]
    )

    return jsonify({
        "ok": True,
        "checkout_slug": checkout_slug
    })


@app.route("/login", methods=["GET", "POST"])
def cliente_login():
    if cliente_logado():
        return redirect("/minha-conta")

    erro = ""
    info_key = (request.args.get("info") or "").strip()
    info = info_key
    if info_key == "senha_ja_configurada":
        info = "Senha ja configurada. Entre com seu e-mail e senha."
    email_form = normalizar_email(request.args.get("email") or "")
    remember_checked = False

    if request.method == "POST":
        token = (request.form.get("csrf_token") or "").strip()
        if not validar_csrf_token(token):
            return "Falha de validacao CSRF.", 403

        email_form = normalizar_email(request.form.get("email") or "")
        senha = request.form.get("senha") or ""
        remember_checked = (request.form.get("remember_me") or "").strip().lower() in {"1", "on", "true", "yes"}

        if not EMAIL_RE.fullmatch(email_form):
            erro = "Informe um e-mail valido."
        else:
            conta = buscar_conta_cliente_por_email(email_form)
            if not conta:
                criado = provisionar_conta_cliente_por_email(email_form, enviar_email_credenciais=False)
                if criado:
                    conta = buscar_conta_cliente_por_email(email_form)
                if not conta:
                    erro = "Este e-mail nao existe no banco."

            if not erro and conta_cliente_requer_configuracao_senha(conta):
                try:
                    ok_fluxo, msg_fluxo = iniciar_fluxo_codigo_primeiro_acesso(
                        email=email_form,
                        conta=conta,
                        remember=remember_checked
                    )
                    if ok_fluxo:
                        return redirect(f"/login/confirmar-codigo?info=codigo_enviado&email={quote(email_form, safe='')}")
                    erro = msg_fluxo or "Nao foi possivel iniciar o primeiro acesso."
                except Exception as exc:
                    print(f"[CLIENTE] Falha ao enviar codigo de primeiro acesso para {email_form}: {exc}", flush=True)
                    erro = "Nao foi possivel enviar o codigo agora. Tente novamente em instantes."

            if not erro and conta:
                senha_hash = (conta.get("password_hash") or "").strip()
                if not senha_hash:
                    erro = "Sua conta ainda nao tem senha ativa. Clique em Entrar para gerar o codigo de primeiro acesso."
                elif not senha:
                    erro = "Informe sua senha para entrar."
                elif not check_password_hash(senha_hash, senha):
                    erro = "Email ou senha invalidos."
                else:
                    return autenticar_cliente_resposta(email_form, remember=remember_checked)

    return render_template(
        "client_login.html",
        csrf_token=gerar_csrf_token(),
        erro=erro,
        info=info,
        email=email_form,
        remember_checked=remember_checked
    )


@app.route("/login/recuperar-senha", methods=["GET", "POST"])
def cliente_recuperar_senha():
    if cliente_logado():
        return redirect("/minha-conta")

    erro = ""
    info = (request.args.get("info") or "").strip()
    email_form = normalizar_email(request.args.get("email") or "")

    if request.method == "POST":
        token = (request.form.get("csrf_token") or "").strip()
        if not validar_csrf_token(token):
            return "Falha de validacao CSRF.", 403

        email_form = normalizar_email(request.form.get("email") or "")
        if not EMAIL_RE.fullmatch(email_form):
            erro = "Informe um e-mail valido."
        else:
            enviado = False
            try:
                ok = iniciar_recuperacao_senha_cliente(email_form)
                if ok:
                    enviado = True
                    info = "Enviamos uma senha temporaria para seu e-mail."
                else:
                    provisionado = provisionar_conta_cliente_por_email(email_form)
                    if provisionado:
                        enviado = True
                        info = "Criamos sua conta e enviamos a senha temporaria para seu e-mail."
            except Exception as exc:
                print(f"[CLIENTE] Falha ao recuperar senha para {email_form}: {exc}", flush=True)
                erro = "Nao foi possivel processar agora. Tente novamente em instantes."

            if not enviado and not erro:
                erro = "Este e-mail nao existe no banco."

    return render_template(
        "client_recover_password.html",
        csrf_token=gerar_csrf_token(),
        erro=erro,
        info=info,
        email=email_form
    )


@app.route("/login/primeiro-acesso", methods=["GET", "POST"])
def cliente_primeiro_acesso():
    email = normalizar_email(session.get(CLIENT_PENDING_EMAIL_KEY))
    if not EMAIL_RE.fullmatch(email):
        email_em_validacao = normalizar_email(session.get(CLIENT_VERIFY_EMAIL_KEY))
        if EMAIL_RE.fullmatch(email_em_validacao):
            return redirect(f"/login/confirmar-codigo?email={quote(email_em_validacao, safe='')}")
        return redirect("/login")

    conta = buscar_conta_cliente_por_email(email)
    if not conta:
        limpar_sessao_cliente()
        return redirect("/login")

    if not conta.get("first_access_required"):
        remember_pending = (session.get(CLIENT_PENDING_REMEMBER_KEY) or "").strip() == "1"
        return autenticar_cliente_resposta(email, remember=remember_pending)

    erro = ""

    if request.method == "POST":
        token = (request.form.get("csrf_token") or "").strip()
        if not validar_csrf_token(token):
            return "Falha de validacao CSRF.", 403

        senha_nova = request.form.get("senha_nova") or ""
        senha_repetida = request.form.get("senha_repetida") or ""

        ok, msg = senha_forte_valida(senha_nova)
        if not ok:
            erro = msg
        elif senha_nova != senha_repetida:
            erro = "As senhas nao conferem."
        elif conta.get("password_hash") and check_password_hash(conta["password_hash"], senha_nova):
            erro = "A nova senha precisa ser diferente da senha temporaria."
        else:
            senha_hash_nova = generate_password_hash(senha_nova)
            ok_confirm = confirmar_senha_conta_cliente(email, senha_hash_nova)
            if not ok_confirm:
                erro = "Falha ao criar sua senha. Tente novamente."
            else:
                remember_pending = (session.get(CLIENT_PENDING_REMEMBER_KEY) or "").strip() == "1"
                return autenticar_cliente_resposta(
                    email,
                    remember=remember_pending,
                    redirect_path="/minha-conta?info=senha_criada"
                )

    return render_template(
        "client_first_access.html",
        csrf_token=gerar_csrf_token(),
        email=email,
        erro=erro
    )


@app.route("/login/confirmar-codigo", methods=["GET", "POST"])
def cliente_confirmar_codigo():
    erro = ""
    info = ""

    email_session = normalizar_email(session.get(CLIENT_VERIFY_EMAIL_KEY))
    email_hint = normalizar_email(request.args.get("email") or request.form.get("email") or "")

    if not EMAIL_RE.fullmatch(email_session) and EMAIL_RE.fullmatch(email_hint):
        conta_hint = buscar_conta_cliente_por_email(email_hint)
        if conta_cliente_requer_configuracao_senha(conta_hint):
            session[CLIENT_VERIFY_EMAIL_KEY] = email_hint
            email_session = email_hint

    email = normalizar_email(session.get(CLIENT_VERIFY_EMAIL_KEY) or email_session)
    if not EMAIL_RE.fullmatch(email):
        return redirect("/login")

    conta = buscar_conta_cliente_por_email(email)
    if not conta:
        limpar_sessao_cliente()
        return redirect("/login")

    if not conta_cliente_requer_configuracao_senha(conta):
        session.pop(CLIENT_VERIFY_EMAIL_KEY, None)
        session.pop(CLIENT_PENDING_EMAIL_KEY, None)
        return redirect(f"/login?info=senha_ja_configurada&email={quote(email, safe='')}")

    info_key = (request.args.get("info") or "").strip().lower()
    if info_key == "codigo_enviado":
        info = "Enviamos um codigo de 6 digitos para seu e-mail."
    elif info_key == "codigo_reenviado":
        info = "Enviamos um novo codigo para seu e-mail."

    if request.method == "POST":
        token = (request.form.get("csrf_token") or "").strip()
        if not validar_csrf_token(token):
            return "Falha de validacao CSRF.", 403

        action = (request.form.get("action") or "verify").strip().lower()
        remember_pending = (session.get(CLIENT_PENDING_REMEMBER_KEY) or "").strip() == "1"

        if action == "resend":
            try:
                ok_fluxo, msg_fluxo = iniciar_fluxo_codigo_primeiro_acesso(
                    email=email,
                    conta=conta,
                    remember=remember_pending
                )
                if ok_fluxo:
                    info = "Enviamos um novo codigo para seu e-mail."
                    conta = buscar_conta_cliente_por_email(email) or conta
                else:
                    erro = msg_fluxo or "Nao foi possivel reenviar o codigo agora."
            except Exception as exc:
                print(f"[CLIENTE] Falha ao reenviar codigo para {email}: {exc}", flush=True)
                erro = "Nao foi possivel reenviar o codigo agora. Tente novamente em instantes."
        else:
            codigo = re.sub(r"\D", "", request.form.get("codigo") or "")[:6]
            tentativas_atuais = int(conta.get("verification_attempts") or 0)
            hash_salvo = (conta.get("verification_code_hash") or "").strip()
            expira_em = conta.get("verification_expires_at")

            ttl_restante = 0
            if expira_em:
                agora_local = datetime.now(expira_em.tzinfo) if getattr(expira_em, "tzinfo", None) else datetime.now()
                ttl_restante = max(0, int((expira_em - agora_local).total_seconds()))

            if tentativas_atuais >= CLIENT_CODE_MAX_ATTEMPTS:
                limpar_codigo_cliente(email)
                erro = "Codigo bloqueado por excesso de tentativas. Reenvie para gerar um novo."
            elif not hash_salvo or ttl_restante <= 0:
                erro = "Codigo expirado. Clique em reenviar para gerar um novo."
            elif len(codigo) != 6:
                erro = "Digite o codigo de 6 digitos."
            else:
                hash_digitado = hash_codigo_validacao(email, codigo)
                if not hmac.compare_digest(hash_digitado, hash_salvo):
                    incrementar_tentativa_codigo_cliente(email)
                    conta = buscar_conta_cliente_por_email(email) or conta
                    tentativas_atuais = int(conta.get("verification_attempts") or 0)
                    restantes = max(0, CLIENT_CODE_MAX_ATTEMPTS - tentativas_atuais)
                    if restantes <= 0:
                        limpar_codigo_cliente(email)
                        erro = "Codigo bloqueado por excesso de tentativas. Reenvie para gerar um novo."
                    else:
                        erro = f"Codigo invalido. Restam {restantes} tentativa(s)."
                else:
                    limpar_codigo_cliente(email)
                    session[CLIENT_PENDING_EMAIL_KEY] = email
                    session.pop(CLIENT_VERIFY_EMAIL_KEY, None)
                    return redirect("/login/primeiro-acesso")

    conta = buscar_conta_cliente_por_email(email) or conta
    tentativas = int(conta.get("verification_attempts") or 0)
    expira_em = conta.get("verification_expires_at")
    ttl_seconds = 0
    if expira_em:
        agora_local = datetime.now(expira_em.tzinfo) if getattr(expira_em, "tzinfo", None) else datetime.now()
        ttl_seconds = max(0, int((expira_em - agora_local).total_seconds()))

    mailbox = resolver_link_caixa_email(email)
    return render_template(
        "client_verify_code.html",
        csrf_token=gerar_csrf_token(),
        email=email,
        email_masked=mascarar_email_compacto(email),
        erro=erro,
        info=info,
        ttl_seconds=ttl_seconds,
        max_attempts=CLIENT_CODE_MAX_ATTEMPTS,
        attempts_used=max(0, min(CLIENT_CODE_MAX_ATTEMPTS, tentativas)),
        mailbox_url=mailbox.get("url"),
        mailbox_label=mailbox.get("label"),
    )


@app.route("/logout", methods=["POST"])
def cliente_logout():
    token = (request.form.get("csrf_token") or "").strip()
    if not validar_csrf_token(token):
        return "Falha de validacao CSRF.", 403

    email = obter_email_cliente_logado()
    if EMAIL_RE.fullmatch(email):
        limpar_remember_token_cliente(email)

    limpar_sessao_cliente()
    response = redirect("/login")
    response.delete_cookie(
        CLIENT_REMEMBER_COOKIE,
        path="/",
        secure=SESSION_COOKIE_SECURE,
        samesite=_remember_cookie_samesite()
    )
    return response


@app.route("/minha-conta/afiliados/ativar", methods=["POST"])
def cliente_ativar_afiliacao():
    token = (request.form.get("csrf_token") or request.headers.get(CSRF_HEADER_NAME) or "").strip()
    if not validar_csrf_token(token):
        return "Falha de validacao CSRF.", 403

    email = obter_email_cliente_logado()
    if not EMAIL_RE.fullmatch(email):
        limpar_sessao_cliente()
        return redirect("/login")

    conta = buscar_conta_cliente_por_email(email)
    if not conta:
        limpar_sessao_cliente()
        return redirect("/login")

    accept_terms = (request.form.get("accept_affiliate_terms") or "").strip().lower()
    if accept_terms not in {"1", "true", "on", "yes"}:
        return redirect("/minha-conta?info=afiliado_termos_obrigatorios#afiliados")

    terms_accepted_at = datetime.now(timezone.utc)
    terms_accepted_ip = (obter_ip_request() or request.remote_addr or "").strip()[:64] or None

    nome_conta = normalizar_nome(descriptografar_texto_cliente(conta.get("nome")) or "")
    telefone_conta = normalizar_telefone(descriptografar_texto_cliente(conta.get("telefone")) or "") or None

    afiliado_existente = buscar_afiliado_por_email(email, apenas_ativos=False)
    if afiliado_existente:
        slug_existente = normalizar_slug_afiliado(afiliado_existente.get("slug") or "")
        if not slug_afiliado_valido(slug_existente):
            return redirect("/minha-conta?info=afiliado_erro#afiliados")

        if not bool(afiliado_existente.get("ativo")):
            atualizar_afiliado(
                slug_atual=slug_existente,
                slug_novo=slug_existente,
                nome=nome_conta or afiliado_existente.get("nome") or "Afiliado TRX",
                email=email,
                telefone=telefone_conta or afiliado_existente.get("telefone") or None,
                ativo=True,
                terms_accepted_at=terms_accepted_at,
                terms_accepted_ip=terms_accepted_ip,
                terms_version=AFFILIATE_TERMS_VERSION
            )
            return redirect("/minha-conta?info=afiliado_reativado#afiliados")

        if not afiliado_existente.get("terms_accepted_at"):
            atualizar_afiliado(
                slug_atual=slug_existente,
                slug_novo=slug_existente,
                nome=nome_conta or afiliado_existente.get("nome") or "Afiliado TRX",
                email=email,
                telefone=telefone_conta or afiliado_existente.get("telefone") or None,
                ativo=bool(afiliado_existente.get("ativo")),
                terms_accepted_at=terms_accepted_at,
                terms_accepted_ip=terms_accepted_ip,
                terms_version=AFFILIATE_TERMS_VERSION
            )
            return redirect("/minha-conta?info=afiliado_termos_aceitos#afiliados")

        return redirect("/minha-conta?info=afiliado_existente#afiliados")

    slug_novo = gerar_slug_afiliado_unico(nome_conta, email)
    if not slug_novo:
        return redirect("/minha-conta?info=afiliado_erro#afiliados")

    nome_afiliado = nome_conta or f"Afiliado {email.split('@', 1)[0]}"
    criado = criar_afiliado(
        slug=slug_novo,
        nome=nome_afiliado,
        email=email,
        telefone=telefone_conta,
        ativo=True,
        terms_accepted_at=terms_accepted_at,
        terms_accepted_ip=terms_accepted_ip,
        terms_version=AFFILIATE_TERMS_VERSION
    )

    if not criado:
        afiliado_slug = buscar_afiliado_por_slug(slug_novo, apenas_ativos=False)
        if afiliado_slug and normalizar_email(afiliado_slug.get("email")) == email:
            return redirect("/minha-conta?info=afiliado_existente#afiliados")
        return redirect("/minha-conta?info=afiliado_erro#afiliados")

    return redirect("/minha-conta?info=afiliado_criado#afiliados")


@app.route("/minha-conta/afiliados/editar-link", methods=["POST"])
def cliente_editar_link_afiliado():
    token = (request.form.get("csrf_token") or request.headers.get(CSRF_HEADER_NAME) or "").strip()
    if not validar_csrf_token(token):
        return "Falha de validacao CSRF.", 403

    email = obter_email_cliente_logado()
    if not EMAIL_RE.fullmatch(email):
        limpar_sessao_cliente()
        return redirect("/login")

    afiliado_existente = buscar_afiliado_por_email(email, apenas_ativos=False)
    if not afiliado_existente:
        return redirect("/minha-conta?info=afiliado_erro#afiliados")

    slug_atual = normalizar_slug_afiliado(afiliado_existente.get("slug") or "")
    if not slug_afiliado_valido(slug_atual):
        return redirect("/minha-conta?info=afiliado_erro#afiliados")

    slug_novo = normalizar_slug_afiliado(request.form.get("affiliate_slug") or "")
    if not slug_afiliado_valido(slug_novo):
        return redirect("/minha-conta?info=afiliado_slug_invalido#afiliados")

    link_saved_at = afiliado_existente.get("link_saved_at")
    link_saved_momento = datetime.now(timezone.utc)

    if slug_novo == slug_atual:
        if link_saved_at:
            return redirect("/minha-conta?info=afiliado_link_sem_alteracao#afiliados")

        confirmado = atualizar_afiliado(
            slug_atual=slug_atual,
            slug_novo=slug_atual,
            nome=normalizar_nome(afiliado_existente.get("nome") or "") or f"Afiliado {email.split('@', 1)[0]}",
            email=email,
            telefone=normalizar_telefone(afiliado_existente.get("telefone") or "") or None,
            ativo=bool(afiliado_existente.get("ativo")),
            link_saved_at=link_saved_momento
        )
        if not confirmado:
            return redirect("/minha-conta?info=afiliado_erro#afiliados")

        return redirect("/minha-conta?info=afiliado_link_confirmado#afiliados")

    conflito = buscar_afiliado_por_slug(slug_novo, apenas_ativos=False)
    if conflito and normalizar_email(conflito.get("email") or "") != email:
        return redirect("/minha-conta?info=afiliado_slug_indisponivel#afiliados")

    atualizado = atualizar_afiliado(
        slug_atual=slug_atual,
        slug_novo=slug_novo,
        nome=normalizar_nome(afiliado_existente.get("nome") or "") or f"Afiliado {email.split('@', 1)[0]}",
        email=email,
        telefone=normalizar_telefone(afiliado_existente.get("telefone") or "") or None,
        ativo=bool(afiliado_existente.get("ativo")),
        link_saved_at=link_saved_momento
    )
    if not atualizado:
        return redirect("/minha-conta?info=afiliado_erro#afiliados")

    if session.get("affiliate_slug") == slug_atual:
        session["affiliate_slug"] = slug_novo

    return redirect("/minha-conta?info=afiliado_link_atualizado#afiliados")


@app.route("/minha-conta")
def cliente_area():
    email = obter_email_cliente_logado()
    if not EMAIL_RE.fullmatch(email):
        limpar_sessao_cliente()
        return redirect("/login")

    conta = buscar_conta_cliente_por_email(email)
    if not conta:
        limpar_sessao_cliente()
        return redirect("/login")

    conta_nome_decrypt = descriptografar_texto_cliente(conta.get("nome"))
    conta_telefone_decrypt = descriptografar_texto_cliente(conta.get("telefone"))
    conta_view = dict(conta)
    conta_view["nome"] = conta_nome_decrypt
    conta_view["telefone"] = conta_telefone_decrypt
    info_key = (request.args.get("info") or "").strip().lower()
    info_messages = {
        "senha_criada": "Senha criada com sucesso.",
        "afiliado_criado": "Afiliacao ativada com sucesso. Agora salve seu link para liberar a copia.",
        "afiliado_reativado": "Sua afiliacao foi reativada com sucesso.",
        "afiliado_termos_aceitos": "Termos e condicoes aceitos com sucesso para o programa de afiliados.",
        "afiliado_existente": "Sua conta ja esta afiliada.",
        "afiliado_termos_obrigatorios": "Para liberar sua indicacao, aceite os Termos e Condicoes do programa.",
        "afiliado_slug_invalido": "Link invalido. Use apenas letras minusculas, numeros e hifen (2 a 60 caracteres).",
        "afiliado_slug_indisponivel": "Este link ja esta em uso por outro afiliado.",
        "afiliado_link_sem_alteracao": "Seu link de afiliado ja esta salvo com esse valor.",
        "afiliado_link_confirmado": "Link salvo com sucesso. Copia liberada.",
        "afiliado_link_atualizado": "Link de afiliado atualizado com sucesso.",
        "afiliado_erro": "Nao foi possivel ativar sua afiliacao agora. Tente novamente."
    }
    info_message = info_messages.get(info_key, "")
    info_warn_keys = {
        "afiliado_erro",
        "afiliado_termos_obrigatorios",
        "afiliado_slug_invalido",
        "afiliado_slug_indisponivel",
    }
    info_message_level = "warn" if info_key in info_warn_keys else "ok"
    quiz_user_key = (session.get("quiz_user_key") or "").strip()
    diagnostico_concluido = existe_quiz_submission(
        account_email=email,
        user_key=quiz_user_key
    )

    pedidos = listar_pedidos_acesso_por_email(email, limite=30)
    pedidos_view = []
    for pedido in pedidos:
        plano_id = pedido.get("plano")
        plano_info = PLANOS.get(plano_id, {})
        status_norm = (pedido.get("status") or "").strip().upper()
        is_bonus = status_norm == "BONUS"
        exp = montar_expiracao_pedido(pedido)
        created_at_local = converter_data_para_timezone_admin(pedido.get("created_at"))
        expira_local = converter_data_para_timezone_admin(exp["expira_em"]) if exp else None
        pedidos_view.append({
            "order_id": pedido.get("order_id"),
            "nome": pedido.get("nome") or "",
            "plano_id": plano_id,
            "plano_nome": plano_info.get("nome", plano_id or "-"),
            "is_paid": int(plano_info.get("preco") or 0) > 0 and not is_bonus,
            "is_bonus": is_bonus,
            "access_label": "Bonus indicacao" if is_bonus else ("Pago" if int(plano_info.get("preco") or 0) > 0 else "Gratuito"),
            "created_at_fmt": created_at_local.strftime("%d/%m/%Y %H:%M") if created_at_local else "-",
            "expira_em_fmt": expira_local.strftime("%d/%m/%Y %H:%M") if expira_local else "-",
            "ativo": bool(exp and exp.get("ativo")),
            "dias_restantes": int(exp.get("dias_restantes") or 0) if exp else 0,
        })

    conta_nome = conta_nome_decrypt or (pedidos_view[0]["nome"] if pedidos_view else "")
    ativos = [p for p in pedidos_view if p.get("ativo")]
    ultimo_pedido = pedidos_view[0] if pedidos_view else None
    client_notifications = []
    if not diagnostico_concluido:
        client_notifications.append({
            "id": "diagnostico_pendente",
            "level": "warn",
            "title": "Diagnostico pendente",
            "message": "Complete o Diagnostico de Perfil TRX para liberar recomendacao de plano e completar seu perfil.",
            "action_url": "/diagnostico-de-perfil-trx",
            "action_label": "Fazer diagnostico"
        })

    if not ativos:
        client_notifications.append({
            "id": "sem_plano_ativo",
            "level": "info",
            "title": "Nenhum plano ativo",
            "message": "Seu acesso ativo expirou. Escolha um plano para voltar a operar.",
            "action_url": "/#planos",
            "action_label": "Ver planos"
        })
    else:
        for ativo in ativos:
            dias_restantes = int(ativo.get("dias_restantes") or 0)
            if dias_restantes <= 3:
                client_notifications.append({
                    "id": f"expira_{ativo.get('order_id')}",
                    "level": "warn",
                    "title": "Plano perto de expirar",
                    "message": f"{ativo.get('plano_nome')}: restam {dias_restantes} dia(s).",
                    "action_url": "/#planos",
                    "action_label": "Renovar agora"
                })
    notification_count = len(client_notifications)

    pedido_curva = None
    for pedido in pedidos:
        exp = montar_expiracao_pedido(pedido)
        if exp and exp.get("ativo"):
            pedido_curva = pedido
            break
    if not pedido_curva and pedidos:
        pedido_curva = pedidos[0]
    capital_chart = montar_curva_capital_plano(pedido_curva)

    plano_gratis_ativo = None
    pedido_gratis_ativo = None
    for pedido in pedidos:
        plano_id = (pedido.get("plano") or "").strip().lower()
        if plano_id not in PLANOS:
            continue
        if int(PLANOS[plano_id].get("preco") or 0) > 0:
            continue
        exp = montar_expiracao_pedido(pedido)
        if not exp or not exp.get("ativo"):
            continue
        plano_gratis_ativo = plano_id
        pedido_gratis_ativo = pedido
        break

    upsell_plans = []
    if plano_gratis_ativo:
        paid_plan_ids = ("trx-bronze", "trx-prata", "trx-gold", "trx-black")
        contracts_map = {
            "trx-bronze": "1 contrato",
            "trx-prata": "1-5 contratos",
            "trx-gold": "1-20 contratos",
            "trx-black": "1-300 contratos",
        }
        theme_class_map = {
            "trx-bronze": "plan-bronze",
            "trx-prata": "plan-prata",
            "trx-gold": "plan-gold",
            "trx-black": "plan-black",
        }

        vendas_por_plano = contar_pedidos_pagos_por_plano(paid_plan_ids)
        plano_mais_comprado = None
        plano_mais_comprado_count = 0
        for pid in paid_plan_ids:
            count = int(vendas_por_plano.get(pid) or 0)
            if count > plano_mais_comprado_count:
                plano_mais_comprado = pid
                plano_mais_comprado_count = count

        for plano_id in paid_plan_ids:
            plano_info = PLANOS.get(plano_id, {})
            preco_centavos = int(plano_info.get("preco") or 0)
            if preco_centavos <= 0:
                continue

            is_most_chosen = bool(plano_mais_comprado_count > 0 and plano_id == plano_mais_comprado)
            checkout_slug = montar_plano_checkout(plano_id, None)
            upsell_plans.append({
                "plano_id": plano_id,
                "nome": plano_info.get("nome", plano_id),
                "preco_centavos": preco_centavos,
                "preco_fmt": fmt_brl_from_centavos(preco_centavos),
                "checkout_slug": checkout_slug,
                "theme_class": theme_class_map.get(plano_id, ""),
                "contracts_text": contracts_map.get(plano_id, ""),
                "is_most_chosen": is_most_chosen,
                "tag_text": "Mais escolhido" if is_most_chosen else "Upgrade",
                "sales_count": int(vendas_por_plano.get(plano_id) or 0),
            })

    afiliado_cliente = buscar_afiliado_por_email(email, apenas_ativos=False)
    afiliado_cliente_view = montar_dados_afiliado_cliente(afiliado_cliente)
    affiliate_copy_pending_steps = []
    if not afiliado_cliente_view:
        affiliate_copy_pending_steps.extend([
            "Criar sua afiliacao.",
            "Salvar seu link de afiliado.",
            "Aceitar os Termos e Condicoes do programa."
        ])
    else:
        if not afiliado_cliente_view.get("ativo"):
            affiliate_copy_pending_steps.append("Ativar ou reativar sua afiliacao.")
        if not afiliado_cliente_view.get("link_saved"):
            affiliate_copy_pending_steps.append("Salvar seu link de afiliado.")
        if not afiliado_cliente_view.get("terms_accepted"):
            affiliate_copy_pending_steps.append("Aceitar os Termos e Condicoes do programa.")
    affiliate_copy_ready = bool(afiliado_cliente_view and not affiliate_copy_pending_steps)

    return render_template(
        "client_area.html",
        csrf_token=gerar_csrf_token(),
        conta=conta_view,
        conta_nome=conta_nome,
        email=email,
        info_message=info_message,
        info_message_level=info_message_level,
        diagnostico_concluido=diagnostico_concluido,
        client_notifications=client_notifications,
        notification_count=notification_count,
        install_video_id=CLIENT_INSTALL_VIDEO_ID,
        capital_chart=capital_chart,
        pedidos=pedidos_view,
        ativos=ativos,
        ultimo_pedido=ultimo_pedido,
        upsell_plans=upsell_plans,
        afiliado_cliente=afiliado_cliente_view,
        affiliate_copy_ready=affiliate_copy_ready,
        affiliate_copy_pending_steps=affiliate_copy_pending_steps,
        affiliate_commission_percent=int(AFFILIATE_COMMISSION_PERCENT)
    )


@app.route("/<affiliate_slug>")
def landing_afiliado(affiliate_slug):
    slug = normalizar_slug_afiliado(affiliate_slug)
    if not slug_afiliado_valido(slug):
        return "Pagina nao encontrada", 404

    afiliado = obter_afiliado_ativo(slug)
    if not afiliado:
        return "Pagina nao encontrada", 404

    session["affiliate_slug"] = afiliado["slug"]

    return render_template(
        "index.html",
        affiliate=afiliado,
        checkout_suffix=montar_checkout_suffix(afiliado)
    )


@app.route("/api/reports/monthly")
def api_reports_monthly():
    pasta_meses = os.path.join("assets", "meses")
    reports = []

    if os.path.isdir(pasta_meses):
        for arquivo in os.listdir(pasta_meses):
            parsed = parse_relatorio_mensal_nome(arquivo)
            if not parsed:
                continue
            reports.append(parsed)

    reports.sort(key=lambda item: item.get("_sort", (99, 99, 99, "")))

    acumulado = 0.0
    for item in reports:
        acumulado += float(item.get("_value") or 0.0)
        acumulado_fmt, acumulado_status = formatar_valor_brl_com_sinal(acumulado)
        item["cumulative_gain"] = acumulado_fmt
        item["cumulative_status"] = acumulado_status
        item.pop("_value", None)
        item.pop("_sort", None)

    # Garante que JAN feche com o mesmo acumulado oficial da curva anual.
    valor_final_fmt = f"{abs(ACUMULADO_FINAL_CICLO):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    acumulado_final_fmt = f"R$ {valor_final_fmt}"
    acumulado_final_status = "pos" if ACUMULADO_FINAL_CICLO > 0 else ("neg" if ACUMULADO_FINAL_CICLO < 0 else "neutral")
    for item in reports:
        if (item.get("month") or "").upper() == "JAN":
            item["cumulative_gain"] = acumulado_final_fmt
            item["cumulative_status"] = acumulado_final_status

    return jsonify({
        "ok": True,
        "reports": reports
    })


@app.route("/api/quiz/submit", methods=["POST"])
def api_quiz_submit():
    payload = request.get_json(silent=True) or {}

    answers = payload.get("answers")
    recommended_plan = (payload.get("recommended_plan") or "").strip().lower()
    next_level_plan = (payload.get("next_level_plan") or "").strip().lower() or None
    show_free_secondary = bool(payload.get("show_free_secondary"))
    reasons = payload.get("reasons") or []
    submission_id = (payload.get("submission_id") or "").strip()

    if not isinstance(answers, dict):
        return jsonify({"ok": False, "error": "answers invÃƒÂ¡lido"}), 400

    planos_validos = {"gratis", "bronze", "prata", "gold", "black"}
    if recommended_plan not in planos_validos:
        return jsonify({"ok": False, "error": "recommended_plan invÃƒÂ¡lido"}), 400

    if next_level_plan and next_level_plan not in planos_validos:
        return jsonify({"ok": False, "error": "next_level_plan invÃƒÂ¡lido"}), 400

    if not isinstance(reasons, list):
        reasons = []

    reasons_limpo = [str(item)[:300] for item in reasons[:5]]

    if not submission_id:
        submission_id = str(uuid.uuid4())

    inserido = registrar_quiz_submission(
        submission_id=submission_id,
        user_key=obter_quiz_user_key(),
        account_email=obter_email_cliente_logado() or None,
        ip_address=obter_ip_request(),
        user_agent=(request.headers.get("User-Agent") or "")[:300],
        answers=answers,
        recommended_plan=recommended_plan,
        next_level_plan=next_level_plan,
        show_free_secondary=show_free_secondary,
        reasons=reasons_limpo
    )

    return jsonify({
        "ok": True,
        "saved": inserido,
        "submission_id": submission_id
    })


@app.route("/checkout/<plano>")
def checkout(plano):
    registrar_usuario_online()
    plano_base, affiliate_slug = decompor_plano_checkout(plano)
    if plano_base not in PLANOS:
        return "Plano invÃƒÂ¡lido", 404

    afiliado = None
    if affiliate_slug:
        afiliado = buscar_afiliado_por_slug(affiliate_slug, apenas_ativos=True)
        if not afiliado:
            return "Afiliado invÃƒÂ¡lido", 404
        session["affiliate_slug"] = afiliado["slug"]

    return render_template(
        "checkout.html",
        csrf_token=gerar_csrf_token(),
        plano=plano,
        plano_base=plano_base,
        is_free_plan=PLANOS[plano_base]["preco"] <= 0,
        affiliate=afiliado,
        nome=session.get("nome", ""),
        email=session.get("email", ""),
        telefone=session.get("telefone", "")
    )
# Ã°Å¸Å¡Â« nunca permitir GET em /comprar
@app.route("/comprar", methods=["GET"])
def comprar_get():
    return redirect("/")

# Ã¢Å“â€¦ POST real
@app.route("/comprar", methods=["POST"])
def comprar():
    token = (request.form.get("csrf_token") or "").strip()
    if not validar_csrf_token(token):
        return "Falha de validacao CSRF.", 403

    nome_raw = request.form.get("nome")
    email_raw = request.form.get("email")
    telefone_raw = request.form.get("telefone")
    plano_checkout = (request.form.get("plano") or "").strip().lower()
    plano_id, affiliate_slug = decompor_plano_checkout(plano_checkout)
    afiliado_checkout = None

    if plano_id not in PLANOS:
        return "Dados invalidos", 400

    if affiliate_slug:
        afiliado_checkout = buscar_afiliado_por_slug(affiliate_slug, apenas_ativos=True)
        if not afiliado_checkout:
            return "Afiliado invalido", 400

    validacao_ok, dados = validar_cadastro_cliente(nome_raw, email_raw, telefone_raw)
    if not validacao_ok:
        return dados, 400

    nome = dados["nome"]
    email = dados["email"]
    telefone = dados["telefone"]

    session["nome"] = nome
    session["email"] = email
    session["telefone"] = telefone

    order_id = str(uuid.uuid4())
    ja_possui_historico = bool(listar_pedidos_pagos_por_email(email, limite=1))
    compra_evolucao_direta = bool(int(PLANOS.get(plano_id, {}).get("preco") or 0) > 0 and ja_possui_historico)
    afiliado_atribuido, referral_info = resolver_afiliado_para_compra(
        email=email,
        affiliate_slug_checkout=affiliate_slug,
        order_id=order_id,
        checkout_slug=plano_checkout or plano_id,
        forcar_direto=compra_evolucao_direta
    )

    salvar_order(
        order_id=order_id,
        plano=plano_id,
        nome=nome,
        email=email,
        telefone=telefone,
        checkout_slug=plano_checkout or plano_id,
        affiliate_slug=(afiliado_atribuido or {}).get("slug"),
        affiliate_nome=(afiliado_atribuido or {}).get("nome") or None,
        affiliate_email=(afiliado_atribuido or {}).get("email") or None,
        affiliate_telefone=(afiliado_atribuido or {}).get("telefone") or None,
    )

    plano_info = PLANOS[plano_id]

    if plano_info["preco"] <= 0:
        arquivo = None
        try:
            arquivo, senha = compactar_plano(plano_info["pasta"], PASTA_SAIDA)
            enviar_email(
                destinatario=email,
                nome_plano=plano_info["nome"],
                arquivo=arquivo,
                senha=senha,
                nome_cliente=nome,
            )

            marcar_order_processada(order_id)
            try:
                removidos = excluir_duplicados_gratis_mesmo_dia(order_id, email)
                if removidos > 0:
                    print(
                        f"[DUPLICADOS] Removidos {removidos} pedido(s) gratis duplicado(s) "
                        f"para {email} no mesmo dia.",
                        flush=True
                    )
            except Exception as exc:
                print(f"[DUPLICADOS] Falha ao limpar duplicados gratis ({order_id}): {exc}", flush=True)

            order_pago = buscar_order_por_id(order_id)
            try:
                garantir_conta_cliente_para_order(order_pago, enviar_email_credenciais=True)
            except Exception as exc:
                print(f"[CLIENTE] Falha ao preparar conta do cliente {order_id}: {exc}", flush=True)
            try:
                registrar_comissao_pedido_afiliado(order_pago)
            except Exception as exc:
                print(f"[AFILIADOS] Falha ao registrar comissao do pedido {order_id}: {exc}", flush=True)
            try:
                conceder_bonus_indicacao_pedido(order_pago)
            except Exception as exc:
                print(f"[AFILIADOS] Falha ao conceder bonus por indicacao {order_id}: {exc}", flush=True)
            registrar_compra_analytics(order_pago)
            agendar_whatsapp(order_id, minutos=WHATSAPP_DELAY_MINUTES)
            agendar_whatsapp_pos_pago(order_pago)
            return redirect(f"/sucesso/{order_id}?t={gerar_token_sucesso_order(order_id)}")
        except Exception as exc:
            registrar_falha_email(order_id, 1, str(exc))
            return "Falha ao enviar o acesso. Tente novamente em instantes.", 500
        finally:
            if arquivo and os.path.exists(arquivo):
                os.remove(arquivo)

    checkout_url = criar_checkout_dinamico(
        plano_id=plano_id,
        order_id=order_id,
        nome=nome,
        email=email,
        telefone=telefone,
    )

    return redirect(checkout_url)

# ======================================================
# WEBHOOK
# ======================================================

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    if not verificar_token_webhook():
        return jsonify({"msg": "Nao autorizado"}), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"msg": "Payload invalido"}), 400

    transaction_nsu = str(data.get("transaction_nsu") or "").strip()
    order_id = str(data.get("order_nsu") or "").strip()

    try:
        paid_amount = int(float(data.get("paid_amount") or 0))
    except (TypeError, ValueError):
        paid_amount = 0

    if not transaction_nsu or not order_id or paid_amount <= 0:
        return jsonify({"msg": "Ignorado"}), 200

    if transacao_ja_processada(transaction_nsu):
        return jsonify({"msg": "Ja processado"}), 200

    order = buscar_order_por_id(order_id)
    if not order or order.get("status") != "PENDENTE":
        return jsonify({"msg": "Pedido invalido"}), 200

    plano_id = order.get("plano")
    if plano_id not in PLANOS:
        return jsonify({"msg": "Plano invalido"}), 400

    plano_info = PLANOS[plano_id]
    preco_esperado = int(plano_info.get("preco") or 0)
    if preco_esperado > 0 and paid_amount < preco_esperado:
        return jsonify({"msg": "Pagamento insuficiente"}), 400

    arquivo = None
    try:
        arquivo, senha = compactar_plano(plano_info["pasta"], PASTA_SAIDA)
        sucesso = enviar_email_com_retry(order, plano_info, arquivo, senha)
        if sucesso:
            marcar_order_processada(order_id)
            marcar_transacao_processada(transaction_nsu)
            order_pago = buscar_order_por_id(order_id)
            try:
                garantir_conta_cliente_para_order(order_pago, enviar_email_credenciais=True)
            except Exception as exc:
                print(f"[CLIENTE] Falha ao preparar conta do cliente {order_id}: {exc}", flush=True)
            try:
                registrar_comissao_pedido_afiliado(order_pago, transaction_nsu=transaction_nsu)
            except Exception as exc:
                print(f"[AFILIADOS] Falha ao registrar comissao do webhook {order_id}: {exc}", flush=True)
            try:
                conceder_bonus_indicacao_pedido(order_pago)
            except Exception as exc:
                print(f"[AFILIADOS] Falha ao conceder bonus do webhook {order_id}: {exc}", flush=True)
            registrar_compra_analytics(order_pago, transaction_nsu=transaction_nsu)
            agendar_whatsapp_pos_pago(order_pago)
    except Exception as exc:
        print(f"Falha ao processar webhook {order_id}: {exc}", flush=True)
        return jsonify({"msg": "Erro no processamento"}), 500
    finally:
        if arquivo and os.path.exists(arquivo):
            os.remove(arquivo)

    return jsonify({"msg": "OK"}), 200

# ======================================================
# ADMIN
# ======================================================

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    ip = obter_ip_request() or (request.remote_addr or "0.0.0.0")

    if request.method == "POST":
        bloqueado, espera = login_bloqueado(ip)
        if bloqueado:
            return f"Acesso temporariamente bloqueado. Tente novamente em {espera}s.", 429

        senha_digitada = request.form.get("senha") or ""
        if verificar_senha_admin(senha_digitada):
            registrar_tentativa_login(ip, sucesso=True)
            session.clear()
            session["_csrf_token"] = secrets.token_urlsafe(32)
            session["admin"] = True
            session.permanent = True
            return redirect("/admin/dashboard")

        registrar_tentativa_login(ip, sucesso=False)
        return "Senha invalida", 403

    return render_template("admin_login.html")


@app.route("/admin/logout", methods=["POST"])
def admin_logout():
    session.clear()
    return redirect("/admin/login")


@app.route("/admin/backup/agora", methods=["POST"])
def admin_backup_agora():
    if not session.get("admin"):
        return jsonify({"ok": False, "message": "Nao autorizado"}), 403

    ok, msg = executar_backup_criptografado(trigger_type="manual")
    if ok:
        status = 200
    elif "em andamento em outro processo" in (msg or "").lower():
        status = 409
    else:
        status = 500
    return jsonify({"ok": ok, "message": msg}), status


@app.route("/admin/backup/logs")
def admin_backup_logs():
    if not session.get("admin"):
        return jsonify({"error": "unauthorized"}), 403

    itens = listar_backups_execucao(limit=30)
    for item in itens:
        for campo in ("started_at", "finished_at"):
            valor = item.get(campo)
            if hasattr(valor, "isoformat"):
                item[campo] = valor.isoformat()

    return jsonify({"ok": True, "items": itens})


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
            pedido["whatsapp_status"] = "telefone invÃƒÂ¡lido"

        if mensagens_enviadas > 0:
            pedido["whatsapp_status"] = f"{mensagens_enviadas} mensagem(ns) enviada(s)"

        data_local = converter_data_para_timezone_admin(pedido.get("created_at"))
        pedido["created_at_local"] = data_local.strftime("%d/%m/%Y %H:%M") if data_local else "-"
        pedido["nome_masked"] = mascarar_nome_compacto(pedido.get("nome"))
        pedido["email_masked"] = mascarar_email_compacto(pedido.get("email"))
        pedido["telefone_masked"] = mascarar_telefone_compacto(pedido.get("telefone"))
        pedido["affiliate_email_masked"] = mascarar_email_compacto(pedido.get("affiliate_email"))

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
                pedido.get("checkout_slug") or "",
                pedido.get("affiliate_slug") or "",
                pedido.get("affiliate_nome") or "",
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


def redirecionar_admin_afiliados(msg=None, ok=False):
    url = "/admin/afiliados"
    if msg:
        url += f"?ok={'1' if ok else '0'}&msg={quote(msg)}"
    return redirect(url)


@app.route("/admin/afiliados")
def admin_afiliados():
    if not session.get("admin"):
        return redirect("/admin/login")

    afiliados = listar_afiliados(include_inativos=True)
    return render_template(
        "admin_afiliados.html",
        afiliados=afiliados,
        msg=(request.args.get("msg") or "").strip(),
        ok=(request.args.get("ok") or "").strip() == "1",
        base_url=(PUBLIC_BASE_URL or request.host_url.rstrip("/"))
    )


@app.route("/admin/afiliados/adicionar", methods=["POST"])
def admin_afiliados_adicionar():
    if not session.get("admin"):
        return redirect("/admin/login")

    nome = (request.form.get("nome") or "").strip()
    slug_raw = (request.form.get("slug") or nome).strip()
    slug = normalizar_slug_afiliado(slug_raw)
    email = (request.form.get("email") or "").strip() or None
    telefone = (request.form.get("telefone") or "").strip() or None
    ativo = (request.form.get("ativo") or "").strip().lower() in ("1", "on", "true", "yes")

    if not nome:
        return redirecionar_admin_afiliados("Informe o nome do afiliado.")

    if email:
        email = normalizar_email(email)
        if not EMAIL_RE.fullmatch(email):
            return redirecionar_admin_afiliados("Email de afiliado invalido.")

    if telefone:
        telefone = normalizar_telefone(telefone)
        if len(telefone) not in (10, 11, 12, 13):
            return redirecionar_admin_afiliados("Telefone de afiliado invalido.")

    if not slug_afiliado_valido(slug):
        return redirecionar_admin_afiliados("Slug invalido ou reservado.")

    try:
        inserido = criar_afiliado(slug=slug, nome=nome, email=email, telefone=telefone, ativo=ativo)
    except Exception:
        return redirecionar_admin_afiliados("Erro ao adicionar afiliado.")

    if not inserido:
        return redirecionar_admin_afiliados("Ja existe um afiliado com esse slug.")

    return redirecionar_admin_afiliados("Afiliado adicionado com sucesso.", ok=True)


@app.route("/admin/afiliados/<slug>/editar", methods=["POST"])
def admin_afiliados_editar(slug):
    if not session.get("admin"):
        return redirect("/admin/login")

    slug_atual = normalizar_slug_afiliado(slug)
    slug_novo = normalizar_slug_afiliado((request.form.get("slug") or "").strip())
    nome = (request.form.get("nome") or "").strip()
    email = (request.form.get("email") or "").strip() or None
    telefone = (request.form.get("telefone") or "").strip() or None
    ativo = (request.form.get("ativo") or "").strip().lower() in ("1", "on", "true", "yes")

    if not slug_afiliado_valido(slug_atual):
        return redirecionar_admin_afiliados("Afiliado invalido.")

    if not nome:
        return redirecionar_admin_afiliados("Informe o nome do afiliado.")

    if email:
        email = normalizar_email(email)
        if not EMAIL_RE.fullmatch(email):
            return redirecionar_admin_afiliados("Email de afiliado invalido.")

    if telefone:
        telefone = normalizar_telefone(telefone)
        if len(telefone) not in (10, 11, 12, 13):
            return redirecionar_admin_afiliados("Telefone de afiliado invalido.")

    if not slug_afiliado_valido(slug_novo):
        return redirecionar_admin_afiliados("Novo slug invalido ou reservado.")

    try:
        atualizado = atualizar_afiliado(
            slug_atual=slug_atual,
            slug_novo=slug_novo,
            nome=nome,
            email=email,
            telefone=telefone,
            ativo=ativo
        )
    except Exception:
        return redirecionar_admin_afiliados("Erro ao editar afiliado. Verifique se o slug ja existe.")

    if not atualizado:
        return redirecionar_admin_afiliados("Afiliado nao encontrado.")

    return redirecionar_admin_afiliados("Afiliado atualizado com sucesso.", ok=True)


@app.route("/admin/afiliados/<slug>/excluir", methods=["POST"])
def admin_afiliados_excluir(slug):
    if not session.get("admin"):
        return redirect("/admin/login")

    slug = normalizar_slug_afiliado(slug)
    if not slug_afiliado_valido(slug):
        return redirecionar_admin_afiliados("Afiliado invalido.")

    removido = excluir_afiliado(slug)
    if not removido:
        return redirecionar_admin_afiliados("Afiliado nao encontrado.")

    if session.get("affiliate_slug") == slug:
        session.pop("affiliate_slug", None)

    return redirecionar_admin_afiliados("Afiliado excluido com sucesso.", ok=True)


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
        return jsonify({"error": "start invÃƒÂ¡lido (YYYY-MM-DD)"}), 400

    try:
        end = parse_iso_date(request.args.get("end"))
    except Exception:
        return jsonify({"error": "end invÃƒÂ¡lido (YYYY-MM-DD)"}), 400

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
        return jsonify({"error": "metric invÃƒÂ¡lida"}), 400
    if group_by not in group_validos:
        return jsonify({"error": "groupBy invÃƒÂ¡lido"}), 400
    if plan != "all" and plan not in PLANOS:
        return jsonify({"error": "plan invÃƒÂ¡lido"}), 400

    try:
        start = parse_iso_date(request.args.get("start"))
    except Exception:
        return jsonify({"error": "start invÃƒÂ¡lido (YYYY-MM-DD)"}), 400

    try:
        end = parse_iso_date(request.args.get("end"))
    except Exception:
        return jsonify({"error": "end invÃƒÂ¡lido (YYYY-MM-DD)"}), 400

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
        return "Pedido nÃƒÂ£o encontrado", 404

    link = gerar_link_whatsapp(pedido)
    if not link:
        return "Telefone do usuÃƒÂ¡rio nÃƒÂ£o encontrado/invÃƒÂ¡lido", 400

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
        return "Pedido nao encontrado", 404

    pedido_view = dict(pedido)
    pedido_view["nome_masked"] = mascarar_nome(pedido.get("nome"))
    pedido_view["email_masked"] = mascarar_email(pedido.get("email"))
    pedido_view["telefone_masked"] = mascarar_telefone(pedido.get("telefone"))
    pedido_view["affiliate_email_masked"] = mascarar_email(pedido.get("affiliate_email"))
    pedido_view["affiliate_telefone_masked"] = mascarar_telefone(pedido.get("affiliate_telefone"))

    return render_template("admin_pedido.html", pedido=pedido_view)

# ======================================================
# START
# ======================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)











