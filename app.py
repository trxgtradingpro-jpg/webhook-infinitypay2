from flask import (
    Flask, request, jsonify, render_template,
    redirect, session, send_from_directory
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
import hmac
import hashlib
import secrets
from collections import defaultdict, deque
from zoneinfo import ZoneInfo
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash

from compactador import compactar_plano
from email_utils import enviar_email, enviar_email_com_anexo
from whatsapp_sender import schedule_whatsapp
from backup_utils import criar_backup_criptografado, remover_backups_antigos

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
    registrar_falha_whatsapp_auto,
    registrar_quiz_submission,
    listar_afiliados,
    buscar_afiliado_por_slug,
    criar_afiliado,
    atualizar_afiliado,
    excluir_afiliado,
    registrar_backup_execucao,
    listar_backups_execucao,
    adquirir_lock_backup_distribuido,
    liberar_lock_backup_distribuido
)

print("ðŸš€ APP INICIADO", flush=True)

# ======================================================
# APP
# ======================================================

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# ======================================================
# SEGURANÃ‡A (ENV)
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
# WHATSAPP FOLLOW-UP (PLANO GRÃTIS)
# ======================================================

WHATSAPP_MENSAGEM = os.environ.get(
    "WHATSAPP_MENSAGEM",
    (
        "OlÃ¡ {nome}\n\n"
        "Seu {plano} foi liberado com sucesso âœ…\n\n"
        "Quero confirmar se conseguiu instalar corretamente.\n"
        "Caso tenha qualquer dÃºvida ou dificuldade, Ã© sÃ³ me chamar que te dou suporte imediato ðŸ¤\n\n"
        "Lembre-se de entrar na nossa comunidade para receber atualizaÃ§Ãµes do nosso robÃ´:\n"
        "https://chat.whatsapp.com/KPcaKf6OsaQHG2cUPAU1CE\n\n"
        "Estou Ã  disposiÃ§Ã£o."
    )
)
WHATSAPP_TEMPLATE = os.environ.get(
    "WHATSAPP_TEMPLATE",
    "âœ… {nome}, seu pagamento do {plano} foi confirmado. Qualquer dÃºvida pode me chamar!"
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

# ======================================================
# PLANOS (COM TESTE + GRÃTIS)
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
        "nome": "TRX GRÃTIS",
        "pasta": "Licencas/TRX GRATIS",
        "preco": 0,
        "gratis": True,
        "redirect_url": "https://sites.google.com/view/planogratuito/in%C3%ADcio"
    }
}

AFFILIATE_SLUG_RE = re.compile(r"^[a-z0-9-]{2,60}$")
RESERVED_AFFILIATE_SLUGS = {
    "admin",
    "api",
    "assets",
    "checkout",
    "comprar",
    "dashboard",
    "diagnostico-de-perfil-trx",
    "quiz",
    "termos",
    "privacidade",
    "contato",
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

# Valor oficial do acumulado final do ciclo (fev -> jan), alinhado Ã  curva anual.
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


def agora_utc():
    return datetime.now(timezone.utc)


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

def formatar_telefone_infinitepay(telefone):
    numeros = re.sub(r"\D", "", telefone)

    if numeros.startswith("55") and len(numeros) > 11:
        numeros = numeros[2:]

    if len(numeros) != 11:
        raise ValueError("Telefone invÃ¡lido")

    return f"+55{numeros}"


def formatar_telefone_whatsapp(telefone):
    numeros = re.sub(r"\D", "", telefone or "")

    if not numeros:
        raise ValueError("Telefone vazio")

    if numeros.startswith("55"):
        return numeros

    if len(numeros) in (10, 11):
        return f"55{numeros}"

    raise ValueError("Telefone invÃ¡lido para WhatsApp")


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
            "Configure WHATSAPP_PHONE_NUMBER_ID e WHATSAPP_ACCESS_TOKEN para envio automÃ¡tico"
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
            print(f"ðŸ“² WhatsApp automÃ¡tico enviado: {pedido['order_id']}", flush=True)
        except Exception as e:
            registrar_falha_whatsapp(
                pedido["order_id"],
                tentativas + 1,
                str(e)
            )
            print(f"âŒ Falha WhatsApp automÃ¡tico {pedido['order_id']}: {e}", flush=True)


def iniciar_worker_whatsapp():
    def worker_loop():
        while True:
            try:
                processar_fila_whatsapp()
            except Exception as e:
                print(f"âš ï¸ Worker WhatsApp com erro: {e}", flush=True)
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
        alerta = f"âš  Faltam {dias_restantes} dias para completar 30 dias"

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
        print(f"âš ï¸ WhatsApp sender nÃ£o configurado; ignorando pedido {order_id}", flush=True)
        return

    agendado = registrar_whatsapp_auto_agendamento(order_id, delay_minutes=WHATSAPP_DELAY_MINUTES)
    if not agendado:
        print(f"â„¹ï¸ WhatsApp jÃ¡ agendado/enviado para {order_id}", flush=True)
        return

    mensagem = montar_mensagem_whatsapp_pos_pago(order)
    print(f"âœ… Pagamento confirmado; agendando WhatsApp para {order_id} em {WHATSAPP_DELAY_MINUTES} min", flush=True)

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
# ROTAS PÃšBLICAS
# ======================================================

@app.route("/assets/<path:filename>")
def serve_assets(filename):
    return send_from_directory("assets", filename)


@app.route("/favicon.ico")
def favicon():
    return send_from_directory("assets", "favicon-32.ico")


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
        return jsonify({"ok": False, "error": "answers invÃ¡lido"}), 400

    planos_validos = {"gratis", "bronze", "prata", "gold", "black"}
    if recommended_plan not in planos_validos:
        return jsonify({"ok": False, "error": "recommended_plan invÃ¡lido"}), 400

    if next_level_plan and next_level_plan not in planos_validos:
        return jsonify({"ok": False, "error": "next_level_plan invÃ¡lido"}), 400

    if not isinstance(reasons, list):
        reasons = []

    reasons_limpo = [str(item)[:300] for item in reasons[:5]]

    if not submission_id:
        submission_id = str(uuid.uuid4())

    inserido = registrar_quiz_submission(
        submission_id=submission_id,
        user_key=obter_quiz_user_key(),
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
        return "Plano invÃ¡lido", 404

    afiliado = None
    if affiliate_slug:
        afiliado = buscar_afiliado_por_slug(affiliate_slug, apenas_ativos=True)
        if not afiliado:
            return "Afiliado invÃ¡lido", 404
        session["affiliate_slug"] = afiliado["slug"]

    return render_template(
        "checkout.html",
        plano=plano,
        plano_base=plano_base,
        is_free_plan=PLANOS[plano_base]["preco"] <= 0,
        affiliate=afiliado,
        nome=session.get("nome", ""),
        email=session.get("email", ""),
        telefone=session.get("telefone", "")
    )
# ðŸš« nunca permitir GET em /comprar
@app.route("/comprar", methods=["GET"])
def comprar_get():
    return redirect("/")

# âœ… POST real
@app.route("/comprar", methods=["POST"])
def comprar():
    nome_raw = request.form.get("nome")
    email_raw = request.form.get("email")
    telefone_raw = request.form.get("telefone")
    plano_checkout = (request.form.get("plano") or "").strip().lower()
    plano_id, affiliate_slug = decompor_plano_checkout(plano_checkout)

    if plano_id not in PLANOS:
        return "Dados invalidos", 400

    validacao_ok, dados = validar_cadastro_cliente(nome_raw, email_raw, telefone_raw)
    if not validacao_ok:
        return dados, 400

    nome = dados["nome"]
    email = dados["email"]
    telefone = dados["telefone"]

    session["nome"] = nome
    session["email"] = email
    session["telefone"] = telefone

    afiliado = None
    if affiliate_slug:
        afiliado = buscar_afiliado_por_slug(affiliate_slug, apenas_ativos=True)
        if not afiliado:
            return "Afiliado invalido", 400

    order_id = str(uuid.uuid4())

    salvar_order(
        order_id=order_id,
        plano=plano_id,
        nome=nome,
        email=email,
        telefone=telefone,
        checkout_slug=plano_checkout or plano_id,
        affiliate_slug=afiliado["slug"] if afiliado else None,
        affiliate_nome=afiliado["nome"] if afiliado else None,
        affiliate_email=afiliado.get("email") if afiliado else None,
        affiliate_telefone=afiliado.get("telefone") if afiliado else None,
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
            )

            marcar_order_processada(order_id)
            order_pago = buscar_order_por_id(order_id)
            registrar_compra_analytics(order_pago)
            agendar_whatsapp(order_id, minutos=WHATSAPP_DELAY_MINUTES)
            agendar_whatsapp_pos_pago(order_pago)
            return redirect(plano_info["redirect_url"])
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
            pedido["whatsapp_status"] = "telefone invÃ¡lido"

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
        return jsonify({"error": "start invÃ¡lido (YYYY-MM-DD)"}), 400

    try:
        end = parse_iso_date(request.args.get("end"))
    except Exception:
        return jsonify({"error": "end invÃ¡lido (YYYY-MM-DD)"}), 400

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
        return jsonify({"error": "metric invÃ¡lida"}), 400
    if group_by not in group_validos:
        return jsonify({"error": "groupBy invÃ¡lido"}), 400
    if plan != "all" and plan not in PLANOS:
        return jsonify({"error": "plan invÃ¡lido"}), 400

    try:
        start = parse_iso_date(request.args.get("start"))
    except Exception:
        return jsonify({"error": "start invÃ¡lido (YYYY-MM-DD)"}), 400

    try:
        end = parse_iso_date(request.args.get("end"))
    except Exception:
        return jsonify({"error": "end invÃ¡lido (YYYY-MM-DD)"}), 400

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
        return "Pedido nÃ£o encontrado", 404

    link = gerar_link_whatsapp(pedido)
    if not link:
        return "Telefone do usuÃ¡rio nÃ£o encontrado/invÃ¡lido", 400

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
        return "Pedido nÃ£o encontrado", 404

    return render_template("admin_pedido.html", pedido=pedido)

# ======================================================
# START
# ======================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)







