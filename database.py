import psycopg2
from psycopg2 import sql
import os
import json
from datetime import date, datetime, time
from decimal import Decimal

DATABASE_URL = os.environ.get("DATABASE_URL")
BACKUP_ADVISORY_LOCK_KEY = 771200913

# ======================================================
# CONEXÃO
# ======================================================

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def _normalizar_preferencia_comissao_interna(valor):
    pref = (valor or "").strip().lower()
    if pref == "plano":
        return "plano"
    return "dinheiro"

# ======================================================
# INIT / MIGRATIONS
# ======================================================

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            plano TEXT NOT NULL,
            nome TEXT,
            email TEXT NOT NULL,
            telefone TEXT,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            email_tentativas INTEGER DEFAULT 0,
            erro_email TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_transactions (
            transaction_nsu TEXT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_auto_dispatches (
            order_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'SCHEDULED',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            scheduled_for TIMESTAMP,
            sent_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_plan_stats (
            user_key TEXT PRIMARY KEY,
            free_count INTEGER NOT NULL DEFAULT 0,
            paid_count INTEGER NOT NULL DEFAULT 0,
            plan_trx_gratis_count INTEGER NOT NULL DEFAULT 0,
            plan_trx_bronze_count INTEGER NOT NULL DEFAULT 0,
            plan_trx_prata_count INTEGER NOT NULL DEFAULT 0,
            plan_trx_gold_count INTEGER NOT NULL DEFAULT 0,
            plan_trx_black_count INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics_purchase_events (
            id BIGSERIAL PRIMARY KEY,
            order_id TEXT UNIQUE NOT NULL,
            transaction_nsu TEXT,
            user_key TEXT NOT NULL,
            plano TEXT NOT NULL,
            is_paid BOOLEAN NOT NULL DEFAULT FALSE,
            amount_centavos INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_analytics_events_created_at ON analytics_purchase_events(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_analytics_events_user_key ON analytics_purchase_events(user_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_analytics_events_plano ON analytics_purchase_events(plano)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS quiz_submissions (
            id BIGSERIAL PRIMARY KEY,
            submission_id TEXT UNIQUE NOT NULL,
            user_key TEXT NOT NULL,
            account_email TEXT,
            ip_address TEXT,
            user_agent TEXT,
            answers JSONB NOT NULL,
            recommended_plan TEXT NOT NULL,
            next_level_plan TEXT,
            show_free_secondary BOOLEAN NOT NULL DEFAULT FALSE,
            reasons JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quiz_submissions_created_at ON quiz_submissions(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quiz_submissions_user_key ON quiz_submissions(user_key)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS client_upgrade_leads (
            id BIGSERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            order_id TEXT,
            current_plan TEXT,
            target_plan TEXT NOT NULL,
            source TEXT,
            affiliate_slug TEXT,
            checkout_slug TEXT,
            ip_address TEXT,
            user_agent TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_client_upgrade_leads_email ON client_upgrade_leads(email)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_client_upgrade_leads_created_at ON client_upgrade_leads(created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_client_upgrade_leads_target_plan ON client_upgrade_leads(target_plan)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS affiliates (
            id BIGSERIAL PRIMARY KEY,
            slug TEXT UNIQUE NOT NULL,
            nome TEXT NOT NULL,
            email TEXT,
            telefone TEXT,
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            commission_preference TEXT NOT NULL DEFAULT 'dinheiro',
            terms_accepted_at TIMESTAMP,
            link_saved_at TIMESTAMP,
            terms_accepted_ip TEXT,
            terms_version TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliates_slug ON affiliates(slug)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS affiliate_referrals (
            referred_email TEXT PRIMARY KEY,
            affiliate_slug TEXT NOT NULL,
            affiliate_nome TEXT,
            affiliate_email TEXT,
            affiliate_telefone TEXT,
            first_order_id TEXT,
            first_checkout_slug TEXT,
            first_source TEXT,
            first_referred_at TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_referrals_slug ON affiliate_referrals(affiliate_slug)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_referrals_first_referred_at ON affiliate_referrals(first_referred_at DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS affiliate_commissions (
            id BIGSERIAL PRIMARY KEY,
            order_id TEXT UNIQUE NOT NULL,
            transaction_nsu TEXT,
            referred_email TEXT NOT NULL,
            affiliate_slug TEXT NOT NULL,
            affiliate_nome TEXT,
            affiliate_email TEXT,
            affiliate_telefone TEXT,
            plano TEXT NOT NULL,
            checkout_slug TEXT,
            order_amount_centavos INTEGER NOT NULL DEFAULT 0,
            commission_percent NUMERIC(5,2) NOT NULL DEFAULT 50.00,
            commission_centavos INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_slug ON affiliate_commissions(affiliate_slug)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_email ON affiliate_commissions(referred_email)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_status ON affiliate_commissions(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_created_at ON affiliate_commissions(created_at DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS backup_runs (
            id BIGSERIAL PRIMARY KEY,
            trigger_type TEXT NOT NULL,
            status TEXT NOT NULL,
            filename TEXT,
            size_bytes BIGINT,
            sha256 TEXT,
            message TEXT,
            started_at TIMESTAMP NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_backup_runs_started_at ON backup_runs(started_at DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_accounts (
            id BIGSERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            nome TEXT,
            telefone TEXT,
            password_hash TEXT,
            first_access_required BOOLEAN NOT NULL DEFAULT TRUE,
            verification_code_hash TEXT,
            verification_expires_at TIMESTAMP,
            verification_attempts INTEGER NOT NULL DEFAULT 0,
            pending_password_hash TEXT,
            remember_token_hash TEXT,
            remember_expires_at TIMESTAMP,
            last_login_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customer_accounts_email ON customer_accounts(email)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_onboarding_progress (
            email TEXT PRIMARY KEY,
            email_accessed BOOLEAN NOT NULL DEFAULT FALSE,
            tool_downloaded BOOLEAN NOT NULL DEFAULT FALSE,
            zip_extracted BOOLEAN NOT NULL DEFAULT FALSE,
            tool_installed BOOLEAN NOT NULL DEFAULT FALSE,
            robot_activated BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customer_onboarding_progress_updated_at ON customer_onboarding_progress(updated_at DESC)")

    # 🔥 MIGRATIONS SEGURAS
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS nome TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS telefone TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS email_tentativas INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS erro_email TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS whatsapp_enviado BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS whatsapp_tentativas INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS erro_whatsapp TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS whatsapp_agendado_para TIMESTAMP")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS whatsapp_mensagens_enviadas INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS checkout_slug TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS affiliate_slug TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS affiliate_nome TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS affiliate_email TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS affiliate_telefone TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_checkout_slug ON orders(checkout_slug)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_affiliate_slug ON orders(affiliate_slug)")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS nome TEXT")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS telefone TEXT")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS password_hash TEXT")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS first_access_required BOOLEAN NOT NULL DEFAULT TRUE")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS verification_code_hash TEXT")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS verification_expires_at TIMESTAMP")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS verification_attempts INTEGER NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS pending_password_hash TEXT")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS remember_token_hash TEXT")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS remember_expires_at TIMESTAMP")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMP")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
    cur.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customer_accounts_remember_token_hash ON customer_accounts(remember_token_hash)")
    cur.execute("ALTER TABLE customer_onboarding_progress ADD COLUMN IF NOT EXISTS email_accessed BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE customer_onboarding_progress ADD COLUMN IF NOT EXISTS tool_downloaded BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE customer_onboarding_progress ADD COLUMN IF NOT EXISTS zip_extracted BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE customer_onboarding_progress ADD COLUMN IF NOT EXISTS tool_installed BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE customer_onboarding_progress ADD COLUMN IF NOT EXISTS robot_activated BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE customer_onboarding_progress ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
    cur.execute("ALTER TABLE customer_onboarding_progress ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customer_onboarding_progress_updated_at ON customer_onboarding_progress(updated_at DESC)")
    cur.execute("ALTER TABLE quiz_submissions ADD COLUMN IF NOT EXISTS account_email TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quiz_submissions_account_email ON quiz_submissions(account_email)")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS affiliate_nome TEXT")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS affiliate_email TEXT")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS affiliate_telefone TEXT")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS first_order_id TEXT")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS first_checkout_slug TEXT")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS first_source TEXT")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS first_referred_at TIMESTAMP DEFAULT NOW()")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
    cur.execute("ALTER TABLE affiliate_referrals ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_referrals_slug ON affiliate_referrals(affiliate_slug)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_referrals_first_referred_at ON affiliate_referrals(first_referred_at DESC)")
    cur.execute("ALTER TABLE affiliates ADD COLUMN IF NOT EXISTS terms_accepted_at TIMESTAMP")
    cur.execute("ALTER TABLE affiliates ADD COLUMN IF NOT EXISTS link_saved_at TIMESTAMP")
    cur.execute("ALTER TABLE affiliates ADD COLUMN IF NOT EXISTS terms_accepted_ip TEXT")
    cur.execute("ALTER TABLE affiliates ADD COLUMN IF NOT EXISTS terms_version TEXT")
    cur.execute("ALTER TABLE affiliates ADD COLUMN IF NOT EXISTS commission_preference TEXT NOT NULL DEFAULT 'dinheiro'")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS transaction_nsu TEXT")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS referred_email TEXT")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS affiliate_slug TEXT")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS affiliate_nome TEXT")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS affiliate_email TEXT")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS affiliate_telefone TEXT")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS plano TEXT")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS checkout_slug TEXT")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS order_amount_centavos INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS commission_percent NUMERIC(5,2) DEFAULT 50.00")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS commission_centavos INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDENTE'")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
    cur.execute("ALTER TABLE affiliate_commissions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_slug ON affiliate_commissions(affiliate_slug)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_email ON affiliate_commissions(referred_email)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_status ON affiliate_commissions(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_created_at ON affiliate_commissions(created_at DESC)")

    cur.execute("""
        INSERT INTO affiliate_referrals (
            referred_email,
            affiliate_slug,
            affiliate_nome,
            affiliate_email,
            affiliate_telefone,
            first_order_id,
            first_checkout_slug,
            first_source,
            first_referred_at,
            created_at,
            updated_at
        )
        SELECT DISTINCT ON (LOWER(COALESCE(TRIM(o.email), '')))
            LOWER(COALESCE(TRIM(o.email), '')) AS referred_email,
            LOWER(COALESCE(TRIM(o.affiliate_slug), '')) AS affiliate_slug,
            o.affiliate_nome,
            o.affiliate_email,
            o.affiliate_telefone,
            o.order_id,
            o.checkout_slug,
            'orders_backfill',
            COALESCE(o.created_at, NOW()),
            NOW(),
            NOW()
        FROM orders o
        WHERE COALESCE(TRIM(o.email), '') <> ''
          AND COALESCE(TRIM(o.affiliate_slug), '') <> ''
        ORDER BY LOWER(COALESCE(TRIM(o.email), '')), o.created_at ASC
        ON CONFLICT (referred_email) DO NOTHING
    """)

    cur.execute("""
        UPDATE orders
        SET whatsapp_agendado_para = created_at + INTERVAL '5 minutes'
        WHERE plano = 'trx-gratis'
          AND status = 'PAGO'
          AND whatsapp_agendado_para IS NULL
    """)

    cur.execute("""
        UPDATE orders
        SET whatsapp_mensagens_enviadas = 1
        WHERE COALESCE(whatsapp_enviado, FALSE) = TRUE
          AND COALESCE(whatsapp_mensagens_enviadas, 0) = 0
    """)

    cur.execute("""
        UPDATE orders
        SET checkout_slug = plano
        WHERE checkout_slug IS NULL
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("🗄️ POSTGRES OK (com migrations)", flush=True)


# ======================================================
# PEDIDOS
# ======================================================

def salvar_order(
    order_id,
    plano,
    nome,
    email,
    telefone,
    checkout_slug=None,
    affiliate_slug=None,
    affiliate_nome=None,
    affiliate_email=None,
    affiliate_telefone=None
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO orders (
            order_id, plano, nome, email, telefone,
            checkout_slug, affiliate_slug, affiliate_nome, affiliate_email, affiliate_telefone
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        order_id,
        plano,
        nome,
        email,
        telefone,
        checkout_slug or plano,
        affiliate_slug,
        affiliate_nome,
        affiliate_email,
        affiliate_telefone
    ))

    conn.commit()
    cur.close()
    conn.close()


def buscar_order_por_id(order_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT order_id, plano, nome, email, telefone,
               status, email_tentativas, erro_email,
               whatsapp_enviado, whatsapp_tentativas,
               erro_whatsapp, whatsapp_agendado_para,
               whatsapp_mensagens_enviadas, created_at,
               checkout_slug, affiliate_slug, affiliate_nome, affiliate_email, affiliate_telefone
        FROM orders
        WHERE order_id = %s
    """, (order_id,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "order_id": row[0],
        "plano": row[1],
        "nome": row[2],
        "email": row[3],
        "telefone": row[4],
        "status": row[5],
        "email_tentativas": row[6],
        "erro_email": row[7],
        "whatsapp_enviado": row[8],
        "whatsapp_tentativas": row[9],
        "erro_whatsapp": row[10],
        "whatsapp_agendado_para": row[11],
        "whatsapp_mensagens_enviadas": row[12],
        "created_at": row[13],
        "checkout_slug": row[14],
        "affiliate_slug": row[15],
        "affiliate_nome": row[16],
        "affiliate_email": row[17],
        "affiliate_telefone": row[18]
    }


def marcar_order_processada(order_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE orders
        SET status = 'PAGO'
        WHERE order_id = %s
    """, (order_id,))

    conn.commit()
    cur.close()
    conn.close()


def registrar_falha_email(order_id, tentativas, erro):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE orders
        SET email_tentativas = %s,
            erro_email = %s
        WHERE order_id = %s
    """, (tentativas, erro, order_id))

    conn.commit()
    cur.close()
    conn.close()

# ======================================================
# TRANSAÇÕES / WEBHOOK
# ======================================================

def transacao_ja_processada(transaction_nsu):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT 1 FROM processed_transactions
        WHERE transaction_nsu = %s
    """, (transaction_nsu,))

    existe = cur.fetchone() is not None
    cur.close()
    conn.close()

    return existe


def marcar_transacao_processada(transaction_nsu):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO processed_transactions (transaction_nsu)
        VALUES (%s)
        ON CONFLICT DO NOTHING
    """, (transaction_nsu,))

    conn.commit()
    cur.close()
    conn.close()

# ======================================================
# DASHBOARD
# ======================================================

def listar_pedidos():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT order_id, nome, email, telefone,
               plano, status, whatsapp_enviado,
               whatsapp_agendado_para, whatsapp_mensagens_enviadas, created_at,
               checkout_slug, affiliate_slug, affiliate_nome, affiliate_email, affiliate_telefone
        FROM orders
        ORDER BY created_at DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    pedidos = []
    for r in rows:
        pedidos.append({
            "order_id": r[0],
            "nome": r[1],
            "email": r[2],
            "telefone": r[3],
            "plano": r[4],
            "status": r[5],
            "whatsapp_enviado": r[6],
            "whatsapp_agendado_para": r[7],
            "whatsapp_mensagens_enviadas": r[8],
            "created_at": r[9],
            "checkout_slug": r[10],
            "affiliate_slug": r[11],
            "affiliate_nome": r[12],
            "affiliate_email": r[13],
            "affiliate_telefone": r[14]
        })

    return pedidos



def buscar_pedido_detalhado(order_id):
    return buscar_order_por_id(order_id)


def obter_estatisticas():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM orders")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM orders WHERE status = 'PAGO'")
    pagos = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM orders WHERE status = 'PENDENTE'")
    pendentes = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        "total": total,
        "pagos": pagos,
        "pendentes": pendentes
    }


def contar_pedidos_pagos_por_plano(planos):
    planos_norm = []
    for plano in (planos or []):
        plano_norm = (plano or "").strip().lower()
        if plano_norm and plano_norm not in planos_norm:
            planos_norm.append(plano_norm)

    if not planos_norm:
        return {}

    conn = get_conn()
    cur = conn.cursor()

    placeholders = ", ".join(["%s"] * len(planos_norm))
    cur.execute(f"""
        SELECT plano, COUNT(*)
        FROM orders
        WHERE status = 'PAGO'
          AND plano IN ({placeholders})
        GROUP BY plano
    """, tuple(planos_norm))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    resultado = {plano: 0 for plano in planos_norm}
    for plano, quantidade in rows:
        plano_norm = (plano or "").strip().lower()
        if plano_norm in resultado:
            resultado[plano_norm] = int(quantidade or 0)

    return resultado


def agendar_whatsapp(order_id, minutos=5):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE orders
        SET whatsapp_agendado_para = NOW() + (%s || ' minutes')::INTERVAL
        WHERE order_id = %s
    """, (str(minutos), order_id))

    conn.commit()
    cur.close()
    conn.close()



def listar_whatsapp_pendentes(limite=50):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT order_id, plano, nome, email, telefone,
               status, email_tentativas, erro_email,
               whatsapp_enviado, whatsapp_tentativas,
               erro_whatsapp, whatsapp_agendado_para,
               whatsapp_mensagens_enviadas, created_at
        FROM orders
        WHERE plano = 'trx-gratis'
          AND status = 'PAGO'
          AND COALESCE(whatsapp_enviado, FALSE) = FALSE
          AND whatsapp_agendado_para IS NOT NULL
          AND whatsapp_agendado_para <= NOW()
        ORDER BY whatsapp_agendado_para ASC
        LIMIT %s
    """, (limite,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    pedidos = []
    for row in rows:
        pedidos.append({
            "order_id": row[0],
            "plano": row[1],
            "nome": row[2],
            "email": row[3],
            "telefone": row[4],
            "status": row[5],
            "email_tentativas": row[6],
            "erro_email": row[7],
            "whatsapp_enviado": row[8],
            "whatsapp_tentativas": row[9],
            "erro_whatsapp": row[10],
            "whatsapp_agendado_para": row[11],
            "whatsapp_mensagens_enviadas": row[12],
            "created_at": row[13]
        })

    return pedidos


def registrar_falha_whatsapp(order_id, tentativas, erro):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE orders
        SET whatsapp_tentativas = %s,
            erro_whatsapp = %s
        WHERE order_id = %s
    """, (tentativas, erro, order_id))

    conn.commit()
    cur.close()
    conn.close()


def incrementar_whatsapp_enviado(order_id, quantidade=1):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE orders
        SET whatsapp_enviado = TRUE,
            erro_whatsapp = NULL,
            whatsapp_mensagens_enviadas = COALESCE(whatsapp_mensagens_enviadas, 0) + %s
        WHERE order_id = %s
    """, (quantidade, order_id))

    conn.commit()
    cur.close()
    conn.close()


def excluir_order(order_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))

    conn.commit()
    cur.close()
    conn.close()


def excluir_duplicados_por_dados(order_id_referencia, nome, email, telefone):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM orders
        WHERE order_id <> %s
          AND LOWER(COALESCE(TRIM(nome), '')) = LOWER(COALESCE(TRIM(%s), ''))
          AND LOWER(COALESCE(TRIM(email), '')) = LOWER(COALESCE(TRIM(%s), ''))
          AND REGEXP_REPLACE(COALESCE(telefone, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(%s, ''), '\\D', '', 'g')
    """, (order_id_referencia, nome, email, telefone))

    removidos = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return removidos


def excluir_duplicados_gratis_mesmo_dia(order_id_referencia, email):
    order_id_ref = (order_id_referencia or "").strip()
    email_norm = _normalizar_email_interno(email)
    if not order_id_ref or not email_norm:
        return 0

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        WITH ref AS (
            SELECT DATE(created_at) AS dia
            FROM orders
            WHERE order_id = %s
            LIMIT 1
        )
        DELETE FROM orders o
        USING ref
        WHERE o.order_id <> %s
          AND o.plano = 'trx-gratis'
          AND o.status = 'PAGO'
          AND LOWER(COALESCE(TRIM(o.email), '')) = LOWER(COALESCE(TRIM(%s), ''))
          AND DATE(o.created_at) = ref.dia
    """, (order_id_ref, order_id_ref, email_norm))

    removidos = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return removidos


def atualizar_order_afiliado(order_id, affiliate_slug=None, affiliate_nome=None, affiliate_email=None, affiliate_telefone=None):
    order_id_norm = (order_id or "").strip()[:120]
    if not order_id_norm:
        return False

    slug_norm = (affiliate_slug or "").strip().lower()[:80] or None
    nome_norm = (affiliate_nome or "").strip()[:120] or None
    email_norm = _normalizar_email_interno(affiliate_email) or None
    telefone_norm = (affiliate_telefone or "").strip()[:40] or None

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE orders
        SET affiliate_slug = COALESCE(%s, affiliate_slug),
            affiliate_nome = COALESCE(%s, affiliate_nome),
            affiliate_email = COALESCE(%s, affiliate_email),
            affiliate_telefone = COALESCE(%s, affiliate_telefone)
        WHERE order_id = %s
    """, (slug_norm, nome_norm, email_norm, telefone_norm, order_id_norm))

    atualizado = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return atualizado


# ======================================================
# AFILIADOS
# ======================================================

def listar_afiliados(include_inativos=True):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT id, slug, nome, email, telefone, ativo, commission_preference,
               terms_accepted_at, link_saved_at, terms_accepted_ip, terms_version, created_at, updated_at
        FROM affiliates
    """
    params = []

    if not include_inativos:
        sql += " WHERE ativo = TRUE"

    sql += " ORDER BY created_at DESC"

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    afiliados = []
    for row in rows:
        afiliados.append({
            "id": row[0],
            "slug": row[1],
            "nome": row[2],
            "email": row[3],
            "telefone": row[4],
            "ativo": bool(row[5]),
            "commission_preference": _normalizar_preferencia_comissao_interna(row[6]),
            "terms_accepted_at": row[7],
            "link_saved_at": row[8],
            "terms_accepted_ip": row[9],
            "terms_version": row[10],
            "created_at": row[11],
            "updated_at": row[12]
        })

    return afiliados


def buscar_afiliado_por_slug(slug, apenas_ativos=False):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT id, slug, nome, email, telefone, ativo, commission_preference,
               terms_accepted_at, link_saved_at, terms_accepted_ip, terms_version, created_at, updated_at
        FROM affiliates
        WHERE slug = %s
    """
    params = [slug]

    if apenas_ativos:
        sql += " AND ativo = TRUE"

    sql += " LIMIT 1"

    cur.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "slug": row[1],
        "nome": row[2],
        "email": row[3],
        "telefone": row[4],
        "ativo": bool(row[5]),
        "commission_preference": _normalizar_preferencia_comissao_interna(row[6]),
        "terms_accepted_at": row[7],
        "link_saved_at": row[8],
        "terms_accepted_ip": row[9],
        "terms_version": row[10],
        "created_at": row[11],
        "updated_at": row[12]
    }


def buscar_afiliado_por_email(email, apenas_ativos=False):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return None

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT id, slug, nome, email, telefone, ativo, commission_preference,
               terms_accepted_at, link_saved_at, terms_accepted_ip, terms_version, created_at, updated_at
        FROM affiliates
        WHERE LOWER(COALESCE(TRIM(email), '')) = %s
    """
    params = [email_norm]

    if apenas_ativos:
        sql += " AND ativo = TRUE"

    sql += " ORDER BY created_at DESC LIMIT 1"

    cur.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "slug": row[1],
        "nome": row[2],
        "email": row[3],
        "telefone": row[4],
        "ativo": bool(row[5]),
        "commission_preference": _normalizar_preferencia_comissao_interna(row[6]),
        "terms_accepted_at": row[7],
        "link_saved_at": row[8],
        "terms_accepted_ip": row[9],
        "terms_version": row[10],
        "created_at": row[11],
        "updated_at": row[12]
    }


def buscar_indicacao_afiliado_por_email(email):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return None

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT referred_email,
               affiliate_slug,
               affiliate_nome,
               affiliate_email,
               affiliate_telefone,
               first_order_id,
               first_checkout_slug,
               first_source,
               first_referred_at,
               created_at,
               updated_at
        FROM affiliate_referrals
        WHERE referred_email = %s
        LIMIT 1
    """, (email_norm,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "referred_email": row[0],
        "affiliate_slug": row[1],
        "affiliate_nome": row[2],
        "affiliate_email": row[3],
        "affiliate_telefone": row[4],
        "first_order_id": row[5],
        "first_checkout_slug": row[6],
        "first_source": row[7],
        "first_referred_at": row[8],
        "created_at": row[9],
        "updated_at": row[10],
    }


def registrar_primeira_indicacao_afiliado(
    referred_email,
    affiliate_slug,
    affiliate_nome=None,
    affiliate_email=None,
    affiliate_telefone=None,
    first_order_id=None,
    first_checkout_slug=None,
    first_source="checkout"
):
    email_norm = _normalizar_email_interno(referred_email)
    affiliate_slug_norm = (affiliate_slug or "").strip().lower()[:80]
    if not email_norm or not affiliate_slug_norm:
        return False

    nome_norm = (affiliate_nome or "").strip()[:120] or None
    affiliate_email_norm = _normalizar_email_interno(affiliate_email) or None
    affiliate_telefone_norm = (affiliate_telefone or "").strip()[:40] or None
    order_norm = (first_order_id or "").strip()[:120] or None
    checkout_norm = (first_checkout_slug or "").strip().lower()[:120] or None
    source_norm = (first_source or "checkout").strip()[:80]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO affiliate_referrals (
            referred_email,
            affiliate_slug,
            affiliate_nome,
            affiliate_email,
            affiliate_telefone,
            first_order_id,
            first_checkout_slug,
            first_source,
            first_referred_at,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW())
        ON CONFLICT (referred_email) DO NOTHING
    """, (
        email_norm,
        affiliate_slug_norm,
        nome_norm,
        affiliate_email_norm,
        affiliate_telefone_norm,
        order_norm,
        checkout_norm,
        source_norm,
    ))

    inserido = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return inserido


def registrar_comissao_afiliado(
    order_id,
    referred_email,
    affiliate_slug,
    plano,
    order_amount_centavos,
    commission_percent,
    commission_centavos,
    transaction_nsu=None,
    checkout_slug=None,
    affiliate_nome=None,
    affiliate_email=None,
    affiliate_telefone=None,
    status="PENDENTE"
):
    order_id_norm = (order_id or "").strip()[:120]
    referred_email_norm = _normalizar_email_interno(referred_email)
    affiliate_slug_norm = (affiliate_slug or "").strip().lower()[:80]
    plano_norm = (plano or "").strip().lower()[:60]

    if not order_id_norm or not referred_email_norm or not affiliate_slug_norm or not plano_norm:
        return False

    transaction_nsu_norm = (transaction_nsu or "").strip()[:120] or None
    checkout_slug_norm = (checkout_slug or "").strip().lower()[:120] or None
    affiliate_nome_norm = (affiliate_nome or "").strip()[:120] or None
    affiliate_email_norm = _normalizar_email_interno(affiliate_email) or None
    affiliate_telefone_norm = (affiliate_telefone or "").strip()[:40] or None
    status_norm = (status or "PENDENTE").strip().upper()[:20] or "PENDENTE"
    percent_num = Decimal(str(commission_percent or 0))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO affiliate_commissions (
            order_id,
            transaction_nsu,
            referred_email,
            affiliate_slug,
            affiliate_nome,
            affiliate_email,
            affiliate_telefone,
            plano,
            checkout_slug,
            order_amount_centavos,
            commission_percent,
            commission_centavos,
            status,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (order_id) DO UPDATE
        SET transaction_nsu = COALESCE(affiliate_commissions.transaction_nsu, EXCLUDED.transaction_nsu),
            updated_at = NOW()
    """, (
        order_id_norm,
        transaction_nsu_norm,
        referred_email_norm,
        affiliate_slug_norm,
        affiliate_nome_norm,
        affiliate_email_norm,
        affiliate_telefone_norm,
        plano_norm,
        checkout_slug_norm,
        int(order_amount_centavos or 0),
        percent_num,
        int(commission_centavos or 0),
        status_norm,
    ))

    alterado = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return alterado


def criar_afiliado(
    slug,
    nome,
    email=None,
    telefone=None,
    ativo=True,
    commission_preference="dinheiro",
    terms_accepted_at=None,
    link_saved_at=None,
    terms_accepted_ip=None,
    terms_version=None
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO affiliates (
            slug, nome, email, telefone, ativo, commission_preference,
            terms_accepted_at, link_saved_at, terms_accepted_ip, terms_version, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (slug) DO NOTHING
    """, (
        slug,
        nome,
        email,
        telefone,
        bool(ativo),
        _normalizar_preferencia_comissao_interna(commission_preference),
        terms_accepted_at,
        link_saved_at,
        terms_accepted_ip,
        terms_version,
    ))

    inserido = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return inserido


def atualizar_afiliado(
    slug_atual,
    slug_novo,
    nome,
    email=None,
    telefone=None,
    ativo=True,
    commission_preference=None,
    terms_accepted_at=None,
    link_saved_at=None,
    terms_accepted_ip=None,
    terms_version=None
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE affiliates
        SET slug = %s,
            nome = %s,
            email = %s,
            telefone = %s,
            ativo = %s,
            commission_preference = COALESCE(%s, commission_preference),
            terms_accepted_at = COALESCE(%s, terms_accepted_at),
            link_saved_at = COALESCE(%s, link_saved_at),
            terms_accepted_ip = COALESCE(%s, terms_accepted_ip),
            terms_version = COALESCE(%s, terms_version),
            updated_at = NOW()
        WHERE slug = %s
    """, (
        slug_novo,
        nome,
        email,
        telefone,
        bool(ativo),
        (_normalizar_preferencia_comissao_interna(commission_preference) if commission_preference is not None else None),
        terms_accepted_at,
        link_saved_at,
        terms_accepted_ip,
        terms_version,
        slug_atual
    ))

    atualizado = cur.rowcount > 0
    slug_antigo = (slug_atual or "").strip().lower()
    slug_novo_norm = (slug_novo or "").strip().lower()
    slug_alterado = bool(atualizado and slug_antigo and slug_novo_norm and slug_antigo != slug_novo_norm)

    if slug_alterado:
        cur.execute("""
            UPDATE affiliate_referrals
            SET affiliate_slug = %s,
                affiliate_nome = COALESCE(%s, affiliate_nome),
                affiliate_email = COALESCE(%s, affiliate_email),
                affiliate_telefone = COALESCE(%s, affiliate_telefone),
                updated_at = NOW()
            WHERE affiliate_slug = %s
        """, (slug_novo_norm, nome, email, telefone, slug_antigo))

        cur.execute("""
            UPDATE affiliate_commissions
            SET affiliate_slug = %s,
                affiliate_nome = COALESCE(%s, affiliate_nome),
                affiliate_email = COALESCE(%s, affiliate_email),
                affiliate_telefone = COALESCE(%s, affiliate_telefone),
                updated_at = NOW()
            WHERE affiliate_slug = %s
        """, (slug_novo_norm, nome, email, telefone, slug_antigo))

        cur.execute("""
            UPDATE orders
            SET affiliate_slug = %s,
                affiliate_nome = COALESCE(%s, affiliate_nome),
                affiliate_email = COALESCE(%s, affiliate_email),
                affiliate_telefone = COALESCE(%s, affiliate_telefone)
            WHERE LOWER(COALESCE(TRIM(affiliate_slug), '')) = %s
        """, (slug_novo_norm, nome, email, telefone, slug_antigo))

        cur.execute("""
            UPDATE client_upgrade_leads
            SET affiliate_slug = %s
            WHERE LOWER(COALESCE(TRIM(affiliate_slug), '')) = %s
        """, (slug_novo_norm, slug_antigo))

    conn.commit()
    cur.close()
    conn.close()
    return atualizado


def excluir_afiliado(slug):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM affiliates WHERE slug = %s", (slug,))
    removido = cur.rowcount > 0

    conn.commit()
    cur.close()
    conn.close()
    return removido


# ======================================================
# ANALYTICS
# ======================================================

def registrar_evento_compra_analytics(order_id, user_key, plano, is_paid, amount_centavos, transaction_nsu=None, created_at=None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO analytics_purchase_events (
            order_id, transaction_nsu, user_key, plano,
            is_paid, amount_centavos, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
        ON CONFLICT (order_id) DO NOTHING
    """, (order_id, transaction_nsu, user_key, plano, bool(is_paid), int(amount_centavos or 0), created_at))

    inserido = cur.rowcount > 0
    if inserido:
        cur.execute("""
            INSERT INTO user_plan_stats (
                user_key,
                free_count,
                paid_count,
                plan_trx_gratis_count,
                plan_trx_bronze_count,
                plan_trx_prata_count,
                plan_trx_gold_count,
                plan_trx_black_count,
                updated_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                NOW()
            )
            ON CONFLICT (user_key)
            DO UPDATE SET
                free_count = user_plan_stats.free_count + EXCLUDED.free_count,
                paid_count = user_plan_stats.paid_count + EXCLUDED.paid_count,
                plan_trx_gratis_count = user_plan_stats.plan_trx_gratis_count + EXCLUDED.plan_trx_gratis_count,
                plan_trx_bronze_count = user_plan_stats.plan_trx_bronze_count + EXCLUDED.plan_trx_bronze_count,
                plan_trx_prata_count = user_plan_stats.plan_trx_prata_count + EXCLUDED.plan_trx_prata_count,
                plan_trx_gold_count = user_plan_stats.plan_trx_gold_count + EXCLUDED.plan_trx_gold_count,
                plan_trx_black_count = user_plan_stats.plan_trx_black_count + EXCLUDED.plan_trx_black_count,
                updated_at = NOW()
        """, (
            user_key,
            1 if plano == 'trx-gratis' else 0,
            1 if is_paid and plano != 'trx-gratis' else 0,
            1 if plano == 'trx-gratis' else 0,
            1 if plano == 'trx-bronze' else 0,
            1 if plano == 'trx-prata' else 0,
            1 if plano == 'trx-gold' else 0,
            1 if plano == 'trx-black' else 0
        ))

    conn.commit()
    cur.close()
    conn.close()

    return inserido




def backfill_analytics_from_orders(precos_por_plano):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT order_id, email, telefone, plano, status, created_at
        FROM orders
        WHERE status = 'PAGO'
        ORDER BY created_at ASC
    """)

    rows = cur.fetchall()

    for row in rows:
        order_id, email, telefone, plano, status, created_at = row
        if plano not in precos_por_plano:
            continue

        email_norm = (email or '').strip().lower()
        telefone_norm = ''.join(ch for ch in (telefone or '') if ch.isdigit())
        user_key = email_norm or telefone_norm or (order_id or '')
        if not user_key:
            continue

        amount_centavos = int(precos_por_plano.get(plano) or 0)

        cur.execute("""
            INSERT INTO analytics_purchase_events (
                order_id, transaction_nsu, user_key, plano,
                is_paid, amount_centavos, created_at
            )
            VALUES (%s, NULL, %s, %s, %s, %s, COALESCE(%s, NOW()))
            ON CONFLICT (order_id) DO NOTHING
        """, (order_id, user_key, plano, amount_centavos > 0, amount_centavos, created_at))

        if cur.rowcount > 0:
            cur.execute("""
                INSERT INTO user_plan_stats (
                    user_key,
                    free_count,
                    paid_count,
                    plan_trx_gratis_count,
                    plan_trx_bronze_count,
                    plan_trx_prata_count,
                    plan_trx_gold_count,
                    plan_trx_black_count,
                    updated_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    NOW()
                )
                ON CONFLICT (user_key)
                DO UPDATE SET
                    free_count = user_plan_stats.free_count + EXCLUDED.free_count,
                    paid_count = user_plan_stats.paid_count + EXCLUDED.paid_count,
                    plan_trx_gratis_count = user_plan_stats.plan_trx_gratis_count + EXCLUDED.plan_trx_gratis_count,
                    plan_trx_bronze_count = user_plan_stats.plan_trx_bronze_count + EXCLUDED.plan_trx_bronze_count,
                    plan_trx_prata_count = user_plan_stats.plan_trx_prata_count + EXCLUDED.plan_trx_prata_count,
                    plan_trx_gold_count = user_plan_stats.plan_trx_gold_count + EXCLUDED.plan_trx_gold_count,
                    plan_trx_black_count = user_plan_stats.plan_trx_black_count + EXCLUDED.plan_trx_black_count,
                    updated_at = NOW()
            """, (
                user_key,
                1 if plano == 'trx-gratis' else 0,
                1 if amount_centavos > 0 and plano != 'trx-gratis' else 0,
                1 if plano == 'trx-gratis' else 0,
                1 if plano == 'trx-bronze' else 0,
                1 if plano == 'trx-prata' else 0,
                1 if plano == 'trx-gold' else 0,
                1 if plano == 'trx-black' else 0
            ))

    conn.commit()
    cur.close()
    conn.close()

def buscar_user_plan_stats(user_key):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT user_key, free_count, paid_count,
               plan_trx_gratis_count, plan_trx_bronze_count,
               plan_trx_prata_count, plan_trx_gold_count,
               plan_trx_black_count, updated_at
        FROM user_plan_stats
        WHERE user_key = %s
    """, (user_key,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        'user_key': row[0],
        'free_count': row[1],
        'paid_count': row[2],
        'plan_trx_gratis_count': row[3],
        'plan_trx_bronze_count': row[4],
        'plan_trx_prata_count': row[5],
        'plan_trx_gold_count': row[6],
        'plan_trx_black_count': row[7],
        'updated_at': row[8]
    }


def listar_eventos_analytics(start_date=None, end_date=None, plano=None):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT order_id, transaction_nsu, user_key, plano,
               is_paid, amount_centavos, created_at
        FROM analytics_purchase_events
        WHERE 1=1
    """
    params = []

    if start_date is not None:
        sql += " AND created_at >= %s"
        params.append(start_date)

    if end_date is not None:
        sql += " AND created_at < %s"
        params.append(end_date)

    if plano and plano != 'all':
        sql += " AND plano = %s"
        params.append(plano)

    sql += " ORDER BY created_at ASC"

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    eventos = []
    for r in rows:
        eventos.append({
            'order_id': r[0],
            'transaction_nsu': r[1],
            'user_key': r[2],
            'plano': r[3],
            'is_paid': r[4],
            'amount_centavos': r[5],
            'created_at': r[6]
        })

    return eventos



def registrar_lead_upgrade_cliente(
    email,
    target_plan,
    current_plan=None,
    source=None,
    order_id=None,
    affiliate_slug=None,
    checkout_slug=None,
    ip_address=None,
    user_agent=None
):
    email_norm = _normalizar_email_interno(email)
    target_plan_norm = (target_plan or "").strip().lower()[:60]
    current_plan_norm = (current_plan or "").strip().lower()[:60]
    source_norm = (source or "client_area").strip()[:120]
    order_id_norm = (order_id or "").strip()[:120] or None
    affiliate_slug_norm = (affiliate_slug or "").strip().lower()[:80] or None
    checkout_slug_norm = (checkout_slug or "").strip().lower()[:120] or None
    ip_norm = (ip_address or "").strip()[:80] or None
    user_agent_norm = (user_agent or "").strip()[:300] or None

    if not email_norm or not target_plan_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO client_upgrade_leads (
            email,
            order_id,
            current_plan,
            target_plan,
            source,
            affiliate_slug,
            checkout_slug,
            ip_address,
            user_agent
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        email_norm,
        order_id_norm,
        current_plan_norm or None,
        target_plan_norm,
        source_norm,
        affiliate_slug_norm,
        checkout_slug_norm,
        ip_norm,
        user_agent_norm
    ))

    inserido = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return inserido


def registrar_whatsapp_auto_agendamento(order_id, delay_minutes=5):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO whatsapp_auto_dispatches (order_id, status, scheduled_for)
        VALUES (%s, 'SCHEDULED', NOW() + (%s || ' minutes')::INTERVAL)
        ON CONFLICT (order_id) DO NOTHING
    """, (order_id, str(delay_minutes)))

    inserido = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return inserido


def marcar_whatsapp_auto_enviado(order_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE whatsapp_auto_dispatches
        SET status = 'SENT',
            sent_at = NOW(),
            attempts = attempts + 1,
            last_error = NULL
        WHERE order_id = %s
    """, (order_id,))

    conn.commit()
    cur.close()
    conn.close()


def registrar_falha_whatsapp_auto(order_id, erro):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE whatsapp_auto_dispatches
        SET status = 'FAILED',
            attempts = attempts + 1,
            last_error = %s
        WHERE order_id = %s
    """, (erro, order_id))

    conn.commit()
    cur.close()
    conn.close()


def registrar_quiz_submission(
    submission_id,
    user_key,
    account_email,
    ip_address,
    user_agent,
    answers,
    recommended_plan,
    next_level_plan=None,
    show_free_secondary=False,
    reasons=None
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO quiz_submissions (
            submission_id,
            user_key,
            account_email,
            ip_address,
            user_agent,
            answers,
            recommended_plan,
            next_level_plan,
            show_free_secondary,
            reasons,
            created_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, NOW()
        )
        ON CONFLICT (submission_id) DO NOTHING
    """, (
        submission_id,
        user_key,
        _normalizar_email_interno(account_email) or None,
        ip_address,
        user_agent,
        json.dumps(answers or {}, ensure_ascii=False),
        recommended_plan,
        next_level_plan,
        bool(show_free_secondary),
        json.dumps(reasons or [], ensure_ascii=False)
    ))

    inserido = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return inserido


def existe_quiz_submission(account_email=None, user_key=None):
    email_norm = _normalizar_email_interno(account_email)
    user_key_norm = (user_key or "").strip()
    if not email_norm and not user_key_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()

    if email_norm and user_key_norm:
        cur.execute("""
            SELECT 1
            FROM quiz_submissions
            WHERE account_email = %s OR user_key = %s
            LIMIT 1
        """, (email_norm, user_key_norm))
    elif email_norm:
        cur.execute("""
            SELECT 1
            FROM quiz_submissions
            WHERE account_email = %s
            LIMIT 1
        """, (email_norm,))
    else:
        cur.execute("""
            SELECT 1
            FROM quiz_submissions
            WHERE user_key = %s
            LIMIT 1
        """, (user_key_norm,))

    existe = cur.fetchone() is not None
    cur.close()
    conn.close()
    return existe


def _normalizar_email_interno(email):
    return (email or "").strip().lower()[:190]


def buscar_conta_cliente_por_email(email):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return None

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT email, nome, telefone, password_hash, first_access_required,
               verification_code_hash, verification_expires_at, verification_attempts,
               pending_password_hash, remember_token_hash, remember_expires_at,
               last_login_at, created_at, updated_at
        FROM customer_accounts
        WHERE email = %s
        LIMIT 1
    """, (email_norm,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "email": row[0],
        "nome": row[1],
        "telefone": row[2],
        "password_hash": row[3],
        "first_access_required": bool(row[4]),
        "verification_code_hash": row[5],
        "verification_expires_at": row[6],
        "verification_attempts": int(row[7] or 0),
        "pending_password_hash": row[8],
        "remember_token_hash": row[9],
        "remember_expires_at": row[10],
        "last_login_at": row[11],
        "created_at": row[12],
        "updated_at": row[13],
    }


def criar_ou_atualizar_conta_cliente(email, nome=None, telefone=None, password_hash=None, first_access_required=True):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return {"created": False, "account": None}

    conta = buscar_conta_cliente_por_email(email_norm)
    if conta:
        conn = get_conn()
        cur = conn.cursor()
        if not (conta.get("password_hash") or "").strip() and (password_hash or "").strip():
            cur.execute("""
                UPDATE customer_accounts
                SET nome = COALESCE(NULLIF(%s, ''), nome),
                    telefone = COALESCE(NULLIF(%s, ''), telefone),
                    password_hash = %s,
                    first_access_required = %s,
                    updated_at = NOW()
                WHERE email = %s
            """, (
                (nome or "").strip(),
                (telefone or "").strip(),
                (password_hash or "").strip(),
                bool(first_access_required),
                email_norm
            ))
        else:
            cur.execute("""
                UPDATE customer_accounts
                SET nome = COALESCE(NULLIF(%s, ''), nome),
                    telefone = COALESCE(NULLIF(%s, ''), telefone),
                    updated_at = NOW()
                WHERE email = %s
            """, ((nome or "").strip(), (telefone or "").strip(), email_norm))
        conn.commit()
        cur.close()
        conn.close()
        return {"created": False, "account": buscar_conta_cliente_por_email(email_norm)}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO customer_accounts (
            email, nome, telefone, password_hash, first_access_required,
            verification_attempts, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, 0, NOW(), NOW())
    """, (
        email_norm,
        (nome or "").strip() or None,
        (telefone or "").strip() or None,
        (password_hash or "").strip() or None,
        bool(first_access_required),
    ))
    conn.commit()
    cur.close()
    conn.close()
    return {"created": True, "account": buscar_conta_cliente_por_email(email_norm)}


def registrar_codigo_primeiro_acesso(email, pending_password_hash, code_hash, expires_at):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customer_accounts
        SET pending_password_hash = %s,
            verification_code_hash = %s,
            verification_expires_at = %s,
            verification_attempts = 0,
            updated_at = NOW()
        WHERE email = %s
    """, (
        (pending_password_hash or "").strip() or None,
        (code_hash or "").strip() or None,
        expires_at,
        email_norm
    ))
    ok = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return ok


def incrementar_tentativa_codigo_cliente(email):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customer_accounts
        SET verification_attempts = COALESCE(verification_attempts, 0) + 1,
            updated_at = NOW()
        WHERE email = %s
    """, (email_norm,))
    ok = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return ok


def limpar_codigo_cliente(email):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customer_accounts
        SET pending_password_hash = NULL,
            verification_code_hash = NULL,
            verification_expires_at = NULL,
            verification_attempts = 0,
            updated_at = NOW()
        WHERE email = %s
    """, (email_norm,))
    ok = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return ok


def confirmar_senha_conta_cliente(email, password_hash):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customer_accounts
        SET password_hash = %s,
            first_access_required = FALSE,
            pending_password_hash = NULL,
            verification_code_hash = NULL,
            verification_expires_at = NULL,
            verification_attempts = 0,
            last_login_at = NOW(),
            updated_at = NOW()
        WHERE email = %s
    """, ((password_hash or "").strip() or None, email_norm))
    ok = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return ok


def forcar_reset_senha_conta_cliente(email, password_hash):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customer_accounts
        SET password_hash = %s,
            first_access_required = TRUE,
            pending_password_hash = NULL,
            verification_code_hash = NULL,
            verification_expires_at = NULL,
            verification_attempts = 0,
            remember_token_hash = NULL,
            remember_expires_at = NULL,
            updated_at = NOW()
        WHERE email = %s
    """, ((password_hash or "").strip() or None, email_norm))
    ok = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return ok


def atualizar_ultimo_login_conta_cliente(email):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customer_accounts
        SET last_login_at = NOW(),
            updated_at = NOW()
        WHERE email = %s
    """, (email_norm,))
    ok = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return ok


def salvar_remember_token_cliente(email, token_hash, expires_at):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customer_accounts
        SET remember_token_hash = %s,
            remember_expires_at = %s,
            updated_at = NOW()
        WHERE email = %s
    """, ((token_hash or "").strip() or None, expires_at, email_norm))
    ok = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return ok


def limpar_remember_token_cliente(email):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customer_accounts
        SET remember_token_hash = NULL,
            remember_expires_at = NULL,
            updated_at = NOW()
        WHERE email = %s
    """, (email_norm,))
    ok = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return ok


def buscar_conta_cliente_por_remember_hash(token_hash):
    token_hash = (token_hash or "").strip()
    if not token_hash:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT email, nome, telefone, password_hash, first_access_required,
               verification_code_hash, verification_expires_at, verification_attempts,
               pending_password_hash, remember_token_hash, remember_expires_at,
               last_login_at, created_at, updated_at
        FROM customer_accounts
        WHERE remember_token_hash = %s
        LIMIT 1
    """, (token_hash,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "email": row[0],
        "nome": row[1],
        "telefone": row[2],
        "password_hash": row[3],
        "first_access_required": bool(row[4]),
        "verification_code_hash": row[5],
        "verification_expires_at": row[6],
        "verification_attempts": int(row[7] or 0),
        "pending_password_hash": row[8],
        "remember_token_hash": row[9],
        "remember_expires_at": row[10],
        "last_login_at": row[11],
        "created_at": row[12],
        "updated_at": row[13],
    }


def buscar_onboarding_progresso_cliente(email):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT email_accessed, tool_downloaded, zip_extracted, tool_installed, robot_activated,
               created_at, updated_at
        FROM customer_onboarding_progress
        WHERE email = %s
        LIMIT 1
    """, (email_norm,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return {
            "email": email_norm,
            "email_accessed": False,
            "tool_downloaded": False,
            "zip_extracted": False,
            "tool_installed": False,
            "robot_activated": False,
            "created_at": None,
            "updated_at": None,
        }

    return {
        "email": email_norm,
        "email_accessed": bool(row[0]),
        "tool_downloaded": bool(row[1]),
        "zip_extracted": bool(row[2]),
        "tool_installed": bool(row[3]),
        "robot_activated": bool(row[4]),
        "created_at": row[5],
        "updated_at": row[6],
    }


def salvar_onboarding_progresso_cliente(email, progresso):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return None

    data = progresso or {}
    email_accessed = bool(data.get("email_accessed"))
    tool_downloaded = bool(data.get("tool_downloaded"))
    zip_extracted = bool(data.get("zip_extracted"))
    tool_installed = bool(data.get("tool_installed"))
    robot_activated = bool(data.get("robot_activated"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO customer_onboarding_progress (
            email,
            email_accessed,
            tool_downloaded,
            zip_extracted,
            tool_installed,
            robot_activated,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (email) DO UPDATE
        SET email_accessed = EXCLUDED.email_accessed,
            tool_downloaded = EXCLUDED.tool_downloaded,
            zip_extracted = EXCLUDED.zip_extracted,
            tool_installed = EXCLUDED.tool_installed,
            robot_activated = EXCLUDED.robot_activated,
            updated_at = NOW()
    """, (
        email_norm,
        email_accessed,
        tool_downloaded,
        zip_extracted,
        tool_installed,
        robot_activated,
    ))
    conn.commit()
    cur.close()
    conn.close()
    return buscar_onboarding_progresso_cliente(email_norm)


def listar_pedidos_pagos_por_email(email, limite=20):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return []

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT order_id, plano, nome, email, telefone, status, created_at,
               checkout_slug, affiliate_slug
        FROM orders
        WHERE LOWER(BTRIM(COALESCE(email, ''))) = %s
          AND UPPER(BTRIM(COALESCE(status, ''))) = 'PAGO'
        ORDER BY created_at DESC
        LIMIT %s
    """, (email_norm, int(max(1, limite))))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    pedidos = []
    for row in rows:
        pedidos.append({
            "order_id": row[0],
            "plano": row[1],
            "nome": row[2],
            "email": row[3],
            "telefone": row[4],
            "status": row[5],
            "created_at": row[6],
            "checkout_slug": row[7],
            "affiliate_slug": row[8],
        })

    return pedidos


def listar_pedidos_acesso_por_email(email, limite=20):
    email_norm = _normalizar_email_interno(email)
    if not email_norm:
        return []

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT order_id, plano, nome, email, telefone, status, created_at,
               checkout_slug, affiliate_slug
        FROM orders
        WHERE LOWER(BTRIM(COALESCE(email, ''))) = %s
          AND UPPER(BTRIM(COALESCE(status, ''))) IN ('PAGO', 'BONUS')
        ORDER BY created_at DESC
        LIMIT %s
    """, (email_norm, int(max(1, limite))))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    pedidos = []
    for row in rows:
        pedidos.append({
            "order_id": row[0],
            "plano": row[1],
            "nome": row[2],
            "email": row[3],
            "telefone": row[4],
            "status": row[5],
            "created_at": row[6],
            "checkout_slug": row[7],
            "affiliate_slug": row[8],
        })

    return pedidos


def conceder_bonus_indicacao_mes_gratis(
    source_order_id,
    plano,
    email,
    nome=None,
    telefone=None,
    checkout_slug=None
):
    source_order_id_norm = (source_order_id or "").strip()[:120]
    plano_norm = (plano or "").strip().lower()[:60]
    email_norm = _normalizar_email_interno(email)
    nome_norm = (nome or "").strip()[:120] or None
    telefone_norm = (telefone or "").strip()[:40] or None

    if not source_order_id_norm or not plano_norm or not email_norm:
        return False, None

    bonus_order_id = f"bonus-indicacao-{source_order_id_norm}"
    if len(bonus_order_id) > 120:
        bonus_order_id = bonus_order_id[:120]

    checkout_slug_norm = (checkout_slug or f"bonus-indicacao-{plano_norm}").strip().lower()[:120]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO orders (
            order_id,
            plano,
            nome,
            email,
            telefone,
            status,
            checkout_slug,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, 'BONUS', %s, NOW())
        ON CONFLICT (order_id) DO NOTHING
    """, (
        bonus_order_id,
        plano_norm,
        nome_norm,
        email_norm,
        telefone_norm,
        checkout_slug_norm
    ))

    inserido = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return inserido, bonus_order_id


def buscar_ultimo_pedido_pago_por_email(email):
    pedidos = listar_pedidos_pagos_por_email(email, limite=1)
    if not pedidos:
        return None
    return pedidos[0]


def _valor_json_compat(v):
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def exportar_snapshot_publico():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name ASC
    """)
    tabelas = [r[0] for r in cur.fetchall()]

    snapshot = {
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "tables": {}
    }

    for tabela in tabelas:
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(tabela))
        cur.execute(query)
        colunas = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        dados_tabela = []
        for row in rows:
            item = {}
            for idx, col in enumerate(colunas):
                item[col] = _valor_json_compat(row[idx])
            dados_tabela.append(item)

        snapshot["tables"][tabela] = dados_tabela

    cur.close()
    conn.close()
    return snapshot


def registrar_backup_execucao(
    trigger_type,
    status,
    filename=None,
    size_bytes=None,
    sha256=None,
    message=None,
    started_at=None,
    finished_at=None
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO backup_runs (
            trigger_type,
            status,
            filename,
            size_bytes,
            sha256,
            message,
            started_at,
            finished_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            COALESCE(%s, NOW()),
            COALESCE(%s, NOW())
        )
    """, (
        (trigger_type or "manual")[:30],
        (status or "UNKNOWN")[:30],
        (filename or "")[:255] or None,
        int(size_bytes) if size_bytes is not None else None,
        (sha256 or "")[:128] or None,
        (message or "")[:1000] or None,
        started_at,
        finished_at
    ))

    conn.commit()
    cur.close()
    conn.close()


def listar_backups_execucao(limit=30):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, trigger_type, status, filename, size_bytes, sha256, message, started_at, finished_at
        FROM backup_runs
        ORDER BY started_at DESC
        LIMIT %s
    """, (int(limit),))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    itens = []
    for r in rows:
        itens.append({
            "id": r[0],
            "trigger_type": r[1],
            "status": r[2],
            "filename": r[3],
            "size_bytes": r[4],
            "sha256": r[5],
            "message": r[6],
            "started_at": r[7],
            "finished_at": r[8],
        })

    return itens


def adquirir_lock_backup_distribuido():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (BACKUP_ADVISORY_LOCK_KEY,))
        ok = bool(cur.fetchone()[0])
    finally:
        cur.close()

    if ok:
        return conn

    conn.close()
    return None


def liberar_lock_backup_distribuido(conn):
    if not conn:
        return

    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT pg_advisory_unlock(%s)", (BACKUP_ADVISORY_LOCK_KEY,))
        finally:
            cur.close()
    except Exception:
        pass
    finally:
        conn.close()
