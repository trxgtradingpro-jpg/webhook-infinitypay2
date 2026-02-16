import psycopg2
import os
import json

DATABASE_URL = os.environ.get("DATABASE_URL")

# ======================================================
# CONEXÃO
# ======================================================

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

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

    conn.commit()
    cur.close()
    conn.close()

    print("🗄️ POSTGRES OK (com migrations)", flush=True)


# ======================================================
# PEDIDOS
# ======================================================

def salvar_order(order_id, plano, nome, email, telefone):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO orders (order_id, plano, nome, email, telefone)
        VALUES (%s, %s, %s, %s, %s)
    """, (order_id, plano, nome, email, telefone))

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
               whatsapp_mensagens_enviadas, created_at
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
        "created_at": row[13]
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
               whatsapp_agendado_para, whatsapp_mensagens_enviadas, created_at
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
            "created_at": r[9]
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
          AND REGEXP_REPLACE(COALESCE(telefone, ''), '\D', '', 'g') = REGEXP_REPLACE(COALESCE(%s, ''), '\D', '', 'g')
    """, (order_id_referencia, nome, email, telefone))

    removidos = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return removidos


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
            %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, NOW()
        )
        ON CONFLICT (submission_id) DO NOTHING
    """, (
        submission_id,
        user_key,
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
