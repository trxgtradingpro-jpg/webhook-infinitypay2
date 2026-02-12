import psycopg2
import os

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

    # 🔥 MIGRATIONS SEGURAS
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS nome TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS telefone TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS email_tentativas INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS erro_email TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS whatsapp_enviado BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS whatsapp_tentativas INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS erro_whatsapp TEXT")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS whatsapp_agendado_para TIMESTAMP")

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
               erro_whatsapp, whatsapp_agendado_para, created_at
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
        "created_at": row[12]
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
               plano, status, created_at
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
            "created_at": r[6]
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
               erro_whatsapp, whatsapp_agendado_para, created_at
        FROM orders
        WHERE plano = 'trx-gratis'
          AND status = 'PAGO'
          AND COALESCE(whatsapp_enviado, FALSE) = FALSE
          AND (whatsapp_agendado_para IS NULL OR whatsapp_agendado_para <= NOW())
        ORDER BY created_at ASC
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
            "created_at": row[12]
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


def marcar_whatsapp_enviado(order_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE orders
        SET whatsapp_enviado = TRUE,
            erro_whatsapp = NULL
        WHERE order_id = %s
    """, (order_id,))

    conn.commit()
    cur.close()
    conn.close()
