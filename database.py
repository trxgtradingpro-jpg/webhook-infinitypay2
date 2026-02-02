import psycopg2
import os

print("📦 DATABASE.PY CARREGADO")

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # TABELA BASE
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            plano TEXT NOT NULL,
            email TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)

    # MIGRATIONS SEGURAS (corrige erro atual)
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS email_tentativas INT DEFAULT 0;")
    cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS ultimo_erro TEXT;")

    # TABELA DE IDEMPOTÊNCIA
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed (
            transaction_nsu TEXT PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("🗄️ POSTGRES OK (com migrations)", flush=True)


def salvar_order(order_id, plano, email):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO orders (order_id, plano, email)
        VALUES (%s, %s, %s)
    """, (order_id, plano, email))

    conn.commit()
    cur.close()
    conn.close()


def buscar_order_por_id(order_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT order_id, plano, email, status, email_tentativas
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
        "email": row[2],
        "status": row[3],
        "email_tentativas": row[4]
    }


def marcar_order_processada(order_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE orders
        SET status = 'PROCESSADO'
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
            ultimo_erro = %s
        WHERE order_id = %s
    """, (tentativas, erro, order_id))

    conn.commit()
    cur.close()
    conn.close()


def transacao_ja_processada(transaction_nsu):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT 1 FROM processed WHERE transaction_nsu = %s",
        (transaction_nsu,)
    )

    exists = cur.fetchone() is not None

    cur.close()
    conn.close()

    return exists


def marcar_transacao_processada(transaction_nsu):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO processed (transaction_nsu)
        VALUES (%s)
        ON CONFLICT DO NOTHING
    """, (transaction_nsu,))

    conn.commit()
    cur.close()
    conn.close()
def listar_pedidos():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT order_id, email, plano, status, email_tentativas, created_at
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
            "email": r[1],
            "plano": r[2],
            "status": r[3],
            "email_tentativas": r[4],
            "created_at": r[5]
        })

    return pedidos


def buscar_pedido_detalhado(order_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
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
        "email": row[2],
        "status": row[3],
        "email_tentativas": row[4],
        "ultimo_erro": row[5],
        "created_at": row[6]
    }


def obter_estatisticas():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM orders")
    total_pedidos = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM orders WHERE status = 'PROCESSADO'")
    processados = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM orders WHERE status = 'PENDENTE'")
    pendentes = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    return {
        "total": total_pedidos,
        "processados": processados,
        "pendentes": pendentes
    }


