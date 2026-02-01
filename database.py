import psycopg2
import os

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            plano TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed (
            transaction_nsu TEXT PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("🗄️ POSTGRES CONECTADO E TABELAS OK", flush=True)


def salvar_order_email(plano, email):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO orders (plano, email) VALUES (%s, %s)",
        (plano, email)
    )

    conn.commit()
    cur.close()
    conn.close()


def buscar_email_pendente(plano):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT email FROM orders
        WHERE plano = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (plano,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    return row[0] if row else None


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


def marcar_processada(transaction_nsu):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO processed (transaction_nsu) VALUES (%s) ON CONFLICT DO NOTHING",
        (transaction_nsu,)
    )

    conn.commit()
    cur.close()
    conn.close()
