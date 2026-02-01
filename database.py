import sqlite3
from datetime import datetime

DB_PATH = "database.db"


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plano TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS processed (
            transaction_nsu TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()
    print("🗄️ BANCO INICIALIZADO COM SUCESSO", flush=True)


# ======================================================
# ORDERS
# ======================================================

def salvar_order_email(plano, email):
    conn = get_conn()
    c = conn.cursor()

    c.execute(
        "INSERT INTO orders (plano, email, created_at) VALUES (?, ?, ?)",
        (plano, email, datetime.utcnow().isoformat())
    )

    conn.commit()
    conn.close()

    print(f"💾 BANCO | SALVO plano={plano} email={email}", flush=True)


def buscar_email_pendente(plano):
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        SELECT email FROM orders
        WHERE plano = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, (plano,))

    row = c.fetchone()
    conn.close()

    return row[0] if row else None


# ======================================================
# TRANSAÇÕES
# ======================================================

def transacao_ja_processada(transaction_nsu):
    conn = get_conn()
    c = conn.cursor()

    c.execute(
        "SELECT 1 FROM processed WHERE transaction_nsu = ?",
        (transaction_nsu,)
    )

    exists = c.fetchone() is not None
    conn.close()

    return exists


def marcar_processada(transaction_nsu):
    conn = get_conn()
    c = conn.cursor()

    c.execute(
        "INSERT OR IGNORE INTO processed (transaction_nsu, created_at) VALUES (?, ?)",
        (transaction_nsu, datetime.utcnow().isoformat())
    )

    conn.commit()
    conn.close()

    print(f"✅ TRANSAÇÃO PROCESSADA: {transaction_nsu}", flush=True)
