import os, psycopg2
dsn = os.environ.get("PG_DSN")
print("[INFO] PG_DSN =", dsn)
try:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT version();")
        print("[OK] Conectou! ->", cur.fetchone()[0])
except Exception as e:
    print("[ERRO] Falhou conexão:", e)
