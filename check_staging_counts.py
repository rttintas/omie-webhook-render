# check_staging_counts.py
import os, psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
)
cur = conn.cursor(cursor_factory=RealDictCursor)

print("== etl_staging_raw (páginas por serviço/método) ==")
cur.execute("""
    SELECT servico, metodo, COUNT(*) AS paginas
    FROM etl_staging_raw
    GROUP BY servico, metodo
    ORDER BY servico, metodo;
""")
for r in cur.fetchall():
    print(f"{r['servico']:30} {r['metodo']:20}  paginas={r['paginas']}")

print("\n== etl_sync_control (cursores) ==")
cur.execute("SELECT servico, ultimo_cursor, atualizado_em FROM etl_sync_control ORDER BY servico;")
for r in cur.fetchall():
    print(f"{r['servico']:12}  cursor={r['ultimo_cursor']}  atualizado_em={r['atualizado_em']}")

cur.close(); conn.close()
print("\nOK: conferido.")
