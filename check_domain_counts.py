# check_domain_counts.py
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

print("== Contagem de linhas nas TABELAS de domínio ==")
cur.execute("""
    SELECT 'omie_cliente'      AS tb, COUNT(*)::bigint AS qtd FROM omie_cliente
    UNION ALL SELECT 'omie_produto',      COUNT(*)     FROM omie_produto
    UNION ALL SELECT 'omie_pedido',       COUNT(*)     FROM omie_pedido
    UNION ALL SELECT 'omie_pedido_item',  COUNT(*)     FROM omie_pedido_item
    UNION ALL SELECT 'omie_nf',           COUNT(*)     FROM omie_nf
    UNION ALL SELECT 'omie_nf_item',      COUNT(*)     FROM omie_nf_item
    ORDER BY tb;
""")
for r in cur.fetchall():
    print(f"{r['tb']:<18} -> {r['qtd']}")

print("\n== Contagem nas VIEWS (se já criadas) ==")
views = [
    ("vw_pedido_itens_principais", "SELECT COUNT(*) AS qtd FROM vw_pedido_itens_principais"),
    ("vw_lista_separacao",         "SELECT COUNT(*) AS qtd FROM vw_lista_separacao"),
    ("vw_etiqueta_78x48",          "SELECT COUNT(*) AS qtd FROM vw_etiqueta_78x48"),
    ("vw_nf_pedido",               "SELECT COUNT(*) AS qtd FROM vw_nf_pedido"),
]
for name, sql in views:
    try:
        cur.execute(sql)
        qtd = cur.fetchone()["qtd"]
        print(f"{name:<28} -> {qtd}")
    except Exception as e:
        print(f"{name:<28} -> (não existe ainda)")

cur.close()
conn.close()
print("\nOK: conferido.")
