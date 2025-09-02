# -*- coding: utf-8 -*-
"""
teste_conexao.py
- Lê o DSN do Postgres de PG_DSN (variável de ambiente)
- Testa conexão
- Lista tabelas / views / materialized views (schema public)
- Conta registros de cada uma (ou mostra estimativa quando não der)
"""

import os
import sys
from datetime import datetime
import psycopg2
import psycopg2.extras

DSN = os.getenv("PG_DSN")

if not DSN:
    print("ERRO: variável de ambiente PG_DSN não encontrada.")
    print('Exemplo de valor:')
    print('  dbname=neondb user=neondb_owner password=*** host=... port=5432')
    sys.exit(1)

def println(title):
    print("\n" + "="*len(title))
    print(title)
    print("="*len(title))

def fetch_all(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()

def count_rows(cur, fqname):
    """Tenta count(*). Se der erro (ex.: permissão), cai para estimativa via pg_class."""
    try:
        cur.execute(f'SELECT count(*) FROM {fqname};')
        return int(cur.fetchone()[0]), False  # False = não é estimativa
    except Exception:
        # estimativa a partir de pg_class
        cur.execute("""
            SELECT c.reltuples::bigint
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """, fqname.split("."))
        r = cur.fetchone()
        est = int(r[0]) if r and r[0] is not None else None
        return est, True  # True = estimativa

def main():
    println("Conectando…")
    print("DSN lido:", DSN)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            # 1) Info de sessão
            cur.execute("SELECT current_database(), current_user, now() AT TIME ZONE 'UTC';")
            db, usr, server_time = cur.fetchone()
            println("Sessão")
            print(f"database : {db}")
            print(f"user     : {usr}")
            print(f"utc time : {server_time}")

            schema = "public"

            # 2) Descobrir objetos do schema public
            println("Catálogo de objetos (schema public)")

            # Tabelas base
            tables = fetch_all(cur, """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema,))

            # Views
            views = fetch_all(cur, """
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = %s
                ORDER BY table_name
            """, (schema,))

            # Materialized Views (pg 9.3+)
            matviews = fetch_all(cur, """
                SELECT matviewname
                FROM pg_matviews
                WHERE schemaname = %s
                ORDER BY matviewname
            """, (schema,))

            print(f"Tabelas ..........: {len(tables)}")
            print(f"Views ............: {len(views)}")
            print(f"Materialized views: {len(matviews)}")

            # 3) Contar cada objeto
            println("Contagem por objeto")
            def print_counts(objs, kind):
                if not objs:
                    print(f"(sem {kind})")
                    return
                for (name,) in objs:
                    fq = f'{schema}."{name}"'
                    try:
                        total, is_est = count_rows(cur, fq)
                        marca = "~" if is_est else ""
                        total_fmt = "desconhecido" if total is None else f"{marca}{total:,}".replace(",", ".")
                        print(f"{kind:<18} {name:<30} -> {total_fmt}")
                    except Exception as e:
                        print(f"{kind:<18} {name:<30} -> erro: {e}")

            print_counts(tables, "table")
            print_counts(views, "view")
            print_counts(matviews, "matview")

            # 4) Destaque das tabelas/visões da Omie (se existirem)
            println("Destaques Omie (se existirem)")

            alvos = [
                "omie_webhook_events",
                "omie_nfe",
                "omie_nfe_itens",
                "omie_pedido",
                "omie_pedido_itens",
                "omie_pedido_status_hist",
                "vw_nf_pedido_std",     # view
                "mv_nf_pedido",         # matview (caso exista)
            ]

            for name in alvos:
                fq = f'{schema}."{name}"'
                try:
                    total, is_est = count_rows(cur, fq)
                    marca = "~" if is_est else ""
                    total_fmt = "desconhecido" if total is None else f"{marca}{total:,}".replace(",", ".")
                    print(f"{name:<22} -> {total_fmt}")
                except Exception as e:
                    # silencioso: apenas informa que não existe/sem permissão
                    print(f"{name:<22} -> não encontrado ou sem acesso ({e.__class__.__name__})")

    println("Fim")
    print(f"Concluído em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")

if __name__ == "__main__":
    main()

