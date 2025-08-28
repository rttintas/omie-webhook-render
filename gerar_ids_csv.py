#!/usr/bin/env python3
import os
import csv
import argparse
from datetime import datetime, timedelta, date
import psycopg2

SQL = """
SELECT
    n.numero_nfe::text       AS id,
    n.chave_nfe::text        AS chave,
    n.data_emissao::date     AS emissao
FROM omie.omie_nfe n
WHERE n.data_emissao >= %s
  AND n.data_emissao <  %s
ORDER BY n.data_emissao DESC, n.numero_nfe DESC;
"""

def parse_args():
    p = argparse.ArgumentParser(
        description="Gera ids.csv com numero_nfe/chave/data_emissao a partir da tabela omie.omie_nfe."
    )
    p.add_argument("--desde", help="Data inicial (YYYY-MM-DD). Default: ontem 00:00", type=str)
    p.add_argument("--ate",   help="Data final exclusiva (YYYY-MM-DD). Default: hoje 00:00", type=str)
    p.add_argument("--csv",   help="Caminho do CSV de saída", default="ids.csv")
    return p.parse_args()

def resolve_interval(args):
    # default: ontem [00:00, hoje 00:00)
    if args.desde:
        d0 = datetime.strptime(args.desde, "%Y-%m-%d").date()
    else:
        d0 = date.today() - timedelta(days=1)

    if args.ate:
        d1 = datetime.strptime(args.ate, "%Y-%m-%d").date()
    else:
        d1 = date.today()

    # datas “exclusivas” na consulta (d1 é limite superior)
    return (d0, d1)

def main():
    args = parse_args()
    d0, d1 = resolve_interval(args)

    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise SystemExit("ERRO: variável de ambiente PG_DSN não definida.")

    print(f"[INFO] Intervalo: {d0} -> {d1} (exclusivo)")
    print(f"[INFO] Conectando com PG_DSN='{dsn}'")

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL, (d0, d1))
            rows = cur.fetchall()

    out = args.csv
    # utf-8-sig para abrir no Excel sem bagunça de acentuação
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["id", "chave", "emissao"])
        w.writerows(rows)

    print(f"[OK] Gerado {out} com {len(rows)} linhas.")

if __name__ == "__main__":
    main()
