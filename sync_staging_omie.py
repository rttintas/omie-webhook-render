# -*- coding: utf-8 -*-
"""
Coleta dados da OMIE (clientes, produtos, pedidos, nfe) e grava em etl_staging_raw (PostgreSQL),
atualizando etl_sync_control com o último cursor. Usa env OMIE_* e PG*.
"""

import os, sys, json, argparse, time
from datetime import datetime, date
import requests
import psycopg2
from psycopg2.extras import Json

OMIE = {
    "clientes": {
        "url": "https://app.omie.com.br/api/v1/geral/clientes/",
        "call": "ListarClientes",
        "page_param": "pagina",
        "page_size_param": "registros_por_pagina",
        "page_size": 200
    },
    "produtos": {
        "url": "https://app.omie.com.br/api/v1/geral/produtos/",
        "call": "ListarProdutos",
        "page_param": "pagina",
        "page_size_param": "registros_por_pagina",
        "page_size": 200
    },
    "pedidos": {
        "url": "https://app.omie.com.br/api/v1/produtos/pedido/",
        "call": "ListarPedidos",
        "page_param": "pagina",
        "page_size_param": "registros_por_pagina",
        "page_size": 200
    },
    "nfe": {
        "url": "https://app.omie.com.br/api/v1/produtos/nfe/",
        "call": "ListarNFe",
        "page_param": "nPagina",
        "page_size_param": "nRegPorPagina",
        "page_size": 200
    }
}

def get_env(k, req=True):
    v = os.getenv(k) or ""
    if req and not v:
        print(f"[ERRO] Variável {k} não definida.")
        sys.exit(2)
    return v

def pg_conn():
    return psycopg2.connect(
        host=get_env("PGHOST"),
        port=get_env("PGPORT"),
        dbname=get_env("PGDATABASE"),
        user=get_env("PGUSER"),
        password=get_env("PGPASSWORD")
    )

def insert_staging(cur, servico, metodo, pagina, payload):
    cur.execute("""
        INSERT INTO etl_staging_raw (servico, metodo, pagina, payload_json)
        VALUES (%s, %s, %s, %s);
    """, (servico, metodo, pagina, Json(payload)))

def upsert_cursor(cur, servico, cursor_str):
    cur.execute("""
        INSERT INTO etl_sync_control (servico, ultimo_cursor, atualizado_em)
        VALUES (%s, %s, now())
        ON CONFLICT (servico)
        DO UPDATE SET ultimo_cursor = EXCLUDED.ultimo_cursor, atualizado_em = EXCLUDED.atualizado_em;
    """, (servico, cursor_str))

def omie_call(url, call, app_key, app_secret, params):
    body = {"call": call, "app_key": app_key, "app_secret": app_secret, "param": [params]}
    r = requests.post(url, json=body, timeout=50)
    try:
        data = r.json()
    except Exception:
        data = None
    return r.status_code, data

def guess_total_pages(serv, data):
    # tenta pegar total de páginas por campos comuns
    for k in ("nTotPaginas","total_de_paginas","nTotalPaginas","TotPaginas","totalPaginas"):
        if isinstance(data.get(k), int):
            return int(data[k])
    # fallback: se tiver lista vazia, retornamos 1
    return 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("entidade", choices=["clientes","produtos","pedidos","nfe"])
    ap.add_argument("--desde", help="Data inicial (YYYY-MM-DD) para NFe/Pedidos", default=None)
    ap.add_argument("--paginas", help="Número de páginas (ALL ou inteiro)", default="1")
    args = ap.parse_args()

    app_key = get_env("OMIE_APP_KEY")
    app_sec = get_env("OMIE_APP_SECRET")

    cfg = OMIE[args.entidade]
    url   = cfg["url"]
    call  = cfg["call"]
    pkey  = cfg["page_param"]
    skey  = cfg["page_size_param"]
    psz   = cfg["page_size"]

    # página inicial
    page = 1
    max_pages = None if args.paginas.upper()=="ALL" else int(args.paginas)

    # parâmetros base
    params = {pkey: page, skey: psz}

    # filtro por data (onde fizer sentido)
    if args.desde and args.entidade in ("pedidos","nfe"):
        # Muitos serviços da Omie filtram por data no request com chaves diferentes.
        # Para simplificar, não vamos enviar filtro no request agora; filtragem fina pode vir depois.
        pass

    conn = pg_conn()
    conn.autocommit = False
    cur = conn.cursor()

    print(f"[SYNC] {args.entidade} -> {url} :: {call}")

    total_coletadas = 0
    while True:
        params[pkey] = page
        status, data = omie_call(url, call, app_key, app_sec, params)
        if status != 200 or not isinstance(data, dict):
            print(f"[WARN] Falha na página {page}: status={status}. Encerrando.")
            break

        insert_staging(cur, url.replace("https://app.omie.com.br/api/v1/","").strip("/"), call, page, data)
        upsert_cursor(cur, args.entidade, f"pagina={page}")
        conn.commit()

        total_coletadas += 1
        print(f"  - Página {page} coletada.")

        # Decide próxima página
        tot = guess_total_pages(args.entidade, data)
        if max_pages is not None and page >= max_pages:
            break
        if tot and page >= tot:
            break

        page += 1
        time.sleep(0.3)

    cur.close()
    conn.close()
    print(f"[OK] Coletas gravadas no staging: {total_coletadas} páginas.")

if __name__ == "__main__":
    main()
