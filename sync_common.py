# sync_common.py
# Utilitários compartilhados pelos scripts de sincronização OMIE
from __future__ import annotations

import os
import json
import time
import logging
import argparse
import datetime as dt
from typing import Optional, Generator, Tuple, List, Any, Dict

import requests
import psycopg2
import psycopg2.extras


# -----------------------------------------------------------------------------
# Carregamento de configuração / .env
# -----------------------------------------------------------------------------
def _load_dotenv() -> None:
    """
    Carrega um .env se existir no diretório atual.
    Não é obrigatório; apenas ajuda em ambientes locais.
    """
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.isfile(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    # não sobrescreve se já houver no ambiente
                    if k not in os.environ:
                        os.environ[k] = v
        except Exception:
            # não falha se não conseguir ler .env
            pass


_load_dotenv()

# Lê variáveis aceitando tanto prefixo DB_* quanto PG*
DB_HOST = os.getenv("DB_HOST") or os.getenv("PGHOST") or "localhost"
DB_PORT = int(os.getenv("DB_PORT") or os.getenv("PGPORT") or 5432)
DB_NAME = os.getenv("DB_NAME") or os.getenv("PGDATABASE") or "postgres"
DB_USER = os.getenv("DB_USER") or os.getenv("PGUSER") or "postgres"
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("PGPASSWORD") or ""
DB_SCHEMA = (
    os.getenv("DB_SCHEMA")
    or os.getenv("PGSCHEMA")
    or "omie"  # padrão para este projeto
)

APP_KEY = os.getenv("OMIE_APP_KEY") or os.getenv("APP_KEY") or ""
APP_SECRET = os.getenv("OMIE_APP_SECRET") or os.getenv("APP_SECRET") or ""

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT") or 60)


# -----------------------------------------------------------------------------
# Logger
# -----------------------------------------------------------------------------
def setup_logger(name: str = "sync", debug: bool = False) -> logging.Logger:
    logger = logging.getLogger(name)
    # evita handlers duplicados quando recarregado
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    return logger


# -----------------------------------------------------------------------------
# Conexão com Postgres
# -----------------------------------------------------------------------------
def get_conn() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    # normalmente vamos querer autocommit para DDL/upserts simples
    conn.autocommit = True
    return conn


# -----------------------------------------------------------------------------
# Controle de checkpoint (schema.omie.sync_control)
# -----------------------------------------------------------------------------
def ensure_control_table(cur: psycopg2.extensions.cursor) -> None:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.sync_control(
            entidade   text PRIMARY KEY,
            last_sync_at timestamp
        );
        """
    )


def get_checkpoint(entidade: str, base_dt: Optional[dt.datetime] = None) -> dt.datetime:
    """
    Lê o checkpoint da tabela {schema}.sync_control.
    Se não existir, retorna base_dt (ou epoch).
    Compatível com chamadas antigas: apenas (entidade, base_dt).
    """
    base = base_dt or dt.datetime(1970, 1, 1)
    with get_conn() as conn, conn.cursor() as cur:
        ensure_control_table(cur)
        cur.execute(
            f"SELECT last_sync_at FROM {DB_SCHEMA}.sync_control WHERE entidade = %s;",
            (entidade,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return base


def set_checkpoint(entidade: str, valor: dt.datetime) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        ensure_control_table(cur)
        cur.execute(
            f"""
            INSERT INTO {DB_SCHEMA}.sync_control(entidade, last_sync_at)
            VALUES (%s, %s)
            ON CONFLICT (entidade) DO UPDATE
            SET last_sync_at = EXCLUDED.last_sync_at;
            """,
            (entidade, valor),
        )


# -----------------------------------------------------------------------------
# Chamadas OMIE
# -----------------------------------------------------------------------------
def omie_call(
    url: str,
    call_name: str,
    app_key: str,
    app_secret: str,
    req: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Envia POST json para a API Omie usando o contrato
    {"call": <call_name>, "app_key":..., "app_secret":..., "param":[{...}]}
    Levanta RuntimeError em status != 200.
    """
    payload = {
        "call": call_name,
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [req],
    }
    r = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} em {url}")
    try:
        return r.json()
    except Exception:
        # tenta parse parcial
        return json.loads(r.text)


def paginar(
    url: str,
    call_name: str,
    app_key: str,
    app_secret: str,
    desde_dt: Optional[dt.datetime] = None,
    page_size: int = 500,
    max_retries: int = 5,
) -> Generator[Tuple[int, List[Dict[str, Any]]], None, None]:
    """
    Itera as páginas do endpoint. Se a chamada com filtros de data falhar (ex.: 500),
    reexecuta automaticamente **sem filtros** (fallback).
    Gera tuplas (pagina, lista_de_registros).
    """
    pagina = 1
    while True:
        # request básico
        req: Dict[str, Any] = {"pagina": pagina, "registros_por_pagina": page_size}

        # filtros genéricos (alguns endpoints ignoram, outros quebram)
        ddmmyyyy = None
        if desde_dt:
            ddmmyyyy = desde_dt.strftime("%d/%m/%Y")
            req.update(
                {
                    "filtrar_por_data_de": ddmmyyyy,
                    "filtrar_por_data_alteracao_de": ddmmyyyy,
                    "data_de": ddmmyyyy,
                }
            )

        tried_without_filters = False
        while True:
            try:
                data = omie_call(url, call_name, app_key, app_secret, req)
                break  # ok
            except Exception as e:
                # Se falhou e tem filtros de data, tenta uma vez sem os filtros
                if not tried_without_filters and ddmmyyyy is not None:
                    for k in (
                        "filtrar_por_data_de",
                        "filtrar_por_data_alteracao_de",
                        "data_de",
                    ):
                        req.pop(k, None)
                    tried_without_filters = True
                    continue  # reenvia sem filtros

                # Caso contrário, tenta algumas vezes com backoff leve
                last_exc = e
                for i in range(max_retries - 1):
                    time.sleep(2)
                    try:
                        data = omie_call(url, call_name, app_key, app_secret, req)
                        last_exc = None
                        break
                    except Exception as e2:
                        last_exc = e2
                        continue
                if last_exc is not None:
                    raise RuntimeError(
                        f"Falha OMIE após {max_retries} tentativas"
                    ) from last_exc
                break  # saiu pelo sucesso dentro do loop de retry

        # Descobrir a lista dentro do payload (varia por endpoint)
        lista: List[Dict[str, Any]] = []
        for key in (
            "clientes_cadastro",
            "pedido",
            "pedido_produto",
            "produtos",
            "produto_servico",
            "nfe",
            "notas_fiscais",
            "lista",
        ):
            if key in data and isinstance(data[key], list):
                lista = data[key]
                break

        # Alguns endpoints trazem {"lista": {"itens":[...]}}
        if not lista and isinstance(data.get("lista"), dict):
            for v in data["lista"].values():
                if isinstance(v, list):
                    lista = v
                    break

        yield pagina, (lista or [])

        total_paginas = data.get("total_de_paginas") or data.get("total_pages") or 1
        try:
            total_paginas = int(total_paginas)
        except Exception:
            total_paginas = 1

        if pagina >= total_paginas:
            break
        pagina += 1


# -----------------------------------------------------------------------------
# CLI comum
# -----------------------------------------------------------------------------
def parse_args(description: str = "Sync OMIE", default_base: Optional[str] = None):
    """
    Retorna args com:
      --desde YYYY-MM-DD
      --debug
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--desde",
        help="Data base no formato YYYY-MM-DD (padrão: não aplicar filtro)",
        default=default_base,
    )
    parser.add_argument(
        "--debug",
        help="Ativa logs de depuração",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()

    if args.desde:
        # converte para datetime à meia-noite
        args.desde = dt.datetime.strptime(args.desde, "%Y-%m-%d")
    else:
        args.desde = None
    return args


# -----------------------------------------------------------------------------
# Execução direta: imprime config e testa conexão
# -----------------------------------------------------------------------------
def _print_env_and_test():
    cfg = {
        "PGHOST/DB_HOST": DB_HOST,
        "PGPORT/DB_PORT": DB_PORT,
        "PGDATABASE/DB_NAME": DB_NAME,
        "PGUSER/DB_USER": DB_USER,
        "PGSCHEMA/DB_SCHEMA": DB_SCHEMA,
        "APP_KEY?": "OK" if APP_KEY else "MISSING",
        "APP_SECRET?": "OK" if APP_SECRET else "MISSING",
    }
    print("ENV / CONFIG:")
    print(json.dumps(cfg, indent=2, ensure_ascii=False))
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_schema(), current_setting('search_path');")
            row = cur.fetchone()
            print(f"PG: {row}")
    except Exception as e:
        print("Falha ao conectar no Postgres:", e)


if __name__ == "__main__":
    _print_env_and_test()
