# scripts/sync.py
# -- coding: utf-8 --

"""
Sync Omie -> Postgres (incremental)
Entidades:
  - clientes        -> omie.omie_clientes
  - produtos        -> omie.omie_produtos
  - pedidos+itens   -> omie.omie_pedidos / omie.omie_pedido_itens
  - nfe             -> omie.omie_nfe
Controle incremental:
  - omie.sync_control (entidade text PK, last_sync_at timestamp)
Requisitos:
  pip install requests psycopg2-binary python-dateutil
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Iterable
import requests
import psycopg2
from psycopg2.extras import execute_values
from dateutil.parser import parse as dt_parse

# =========================
# CONFIG
# =========================
OMIE_APP_KEY    = os.getenv("OMIE_APP_KEY",    "<SUA_APP_KEY_AQUI>")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "<SEU_APP_SECRET_AQUI>")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "omie_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

PAGE_SIZE   = 100
REQ_TIMEOUT = 60

# Endpoints padrão Omie (HTTP POST com JSON)
URL_CLIENTES = "https://app.omie.com.br/api/v1/geral/clientes/"
URL_PRODUTOS = "https://app.omie.com.br/api/v1/geral/produtos/"
URL_PEDIDOS  = "https://app.omie.com.br/api/v1/produtos/pedido/"   # pedidos de venda
URL_NFE      = "https://app.omie.com.br/api/v1/produtos/nfemitidas/"

# Logging legível e sem erro de formatação de data
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sync")

# =========================
# DB helpers
# =========================
def get_conn():
    dsn = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASS}"
    return psycopg2.connect(dsn)

def ensure_sync_control(conn):
    with conn.cursor() as cur:
        cur.execute("""
            create schema if not exists omie;
            create table if not exists omie.sync_control (
              entidade   text primary key,
              last_sync_at timestamp without time zone
            );
        """)
        cur.execute("""
            insert into om