# -*- coding: utf-8 -*-
import os
import psycopg2
import psycopg2.extras

# >>>>> PREENCHA AQUI ou use variáveis de ambiente <<<<<
PG_HOST = os.getenv("PGHOST", "YOUR_HOST")
PG_DB   = os.getenv("PGDATABASE", "YOUR_DB")
PG_USER = os.getenv("PGUSER", "YOUR_USER")
PG_PW   = os.getenv("PGPASSWORD", "YOUR_PASSWORD")
PG_PORT = int(os.getenv("PGPORT", "5432"))

def get_conn():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PW, port=PG_PORT
    )

def query_dicts(sql, params=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()

def execute(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            conn.commit()
