# sync_nfe_dfedocs.py
# Consulta Omie DFeDocs -> ObterNfe e grava em Postgres
from __future__ import annotations

import os, sys, csv, json, time, logging, argparse, datetime as dt
from typing import Any, Dict, Iterable, List, Tuple, Optional

import requests
import psycopg2
from psycopg2.extras import execute_values

OMIE_URL = "https://app.omie.com.br/api/v1/produtos/dfedocs/"
CALL_OBTER_NFE = "ObterNfe"

# ------------------------ logging ------------------------
def setup_logging(debug: bool):
    lvl = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# ------------------------ utils ------------------------
def try_parse_date(d: Optional[str]) -> Optional[dt.datetime]:
    if not d:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(d, fmt)
        except Exception:
            pass
    return None

def load_ids(args) -> List[int]:
    ids: List[int] = []
    if args.ids:
        for tok in str(args.ids).replace(";", ",").split(","):
            tok = tok.strip()
            if tok:
                try:
                    ids.append(int(tok))
                except:
                    logging.warning("Ignorando ID inválido: %r", tok)
    if args.csv:
        with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.DictReader(f)
            # aceita cabeçalhos: nIdNFe, id, nidnfe
            cand_cols = [c for c in rdr.fieldnames or [] if c.lower() in ("nidnfe","nidnfe","id","nIdNFe".lower())]
            col = cand_cols[0] if cand_cols else (rdr.fieldnames or [""])[0]
            for row in rdr:
                tok = (row.get("nIdNFe") or row.get("nidnfe") or row.get("id") or row.get(col) or "").strip()
                if tok:
                    try:
                        ids.append(int(tok))
                    except:
                        logging.warning("Ignorando ID inválido do CSV: %r", tok)
    # remove duplicados mantendo ordem
    seen=set(); dedup=[]
    for i in ids:
        if i not in seen:
            seen.add(i); dedup.append(i)
    return dedup

# ------------------------ OMIE ------------------------
def omie_call(url: str, call: str, app_key: str, app_secret: str, params: Dict[str, Any]) -> Dict[str, Any]:
    body = {
        "call": call,
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [params],
    }
    r = requests.post(url, json=body, timeout=60)
    logging.debug("POST %s -> %s", url, r.status_code)
    try:
        data = r.json()
    except Exception:
        data = {"_raw": r.text}
    if r.status_code != 200:
        logging.debug("OMIE BODY: %s", json.dumps(data, ensure_ascii=False))
        raise RuntimeError(f"HTTP {r.status_code} em {url}")
    # Omie às vezes retorna 200 com erro no corpo
    if isinstance(data, dict) and "status" in data and str(data.get("status")).lower()=="error":
        logging.debug("OMIE BODY: %s", json.dumps(data, ensure_ascii=False))
        raise RuntimeError(data.get("message","erro Omie"))
    return data

def obter_nfe(app_key: str, app_secret: str, nid: int) -> Dict[str, Any]:
    return omie_call(OMIE_URL, CALL_OBTER_NFE, app_key, app_secret, {"nIdNFe": nid})

# ------------------------ DB schema ------------------------
DDL_CREATE = """
CREATE SCHEMA IF NOT EXISTS omie;

CREATE TABLE IF NOT EXISTS omie.omie_nfe (
  chave_nfe   text PRIMARY KEY,
  numero_nfe  integer,
  serie_nfe   text,
  data_emissao timestamp without time zone,
  valor_total numeric
);

CREATE TABLE IF NOT EXISTS omie.omie_nfe_docs (
  chave_nfe   text PRIMARY KEY,
  xml         text,
  pdf_link    text,
  portal_link text,
  cod_status  text,
  des_status  text,
  fetched_at  timestamp without time zone DEFAULT now()
);
"""

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_CREATE)
    conn.commit()

# ------------------------ UPSERTS ------------------------
def upsert_nfe(cur, payload: Dict[str, Any]):
    chave = (payload.get("nChaveNfe") or payload.get("cChaveNfe") or "").strip()
    if not chave:
        return
    numero = payload.get("cNumNfe") or payload.get("nNumNfe")
    try:
        numero_int = int(str(numero)) if numero is not None else None
    except:
        numero_int = None
    serie = payload.get("cSerieNfe") or payload.get("serie") or ""
    dtem = try_parse_date(payload.get("dDataEmisNfe")) or dt.datetime(1970,1,1)
    cur.execute("""
        INSERT INTO omie.omie_nfe (chave_nfe, numero_nfe, serie_nfe, data_emissao, valor_total)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (chave_nfe) DO UPDATE SET
            numero_nfe = EXCLUDED.numero_nfe,
            serie_nfe  = EXCLUDED.serie_nfe,
            data_emissao = EXCLUDED.data_emissao
    """, (chave, numero_int, serie, dtem, None))

def upsert_docs(cur, payload: Dict[str, Any]):
    """Grava XML/PDF/links/status. Pula respostas sem chave (ex.: 1011)."""
    chave = (payload.get("nChaveNfe") or payload.get("cChaveNfe") or "").strip()
    cod   = str(payload.get("cCodStatus") or "").strip()
    des   = (payload.get("cDesStatus") or "").strip()
    xml   = payload.get("cXmlNfe") or ""
    pdf   = payload.get("cPdf") or ""
    link  = payload.get("cLinkPortal") or ""
    if not chave:
        logging.info("Resposta sem chave (provável 1011: %s). Ignorando gravação.", des)
        return
    cur.execute("""
        INSERT INTO omie.omie_nfe_docs
            (chave_nfe, xml, pdf_link, portal_link, cod_status, des_status, fetched_at)
        VALUES (%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (chave_nfe) DO UPDATE SET
            xml         = EXCLUDED.xml,
            pdf_link    = EXCLUDED.pdf_link,
            portal_link = EXCLUDED.portal_link,
            cod_status  = EXCLUDED.cod_status,
            des_status  = EXCLUDED.des_status,
            fetched_at  = now()
    """, (chave, xml, pdf, link, cod, des))

# ------------------------ main ------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Sync NF-e via DFeDocs.ObterNfe (Omie) -> Postgres")
    p.add_argument("--ids", help="Lista de nIdNFe (ex: 111,222,333)")
    p.add_argument("--csv", help="Arquivo CSV com coluna nIdNFe")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--dry-run", action="store_true")

    p.add_argument("--app-key", default=os.getenv("OMIE_APP_KEY", ""))
    p.add_argument("--app-secret", default=os.getenv("OMIE_APP_SECRET", ""))

    p.add_argument("--pg-dsn", default=os.getenv("PG_DSN",""))
    p.add_argument("--pg-host", default=os.getenv("PGHOST","localhost"))
    p.add_argument("--pg-port", default=os.getenv("PGPORT","5432"))
    p.add_argument("--pg-db",   default=os.getenv("PGDATABASE","postgres"))
    p.add_argument("--pg-user", default=os.getenv("PGUSER","postgres"))
    p.add_argument("--pg-pass", default=os.getenv("PGPASSWORD","postgres"))
    return p.parse_args()

def build_conn(args):
    if args.pg_dsn:
        return psycopg2.connect(args.pg_dsn)
    return psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname=args.pg_db, user=args.pg_user, password=args.pg_pass
    )

def main():
    args = parse_args()
    setup_logging(args.debug)
    logging.info("== INÍCIO SYNC_NFE_DFEDOCS ==")

    ids = load_ids(args)
    logging.info("Total de IDs a consultar: %d", len(ids))
    if not ids:
        logging.info("Nada a fazer.")
        return

    if not args.app_key or not args.app_secret:
        logging.error("Informe OMIE APP KEY/SECRET via --app-key/--app-secret ou variáveis de ambiente.")
        sys.exit(2)

    if args.dry_run:
        # apenas mostra a primeira resposta
        first = ids[0]
        data = obter_nfe(args.app_key, args.app_secret, first)
