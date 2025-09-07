# app_combined.py — serviço único (webhooks + jobs) para Omie (Pedidos + NF-e)
import os
import json
import logging
import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL")

WEBHOOK_TOKEN_PED = os.getenv("OMIE_WEBHOOK_TOKEN", "t1")
WEBHOOK_TOKEN_XML = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "t2")
ADMIN_RUNJOBS_SECRET = os.getenv("ADMIN_RUNJOBS_SECRET", "julia-matheus")

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT_SECONDS = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

OMIE_PEDIDO_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_XML_URL = "https://app.omie.com.br/api/v1/contador/xml/"
OMIE_XML_LIST_CALL = "ListarDocumentos"

ENRICH_PEDIDO_IMEDIATO = True
NFE_LOOKBACK_DAYS = 7
NFE_LOOKAHEAD_DAYS = 1

router = APIRouter()

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        async with app.state.pool.acquire() as conn:
            await _ensure_tables(conn)
        logger.info("✅ Database pool conectado e tabelas verificadas")
        yield
    finally:
        if hasattr(app.state, 'pool'):
            await app.state.pool.close()
            logger.info("✅ Database pool fechado")

app = FastAPI(title="Omie Webhooks + Jobs", lifespan=lifespan)

# ------------------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _as_json(obj: Any) -> Dict[str, Any]:
    try:
        if isinstance(obj, dict): return obj
        if isinstance(obj, str): return json.loads(obj)
    except Exception as e:
        logger.error(f"Erro ao converter para JSON: {e}")
    return {}

def _safe_text(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    return s or None

def _parse_dt(v: Any) -> Optional[datetime]:
    if not v: return None
    if isinstance(v, datetime): return v
    try:
        # Omie costuma mandar ISO com timezone: "2025-09-06T00:00:00-03:00"
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None

def _date_range_for_omie(data_emis: Optional[datetime]) -> Tuple[datetime, datetime]:
    if data_emis:
        start = (data_emis - timedelta(days=1)).astimezone(timezone.utc)
        end   = (data_emis + timedelta(days=2)).astimezone(timezone.utc)
    else:
        end   = _now_utc() + timedelta(days=NFE_LOOKAHEAD_DAYS)
        start = _now_utc() - timedelta(days=NFE_LOOKBACK_DAYS)
    return start, end

def _build_omie_body(call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"call": call, "app_key": OMIE_APP_KEY, "app_secret": OMIE_APP_SECRET, "param": [payload]}

async def _omie_post(client: httpx.AsyncClient, url: str, call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = _build_omie_body(call, payload)
    res = await client.post(url, json=body, timeout=OMIE_TIMEOUT_SECONDS)
    res.raise_for_status()
    j = res.json()
    if isinstance(j, dict) and j.get("faultstring"):
        raise RuntimeError(f"Omie error: {j.get('faultstring')}")
    return j

# ------------------------------------------------------------------------------
# DDL
# ------------------------------------------------------------------------------
async def _ensure_tables(conn: asyncpg.Connection) -> None:
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_webhook_events (
        id           bigserial PRIMARY KEY,
        source       text,
        event_type   text,
        route        text,
        http_status  integer,
        topic        text,
        status       text,
        raw_headers  jsonb,
        payload      jsonb,
        event_ts     timestamptz,
        received_at  timestamptz DEFAULT now(),
        processed    boolean DEFAULT false,
        processed_at timestamptz,
        error        text
    );""")

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_pedido (
        id_pedido_omie   bigint PRIMARY KEY,
        numero           text,
        valor_total      numeric,
        situacao         text,
        quantidade_itens integer,
        cliente_codigo   text,
        detalhe          jsonb,
        created_at       timestamptz DEFAULT now(),
        updated_at       timestamptz
    );""")

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe (
        chave_nfe         text PRIMARY KEY,
        numero            text,
        serie             text,
        emitida_em        timestamptz,
        cnpj_emitente     text,
        cnpj_destinatario text,
        valor_total       numeric(15,2),
        status            text,
        xml               text,
        xml_url           text,
        danfe_url         text,
        last_event_at     timestamptz,
        updated_at        timestamptz,
        recebido_em       timestamptz DEFAULT now(),
        raw               jsonb
    );""")

    await conn.execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='idx_omie_nfe_chave_unique'
      ) THEN
        BEGIN
          CREATE UNIQUE INDEX idx_omie_nfe_chave_unique ON public.omie_nfe (chave_nfe);
        EXCEPTION WHEN duplicate_table THEN
        END;
      END IF;
    END$$;""")

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe_xml (
        chave_nfe   text PRIMARY KEY,
        numero      text,
        serie       text,
        emitida_em  timestamptz,
        xml_base64  text,
        recebido_em timestamptz DEFAULT now(),
        created_at  timestamptz DEFAULT now(),
        updated_at  timestamptz
    );""")

# ------------------------------------------------------------------------------
# HEALTH
# ------------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    try:
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT now() AS now, count(*) AS pend FROM public.omie_webhook_events WHERE processed=false")
            return {"ok": True, "now": str(row["now"]), "pending_events": row["pend"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ------------------------------------------------------------------------------
# WEBHOOKS
# ------------------------------------------------------------------------------
@router.post("/omie/webhook")
async def pedidos_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_PED:
        raise HTTPException(status_code=403, detail="invalid token")
    try:
        body = await request.json()
    except Exception:
        body = {}
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source,event_type,route,http_status,topic,raw_headers,payload,event_ts,received_at,processed)
            VALUES ('omie','omie_web
