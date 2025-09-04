# main.py — Omie Webhook API (FastAPI + asyncpg + httpx)
import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
APP_NAME = "Omie Webhook API"
TZ = timezone.utc

DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "false").lower() == "true"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado.")

OMIE_PEDIDO_ENDPOINT = "https://app.omie.com.br/api/v1/produtos/pedido/"

# ------------------------------------------------------------------------------
# App & Logger
# ------------------------------------------------------------------------------
app = FastAPI(title=APP_NAME)
logger = logging.getLogger("uvicorn")

def jlog(level: str, **data):
    msg = json.dumps(data, ensure_ascii=False, default=str)
    getattr(logger, level, logger.info)(msg)

# ------------------------------------------------------------------------------
# DB Pool
# ------------------------------------------------------------------------------
pool: Optional[asyncpg.pool.Pool] = None

async def db_init_schema():
    async with pool.acquire() as conn:
        # pedidos
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_pedido (
            id_pedido      BIGSERIAL PRIMARY KEY,
            numero         TEXT UNIQUE,
            id_pedido_omie BIGINT,
            valor_total    NUMERIC,
            status         TEXT DEFAULT 'pendente_consulta',
            raw_basico     JSONB,
            raw_detalhe    JSONB,
            recebido_em    TIMESTAMPTZ DEFAULT now()
        );
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_pedido_numero ON public.omie_pedido (numero);")

        # nfe (campos que você já tem no Neon)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_nfe (
            chave_nfe     TEXT,
            numero        TEXT,
            serie         TEXT,
            emitida_em    TIMESTAMPTZ,
            cnpj_emitente TEXT,
            cnpj_destinatario TEXT,
            valor_total   NUMERIC,
            status        TEXT,
            xml           TEXT,
            pdf_url       TEXT,
            last_event_at TIMESTAMPTZ,
            updated_at    TIMESTAMPTZ,
            data_emissao  TEXT,
            cliente_nome  TEXT,
            raw           JSONB,
            created_at    TIMESTAMPTZ DEFAULT now(),
            recebido_em   TIMESTAMPTZ DEFAULT now(),
            id_nfe        BIGINT,
            danfe_url     TEXT,
            xml_url       TEXT
        );
        """)

        # jobs
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_jobs (
            id         BIGSERIAL PRIMARY KEY,
            job_type   TEXT NOT NULL,
            status     TEXT NOT NULL DEFAULT 'pending',
            attempts   INT  NOT NULL DEFAULT 0,
            next_run   TIMESTAMPTZ DEFAULT now(),
            last_error TEXT,
            payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_jobs_status_next_run ON public.omie_jobs(status, next_run);")

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _ensure_dict(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return {}
    try:
        return dict(payload)
    except Exception:
        return {}

async def upsert_pedido_basico(numero: Optional[str], id_pedido: Optional[int],
                               valor: Optional[float], raw: Dict[str, Any]):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_pedido (numero, id_pedido_omie, valor_total, status, raw_basico, recebido_em)
            VALUES ($1, $2, $3, 'pendente_consulta', $4::jsonb, now())
            ON CONFLICT (numero) DO UPDATE SET
               id_pedido_omie = EXCLUDED.id_pedido_omie,
               valor_total    = COALESCE(EXCLUDED.valor_total, public.omie_pedido.valor_total),
               raw_basico     = EXCLUDED.raw_basico,
               recebido_em    = now()
        """, numero, id_pedido, valor, json.dumps(raw))

async def enqueue_job(job_type: str, payload: Dict[str, Any], delay_seconds: int = 0):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_jobs (job_type, status, attempts, next_run, payload, created_at, updated_at)
            VALUES ($1, 'pending', 0, now() + ($2 || ' seconds')::interval, $3::jsonb, now(), now())
        """, job_type, delay_seconds, json.dumps(payload))

async def pick_pending_jobs(limit: int = 20):
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT id, job_type, status, attempts, next_run, last_error, payload
              FROM public.omie_jobs
             WHERE status='pending' AND next_run <= now()
             ORDER BY next_run ASC
             LIMIT $1
        """, limit)

async def update_job_done(job_id: int):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE public.omie_jobs
               SET status='done', updated_at=now(), last_error=NULL
             WHERE id=$1
        """, job_id)

async def update_job_fail(job_id: int, err: str):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE public.omie_jobs
               SET attempts = attempts + 1,
                   last_error = $1,
                   updated_at = now(),
                   next_run = now() + interval '2 minutes'
             WHERE id=$2
        """, err[:800], job_id)

async def set_pedido_detalhe(numero: str, detalhe: Dict[str, Any]):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE public.omie_pedido
               SET raw_detalhe = $1::jsonb,
                   status='consultado'
             WHERE numero=$2
        """, json.dumps(detalhe), numero)

# ------------------------------------------------------------------------------
# Omie API (httpx)
# ------------------------------------------------------------------------------
async def omie_consultar_pedido(id_pedido: Optional[int], numero_pedido: Optional[str]) -> Dict[str, Any]:
    if not OMIE_APP_KEY or not OMIE_APP_SECRET:
        raise RuntimeError("OMIE_APP_KEY/OMIE_APP_SECRET não configurados.")
    call = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "numero_pedido": str(numero_pedido) if numero_pedido else None,
            "id_pedido": int(id_pedido) if id_pedido else None
        }]
    }
    # remove chaves None
    call["param"][0] = {k: v for k, v in call["param"][0].items() if v is not None}

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(OMIE_PEDIDO_ENDPOINT, json=call)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("faultstring"):
            raise RuntimeError(f"Omie fault: {data.get('faultstring')}")
        return data

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await db_init_schema()
    jlog("info", tag="startup", msg="Pool OK & schema verificado.")

@app.get("/health/app")
async def health_app():
    return {"ok": True, "name": APP_NAME}

@app.get("/health/db")
async def health_db():
    try:
        async with pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1;")
        return {"db": "ok", "select1": val}
    except Exception as e:
        return JSONResponse(status_code=500, content={"db": "error", "detail": str(e)})

# ---------- Webhook ----------
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: Optional[str] = Query(default=None)):
    if token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

    # aceita JSON puro; se vier form com campo "json", tenta ler também
    payload: Dict[str, Any]
    try:
        if "application/json" in (request.headers.get("content-type") or ""):
            payload = await request.json()
        else:
            form = await request.form()
            if "json" in form:
                payload = json.loads(form["json"])
            else:
                body = (await request.body()).decode() or "{}"
                payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="payload parse error")

    event = _ensure_dict(payload.get("event") or payload.get("evento") or {})
    numero_pedido = str(event.get("numeroPedido") or event.get("numero_pedido") or "")

    _id_pedido = event.get("idPedido") or event.get("id_pedido")
    try:
        id_pedido_omie = int(_id_pedido) if _id_pedido not in (None, "", "None") else None
    except Exception:
        id_pedido_omie = None

    try:
        valor_pedido = float(event.get("valorPedido") or event.get("valor_pedido") or 0) or None
    except Exception:
        valor_pedido = None

    await upsert_pedido_basico(numero_pedido or None, id_pedido_omie, valor_pedido, payload)

    job_payload = {"id_pedido": id_pedido_omie, "numero_pedido": numero_pedido}
    if ENRICH_PEDIDO_IMEDIATO and (id_pedido_omie or numero_pedido):
        try:
            data = await omie_consultar_pedido(id_pedido_omie, numero_pedido)
            await set_pedido_detalhe(numero_pedido, data)
            jlog("info", tag="webhook_enriched_immediate", numero=numero_pedido, id_pedido=id_pedido_omie)
        except Exception as e:
            jlog("warning", tag="webhook_enrich_fail", numero=numero_pedido, id_pedido=id_pedido_omie, error=str(e))
            await enqueue_job("pedido.consultar", job_payload)
    else:
        await enqueue_job("pedido.consultar", job_payload)

    jlog("info", tag="omie_webhook_received",
         numero_pedido=numero_pedido, id_pedido=id_pedido_omie)

    return {"ok": True}

# ---------- Admin: processa fila ----------
@app.post("/admin/run-jobs")
async def admin_run_jobs(secret: Optional[str] = Query(default=None), limit: int = 20):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    jobs = await pick_pending_jobs(limit)
    ran = 0
    for job in jobs:
        job_id = job["id"]
        payload = _ensure_dict(job["payload"])
        jtype = job["job_type"]

        try:
            if jtype == "pedido.consultar":
                numero = payload.get("numero_pedido")
                id_ped = payload.get("id_pedido")
                if not numero and not id_ped:
                    raise RuntimeError("payload sem numero_pedido ou id_pedido")

                data = await omie_consultar_pedido(id_ped, numero)
                await set_pedido_detalhe(numero, data)

            else:
                jlog("warning", tag="job_unknown", id=job_id, job_type=jtype)

            await update_job_done(job_id)
            jlog("info", tag="job_done", id=job_id)
            ran += 1

        except Exception as e:
            await update_job_fail(job_id, str(e))
            jlog("error", tag="job_fail", id=job_id, error=str(e))

    return {"ok": True, "picked": len(jobs), "ran": ran}

# ---------- Admin: stats ----------
@app.get("/admin/jobs-stats")
async def jobs_stats(secret: Optional[str] = Query(default=None)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT status, COUNT(*) AS qnt FROM public.omie_jobs GROUP BY status;")
    return {"ok": True, "by_status": [{ "status": r["status"], "qnt": int(r["qnt"]) } for r in rows]}
