# app_combined.py
# -------------------------------------------------------------------------
# Webhooks Omie (Pedidos + NFe/XML) + "chute" automático do processador
# -------------------------------------------------------------------------

import os
import json
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx
import asyncpg
from fastapi import FastAPI, APIRouter, Request, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse

# -----------------------------------------------------------------------------
# Configurações via ambiente (Render)
# -----------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "julia-matheus")

# Token do webhook de PEDIDOS (o mesmo que você configurou na Omie)
WEBHOOK_TOKEN_PEDIDO = os.getenv("WEBHOOK_TOKEN", "um-segredo-forte")

# Token do webhook de NFe/XML (o mesmo que você configurou na Omie)
WEBHOOK_TOKEN_XML = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "tiago-nati")

# -----------------------------------------------------------------------------
# App e Router
# -----------------------------------------------------------------------------
app = FastAPI(title="omie-webhook-render")
router = APIRouter()

# -----------------------------------------------------------------------------
# Conexão com Postgres (pool) + tabela da fila
# -----------------------------------------------------------------------------
_pg_pool = None

async def get_pool():
    global _pg_pool
    if _pg_pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL não configurado")
        _pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pg_pool

CREATE_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS omie_webhook_events (
    id           bigserial PRIMARY KEY,
    topic        text NOT NULL,
    status       text NOT NULL DEFAULT 'queued',
    payload      jsonb NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now(),
    attempts     int NOT NULL DEFAULT 0
);
"""

async def ensure_events_table():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(CREATE_EVENTS_TABLE_SQL)

# -----------------------------------------------------------------------------
# "Chute" automático do processador após cada webhook recebido
# -----------------------------------------------------------------------------
async def _kick_jobs(request: Request) -> None:
    """
    Faz POST no próprio /admin/run-jobs?secret=... usando o host da requisição.
    Se falhar, só loga; não bloqueia o webhook nem dá erro 5xx.
    """
    base = f"{request.url.scheme}://{request.headers.get('host')}"
    url = f"{base}/admin/run-jobs?{urlencode({'secret': ADMIN_SECRET})}"
    try:
        async with httpx.AsyncClient(timeout=10) as cli:
            await cli.post(url)
        print({"tag": "kick_ok", "url": url})
    except Exception as e:
        print({"tag": "kick_fail", "url": url, "err": str(e)})

# -----------------------------------------------------------------------------
# Healthchecks
# -----------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    return {
        "status": "healthy",
        "components": ["pedidos", "nfe_xml"],
        "compat": True,
    }

@router.get("/xml/healthz")
async def xml_healthz():
    return {"status": "saudável"}

# -----------------------------------------------------------------------------
# Webhook PEDIDOS  (endpoint usado na Omie para eventos de venda)
# -----------------------------------------------------------------------------
@router.post("/omie/webhook")
async def omie_webhook_pedidos(
    request: Request,
    background_tasks: BackgroundTasks,
    token: str
):
    if token != WEBHOOK_TOKEN_PEDIDO:
        raise HTTPException(status_code=401, detail="invalid token")

    # Lê o payload recebido da Omie (como veio)
    payload = await request.json()
    received_at = datetime.now(timezone.utc)

    await ensure_events_table()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO omie_webhook_events (topic, status, payload, received_at) "
            "VALUES ($1, 'queued', $2::jsonb, $3)",
            "pedido",
            json.dumps(payload),
            received_at,
        )

    # Chuta o processador de jobs sem bloquear o retorno do webhook
    background_tasks.add_task(_kick_jobs, request)

    print({"tag": "omie_webhook_received", "numero_pedido": str(payload.get("numero_pedido")), "id_pedido": str(payload.get("id_pedido"))})
    return JSONResponse({"ok": True, "received": True})

# (opcional) Ping do endpoint de pedidos
@router.get("/omie/webhook")
async def omie_webhook_pedidos_ping(token: str):
    if token != WEBHOOK_TOKEN_PEDIDO:
        raise HTTPException(status_code=401, detail="invalid token")
    return {"ok": True, "service": "omie_pedidos", "mode": "ping"}

# -----------------------------------------------------------------------------
# Webhook NFe/XML  (endpoint usado na Omie para eventos de nota fiscal)
# -----------------------------------------------------------------------------
@router.post("/xml/omie/webhook")
async def omie_webhook_xml(
    request: Request,
    background_tasks: BackgroundTasks,
    token: str
):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=401, detail="invalid token")

    payload = await request.json()
    received_at = datetime.now(timezone.utc)

    await ensure_events_table()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO omie_webhook_events (topic, status, payload, received_at) "
            "VALUES ($1, 'queued', $2::jsonb, $3)",
            "nfe",
            json.dumps(payload),
            received_at,
        )

    # Chuta o processador de jobs sem bloquear o retorno do webhook
    background_tasks.add_task(_kick_jobs, request)

    print({"tag": "omie_xml_received", "chave": str(payload.get("nfe_chave"))})
    return JSONResponse({"ok": True, "received": True})

# (opcional) Ping do endpoint de XML
@router.get("/xml/omie/webhook")
async def omie_webhook_xml_ping(token: str):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=401, detail="invalid token")
    return {"ok": True, "service": "omie_xml", "mode": "ping"}

# -----------------------------------------------------------------------------
# Monta as rotas no app
# -----------------------------------------------------------------------------
app.include_router(router)
