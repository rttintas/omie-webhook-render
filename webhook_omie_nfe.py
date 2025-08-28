import os
import json
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse

APP_NAME = "omie-webhook"
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
PG_DSN = os.getenv("PG_DSN", "")
AUTO_MIGRATE = os.getenv("AUTO_MIGRATE", "0") in ("1", "true", "True")

app = FastAPI(title="Omie Webhook", version="1.0.0")

# Conexão com Postgres (pool)
db_pool: Optional[asyncpg.pool.Pool] = None


async def create_schema_if_needed(conn: asyncpg.Connection) -> None:
    """
    Cria a tabela de log se ainda não existir.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS omie_nf_webhook_log (
        id            BIGSERIAL PRIMARY KEY,
        received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        remote_addr   TEXT,
        user_agent    TEXT,
        payload       JSONB NOT NULL
    );
    """
    await conn.execute(ddl)


def client_ip(request: Request) -> str:
    # Se estiver atrás do Cloudflare, ele envia CF-Connecting-IP
    cf_ip = request.headers.get("CF-Connecting-IP")
    if cf_ip:
        return cf_ip
    # Fallback do Uvicorn
    return request.client.host if request.client else "unknown"


@app.on_event("startup")
async def on_startup() -> None:
    global db_pool
    if PG_DSN:
        db_pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
        if AUTO_MIGRATE:
            async with db_pool.acquire() as conn:
                await create_schema_if_needed(conn)
        print(f"[{APP_NAME}] DB pool conectado.")
    else:
        print(f"[{APP_NAME}] Atenção: variável PG_DSN não definida. Persistência desabilitada.")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global db_pool
    if db_pool:
        await db_pool.close()
        print(f"[{APP_NAME}] DB pool fechado.")


@app.get("/health")
async def health() -> Dict[str, Any]:
    """
    Health-check simples. Se o banco estiver configurado, tenta um SELECT 1.
    """
    status_db = "disabled"
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1;")
            status_db = "ok"
        except Exception as e:
            status_db = f"error: {e.__class__.__name__}"

    return {
        "status": "ok",
        "db": status_db,
        "at": datetime.now(timezone.utc).isoformat(),
        "app": APP_NAME,
    }


@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str) -> JSONResponse:
    """
    Recebe eventos do OMI·E.
    Autorização é por 'token' na query (?token=...).
    Corpo deve ser JSON.
    """
    # 1) Autorização por token
    if not WEBHOOK_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WEBHOOK_TOKEN não configurado no servidor."
        )
    if token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido.")

    # 2) Leitura do JSON
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="JSON inválido.")

    # 3) Enriquecer com metadados úteis
    meta = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "remote_addr": client_ip(request),
        "user_agent": request.headers.get("User-Agent", ""),
    }

    # 4) Persistir (se banco configurado)
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO omie_nf_webhook_log (received_at, remote_addr, user_agent, payload)
                    VALUES (now(), $1, $2, $3::jsonb)
                    """,
                    meta["remote_addr"],
                    meta["user_agent"],
                    json.dumps(payload),
                )
        except Exception as e:
            # Se algo falhar no banco, ainda devolvemos 202 para o OMI·E não reter a fila,
            # mas registramos o erro no corpo de resposta.
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={"status": "accepted_with_db_error", "error": str(e), "meta": meta},
            )

    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "ok", "meta": meta})
