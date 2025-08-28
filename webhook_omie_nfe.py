# webhook_omie_nfe.py
# FastAPI + (opcional) Postgres para receber webhooks da Omie via Cloudflare Tunnel
# Autor: você 😊

import os
import json
import asyncio
from datetime import datetime, timezone
from typing import Optional, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

# ---- Config por variáveis de ambiente ---------------------------------------
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "um-segredo-forte")
PG_DSN = os.getenv("PG_DSN")  # exemplo: host=localhost port=5432 dbname=omie_dw user=postgres password=123

# asyncpg é opcional. Se não instalado, o app funciona sem banco.
POOL = None
try:
    import asyncpg  # type: ignore
except Exception:
    asyncpg = None  # sem dependência -> funciona sem DB

# ---- App ---------------------------------------------------------------------
app = FastAPI(title="Webhook Omie NFe", version="1.0.0")


# ---- Helpers -----------------------------------------------------------------
def utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


async def safe_json(body_bytes: bytes) -> Optional[Any]:
    """Tenta fazer parse de JSON; se falhar, retorna None."""
    if not body_bytes:
        return None
    try:
        return json.loads(body_bytes.decode("utf-8"))
    except Exception:
        return None


async def ensure_table(conn) -> None:
    """Cria tabela genérica para log de eventos, se não existir."""
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS omie_webhook_events (
            id              BIGSERIAL PRIMARY KEY,
            received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            topic           TEXT NULL,
            payload_json    JSONB NULL,
            payload_raw     TEXT NULL,
            source_ip       TEXT NULL
        );
        """
    )


async def insert_event(pool, *, topic: Optional[str], payload_json, payload_raw: Optional[str], ip: Optional[str]):
    async with pool.acquire() as conn:
        await ensure_table(conn)
        await conn.execute(
            """
            INSERT INTO omie_webhook_events (topic, payload_json, payload_raw, source_ip)
            VALUES ($1, $2, $3, $4);
            """,
            topic,
            payload_json,
            payload_raw,
            ip,
        )


# ---- Lifespan / DB pool ------------------------------------------------------
@app.on_event("startup")
async def _startup():
    global POOL
    if PG_DSN and asyncpg:
        try:
            POOL = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
            print("[DB] Pool conectado.")
        except Exception as e:
            print("[DB] Falha ao conectar pool:", repr(e))
            POOL = None
    else:
        if not PG_DSN:
            print("[DB] PG_DSN não definido. Rodando SEM banco.")
        else:
            print("[DB] asyncpg não instalado. Rodando SEM banco.")


@app.on_event("shutdown")
async def _shutdown():
    global POOL
    if POOL:
        await POOL.close()
        print("[DB] Pool fechado.")


# ---- Endpoints ---------------------------------------------------------------
@app.get("/health")
async def health():
    db = "disabled"
    if POOL:
        try:
            async with POOL.acquire() as conn:
                await conn.execute("SELECT 1;")
            db = "ok"
        except Exception as e:
            db = f"error: {e!r}"
    return {"status": "ok", "db": db, "at": utcnow_iso()}


@app.api_route("/omie/webhook", methods=["GET", "POST"])
async def omie_webhook(request: Request):
    """
    - Valida token pela query (?token=...)
    - Lê corpo de forma tolerante (JSON, texto, ou vazio)
    - Não levanta exceção em casos de teste do Omie; retorna 200 sempre que token for válido
    - Se PG_DSN estiver definido e asyncpg instalado, guarda o evento
    """
    # 1) validação do token
    token = request.query_params.get("token")
    if token != WEBHOOK_TOKEN:
        print("[WEBHOOK] token inválido ou ausente:", token)
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 2) captura IP de origem (útil para debug)
    client_ip = None
    if request.client:
        client_ip = request.client.host

    # 3) tenta ler body
    body_bytes = await request.body()
    payload_json = await safe_json(body_bytes)
    payload_raw: Optional[str] = None

    # 4) se não for JSON, guarda como texto bruto (útil p/ XML)
    if payload_json is None and body_bytes:
        payload_raw = body_bytes.decode("utf-8", errors="ignore")

    # 5) tenta descobrir "topic" (alguns sistemas mandam cabeçalho)
    # Omie não documenta um header fixo p/ tópico nos webhooks públicos,
    # então fica opcional/heurístico. Deixamos vazio.
    topic = request.headers.get("X-Event-Topic") or None

    print("[WEBHOOK] recebido @", utcnow_iso(), "| ip:", client_ip)
    if payload_json is not None:
        print("[WEBHOOK] JSON:", payload_json)
    elif payload_raw:
        print("[WEBHOOK] RAW:", payload_raw[:500], "..." if len(payload_raw) > 500 else "")
    else:
        print("[WEBHOOK] (sem corpo)")

    # 6) persiste se houver DB
    if POOL:
        try:
            await insert_event(
                POOL,
                topic=topic,
                payload_json=payload_json,
                payload_raw=payload_raw,
                ip=client_ip,
            )
        except Exception as e:
            # não quebrar a resposta do webhook por erro de DB
            print("[DB] Falha ao inserir evento:", repr(e))

    # 7) resposta OK (necessário p/ Omie aceitar o endpoint)
    return JSONResponse({"ok": True}, status_code=200)


# --- Fim do arquivo -----------------------------------------------------------
# Execução local (opcional):
# uvicorn webhook_omie_nfe:app --host 0.0.0.0 --port 8000 --reload
