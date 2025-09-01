# webhook_omie_nfe.py — Versão completa e funcional

import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse

APP_NAME = "omie-webhook"
PG_DSN = os.getenv("PG_DSN")                  # ex: postgresql://user:pass@host/db?sslmode=require
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN")    # opcional; se definido, valida ?token=...

app = FastAPI(title=APP_NAME, version="1.0.0")
_pool: Optional[asyncpg.Pool] = None

# DDLs para as tabelas (caso não existam)
DDL_WEBHOOK_EVENTS = """
CREATE TABLE IF NOT EXISTS public.omie_webhook_events (
  id           BIGSERIAL PRIMARY KEY,
  received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_type   TEXT,
  event_id     TEXT,
  payload      JSONB,
  raw_headers  JSONB,
  http_status  INT
);
"""

DDL_PEDIDO = """
CREATE TABLE IF NOT EXISTS public.omie_pedido (
    numero_pedido TEXT PRIMARY KEY,
    cnpj_destinatario TEXT,
    raw JSONB
);
"""

DDL_NFE = """
CREATE TABLE IF NOT EXISTS public.omie_nfe (
    numero TEXT PRIMARY KEY,
    cnpj_destinatario TEXT,
    raw JSONB
);
"""

# ---------- helpers ----------
def _json_canonical(d: Any) -> str:
    """JSON determinístico para hashing/idempotência."""
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def _hash_event(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(_json_canonical(payload).encode("utf-8")).hexdigest()

def _extract_event_type(body: Dict[str, Any]) -> str:
    return (
        body.get("evento")
        or body.get("event")
        or body.get("event_type")
        or body.get("tipo")
        or body.get("Tipo")
        or "desconhecido"
    )

# ---------- lifecycle ----------
@app.on_event("startup")
async def startup() -> None:
    global _pool
    if not PG_DSN:
        print("[WARN] PG_DSN não definido. O app sobe, mas não gravará no banco.")
        return

    _pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
    async with _pool.acquire() as con:
        # Aplica as DDLs para todas as tabelas
        await con.execute(DDL_WEBHOOK_EVENTS)
        await con.execute(DDL_PEDIDO)
        await con.execute(DDL_NFE)
    print("[OK] Pool conectado e DDLs aplicadas.")

@app.on_event("shutdown")
async def shutdown() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

# ---------- health ----------
@app.get("/health")
async def health():
    if not _pool:
        return {"ok": True, "app": APP_NAME, "db": False}
    try:
        async with _pool.acquire() as con:
            await con.execute("SELECT 1;")
        return {"ok": True, "app": APP_NAME, "db": True}
    except Exception:
        return JSONResponse({"ok": False, "app": APP_NAME, "db": False}, status_code=500)

# ---------- webhook ----------
@app.post("/omie/webhook")
async def omie_webhook(
    request: Request,
    token: Optional[str] = Query(None, description="Token simples via querystring")
):
    # 1) valida token (se configurado)
    if WEBHOOK_TOKEN and token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")

    # 2) lê payload de forma resiliente
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {"_raw": body}
    except Exception:
        raw = (await request.body()).decode("utf-8", errors="ignore")
        body = {"_raw": raw}

    # 3) metadados básicos
    event_type = _extract_event_type(body)
    event_id = body.get("event_id") or body.get("id") or body.get("guid") or _hash_event(body)
    headers = dict(request.headers)

    # 4) se sem pool/banco, apenas ecoa (não falha para Omie)
    if not _pool:
        return {"ok": True, "warning": "PG_DSN ausente; evento não foi persistido", "event_type": event_type}

    # 5) insere no banco (lógica de roteamento e inserção)
    try:
        async with _pool.acquire() as con:
            # Insere o evento bruto na tabela de eventos para fins de log
            await con.execute(
                """
                INSERT INTO public.omie_webhook_events
                    (received_at, event_type, event_id, payload, raw_headers, http_status)
                VALUES
                    (now(), $1, $2, $3::jsonb, $4::jsonb, 200)
                """,
                event_type,
                event_id,
                json.dumps(body, ensure_ascii=False),
                json.dumps(headers, ensure_ascii=False),
            )

            # Lógica para inserir nas tabelas específicas
            if event_type == "nfe.emitida":
                nfe_data = body.get("event_data", {}).get("nfe", {})
                numero_nf = nfe_data.get("numero_nf")
                cnpj_destinatario = nfe_data.get("cnpj_destinatario")

                if numero_nf:
                    await con.execute(
                        """
                        INSERT INTO public.omie_nfe (numero, cnpj_destinatario, raw)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (numero) DO UPDATE SET raw = EXCLUDED.raw;
                        """,
                        numero_nf,
                        cnpj_destinatario,
                        json.dumps(nfe_data)
                    )

            if event_type == "pedido_venda.faturado":
                pedido_data = body.get("event_data", {}).get("pedidovenda", {})
                numero_pedido = pedido_data.get("numero_pedido_cliente")
                cnpj_destinatario = pedido_data.get("cnpj_cliente")

                if numero_pedido:
                    await con.execute(
                        """
                        INSERT INTO public.omie_pedido (numero_pedido, cnpj_destinatario, raw)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (numero_pedido) DO UPDATE SET raw = EXCLUDED.raw;
                        """,
                        numero_pedido,
                        cnpj_destinatario,
                        json.dumps(pedido_data)
                    )

        return {"ok": True, "event_type": event_type, "stored": True}
    except Exception as e:
        return JSONResponse(
            {"ok": False, "event_type": event_type, "stored": False, "error": str(e)[:300]},
            status_code=500,
        )
