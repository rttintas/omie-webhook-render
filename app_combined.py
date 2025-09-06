# app_combined.py — Pedidos + XML com reprocesso de ambos
import os
import json
import base64
from typing import Any, Dict, Optional, List

import asyncpg
import httpx
from fastapi import FastAPI, Request, Depends, HTTPException, Query, BackgroundTasks, Body
from fastapi.responses import JSONResponse

# =========================
# Config
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Faltou DATABASE_URL no ambiente")

TOKEN_PEDIDOS = os.getenv("OMIE_WEBHOOK_TOKEN", "um-segredo-forte")
TOKEN_XML     = os.getenv("OMIE_WEBHOOK_TOKEN_XML") or os.getenv("OMIE_XML_TOKEN", "tiago-nati")

ADMIN_SECRET  = os.getenv("ADMIN_SECRET") or os.getenv("ADMIN_JOB_SECRET", "julia-matheus")

OMIE_APP_KEY     = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET  = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT     = float(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

app = FastAPI(title="RTT Omie - Combined (Pedidos + XML)")

# =========================
# Conexão / Schema
# =========================
async def get_pool() -> asyncpg.pool.Pool:
    if not hasattr(app.state, "pool"):
        app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    return app.state.pool

async def ensure_schema(conn: asyncpg.Connection) -> None:
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_webhook_events (
        id           BIGSERIAL PRIMARY KEY,
        source       TEXT,
        event_type   TEXT,
        event_ts     TIMESTAMPTZ,
        event_id     TEXT,
        payload      JSONB,
        processed    BOOLEAN DEFAULT FALSE,
        processed_at TIMESTAMPTZ,
        received_at  TIMESTAMPTZ DEFAULT NOW(),
        raw_headers  JSONB,
        http_status  INTEGER,
        topic        TEXT,
        route        TEXT,
        status       TEXT
    );
    """)
    for col, typ in (("topic","TEXT"),("route","TEXT"),("status","TEXT")):
        await conn.execute(f'ALTER TABLE public.omie_webhook_events ADD COLUMN IF NOT EXISTS "{col}" {typ};')

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_pedido (
        id_pedido      BIGINT PRIMARY KEY,
        numero_pedido  TEXT,
        status         TEXT,
        recebido_em    TIMESTAMPTZ DEFAULT NOW(),
        raw_detalhe    JSONB
    );
    """)

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe (
        chave_nfe   TEXT PRIMARY KEY,
        numero      TEXT,
        serie       TEXT,
        status      TEXT,
        xml_text    TEXT,
        recebido_em TIMESTAMPTZ DEFAULT NOW()
    );
    """)

@app.on_event("startup")
async def on_startup():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await ensure_schema(conn)

# =========================
# Helpers
# =========================
def _pick(d: Dict[str, Any], *names: str, default=None):
    for n in names:
        v = d.get(n)
        if v not in (None, "", "NULL", "null"):
            return v
    return default

async def _insert_event(
    conn: asyncpg.Connection,
    *,
    source: str,
    event_type: str,
    route: str,
    payload: Dict[str, Any],
    headers: Dict[str, Any] | None,
    status_text: str,
    topic: str,
    event_ts: Optional[str] = None,
    event_id: Optional[str] = None,
) -> int:
    row_id = await conn.fetchval("""
        INSERT INTO public.omie_webhook_events
            (source, event_type, event_ts, event_id, payload, processed, received_at,
             raw_headers, http_status, topic, route, status)
        VALUES ($1,     $2,         $3,      $4,      $5,      FALSE,    NOW(),
                $6,          200,         $7,    $8,    $9)
        RETURNING id;
    """, source, event_type, event_ts, event_id,
         json.dumps(payload, ensure_ascii=False),
         json.loads(json.dumps(dict(headers or {}), ensure_ascii=False)),
         topic, route, status_text)
    return row_id

# =========================
# Omie - consulta de pedido
# =========================
async def omie_consultar_pedido(codigo_pedido: int) -> Dict[str, Any]:
    if not OMIE_APP_KEY or not OMIE_APP_SECRET:
        return {"ok": False, "motivo": "sem_credencial"}

    url = "https://app.omie.com.br/api/v1/produtos/pedido/"
    payload = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{"codigo_pedido": int(codigo_pedido)}],
    }
    async with httpx.AsyncClient(timeout=OMIE_TIMEOUT) as cli:
        r = await cli.post(url, json=payload)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "status_code": r.status_code, "erro": str(e), "body": r.text}
        data = r.json()
        if isinstance(data, dict) and "faultstring" in data:
            return {"ok": False, "status_code": 200, "erro": data.get("faultstring"), "body": data}
        return {"ok": True, "status_code": 200, "body": data}

async def _upsert_pedido(conn: asyncpg.Connection, *, id_pedido: int, numero: Optional[str], status: str, detalhe: Optional[Dict[str, Any]] = None):
    await conn.execute("""
        INSERT INTO public.omie_pedido (id_pedido, numero_pedido, status, recebido_em, raw_detalhe)
        VALUES ($1, $2, $3, NOW(), $4)
        ON CONFLICT (id_pedido) DO UPDATE
            SET numero_pedido = EXCLUDED.numero_pedido,
                status        = EXCLUDED.status,
                recebido_em   = EXCLUDED.recebido_em,
                raw_detalhe   = COALESCE(EXCLUDED.raw_detalhe, public.omie_pedido.raw_detalhe);
    """, int(id_pedido), numero, status, json.dumps(detalhe, ensure_ascii=False) if detalhe else None)

# =========================
# XML helpers
# =========================
async def _save_xml_payload(conn: asyncpg.Connection, payload: Dict[str, Any]):
    chave  = _pick(payload, "nfe_chave", "chave_nfe", "chave")
    numero = _pick(payload, "numero_nf", "numero")
    serie  = _pick(payload, "id_serie", "serie")

    xml = _pick(payload, "nfe_xml", "xml", "xml_text")
    if not xml and "xml_base64" in payload:
        try:
            xml = base64.b64decode(payload["xml_base64"]).decode("utf-8")
        except Exception:
            xml = None

    await conn.execute("""
        INSERT INTO public.omie_nfe (chave_nfe, numero, serie, status, xml_text, recebido_em)
        VALUES ($1,$2,$3,'recebido',$4, NOW())
        ON CONFLICT (chave_nfe) DO UPDATE
           SET numero      = COALESCE(EXCLUDED.numero, public.omie_nfe.numero),
               serie       = COALESCE(EXCLUDED.serie , public.omie_nfe.serie ),
               xml_text    = COALESCE(EXCLUDED.xml_text, public.omie_nfe.xml_text),
               status      = 'atualizado',
               recebido_em = NOW();
    """, chave, numero, serie, xml)

# =========================
# Endpoints básicos
# =========================
@app.get("/healthz")
async def healthz():
    return {"status": "healthy", "components": ["pedidos", "nfe_xml"], "compat": True}

@app.get("/")
async def root():
    return {"ok": True, "service": "omie-combined"}

# =========================
# Webhook PEDIDOS
# =========================
@app.get("/omie/webhook")
async def pedidos_ping(token: str = Query(...)):
    if token != TOKEN_PEDIDOS:
        raise HTTPException(status_code=404, detail="Not Found")
    return {"ok": True, "service": "omie_pedidos", "mode": "ping"}

@app.post("/omie/webhook")
async def pedidos_webhook(
    request: Request,
    token: str = Query(...),
    background: BackgroundTasks | None = None,
    pool: asyncpg.Pool = Depends(get_pool),
):
    if token != TOKEN_PEDIDOS:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        body = await request.json()
    except Exception:
        body = {}
    headers = request.headers

    async with pool.acquire() as conn:
        await _insert_event(
            conn,
            source="omie",
            event_type="pedido",
            route="/omie/webhook",
            payload=body,
            headers=headers,
            status_text="received",
            topic="omie_pedido",
        )
        idp    = _pick(body, "id_pedido", "codigo_pedido", "codigo")
        numero = _pick(body, "numero_pedido", "numero")
        if idp:
            await _upsert_pedido(conn, id_pedido=int(idp), numero=numero, status="pendente_consulta")

    if background:
        background.add_task(run_jobs_once)

    return {"ok": True, "received": True}

# =========================
# Webhook XML (NFe) – salva já
# =========================
@app.get("/xml/omie/webhook")
async def xml_ping(token: str = Query(...)):
    if token != TOKEN_XML:
        raise HTTPException(status_code=404, detail="Not Found")
    return {"ok": True, "service": "omie_xml", "mode": "ping"}

@app.post("/xml/omie/webhook")
async def xml_webhook(
    request: Request,
    token: str = Query(...),
    pool: asyncpg.Pool = Depends(get_pool),
):
    if token != TOKEN_XML:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        body = await request.json()
    except Exception:
        body = {}
    headers = request.headers

    async with pool.acquire() as conn:
        evt_id = await _insert_event(
            conn,
            source="omie",
            event_type="nfe_xml",
            route="/xml/omie/webhook",
            payload=body,
            headers=headers,
            status_text="recebido",
            topic="nfe",
        )
        await _save_xml_payload(conn, body)
        await conn.execute(
            "UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=NOW(), status='consumido' WHERE id=$1;",
            evt_id
        )

    return {"ok": True, "stored": True}

# =========================
# ADMIN – reprocessamento
# =========================
@app.api_route("/admin/run-jobs", methods=["GET","POST"])
async def admin_run_jobs(secret: str = Query(...)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=404, detail="Not Found")
    summary = await run_jobs_once()
    return {"ok": True, **summary}

# ---- Reprocesso de XML ----
@app.post("/admin/reprocessar-xml-pendentes")
async def admin_reprocessar_xml_pendentes(secret: str = Query(...), limit: int = Query(200)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=404, detail="Not Found")
    pool = await get_pool()
    ok = 0
    total = 0
    async with pool.acquire() as conn:
        eventos = await conn.fetch("""
            SELECT id, payload
              FROM public.omie_webhook_events
             WHERE processed IS NOT TRUE
               AND (event_type ILIKE 'nfe%%'
                    OR route LIKE '/xml%%'
                    OR payload ? 'nfe_xml')
             ORDER BY received_at ASC
             LIMIT $1;
        """, limit)
        total = len(eventos)
        for e in eventos:
            try:
                await _save_xml_payload(conn, e["payload"])
                await conn.execute(
                    "UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=NOW(), status='consumido' WHERE id=$1;",
                    e["id"]
                )
                ok += 1
            except Exception:
                await conn.execute(
                    "UPDATE public.omie_webhook_events SET status='erro', http_status=500 WHERE id=$1;",
                    e["id"]
                )
    return {"ok": True, "processados_ok": ok, "lidos": total}

@app.post("/admin/reprocessar-xml")
async def admin_reprocessar_xml(
    secret: str = Query(...),
    evento_id: Optional[int] = Query(None),
    chave: Optional[str] = Query(None)
):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=404, detail="Not Found")
    if not evento_id and not chave:
        raise HTTPException(status_code=400, detail="informe evento_id ou chave")

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = None
        if evento_id:
            row = await conn.fetchrow("SELECT id, payload FROM public.omie_webhook_events WHERE id=$1;", evento_id)
        else:
            row = await conn.fetchrow("""
                SELECT id, payload
                  FROM public.omie_webhook_events
                 WHERE (payload->>'nfe_chave' = $1
                        OR payload->>'chave_nfe' = $1
                        OR payload->>'chave' = $1)
                 ORDER BY received_at DESC
                 LIMIT 1;
            """, chave)

        if not row:
            return {"ok": False, "msg": "evento não encontrado"}

        await _save_xml_payload(conn, row["payload"])
        await conn.execute(
            "UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=NOW(), status='consumido' WHERE id=$1;",
            row["id"]
        )
        return {"ok": True, "event_id": row["id"]}

# =========================
# Processador de fila (PEDIDOS)
# =========================
async def run_jobs_once() -> Dict[str, Any]:
    pool = await get_pool()
    processed = 0
    errors = 0
    touched_ids: List[int] = []

    async with pool.acquire() as conn:
        rows: List[asyncpg.Record] = await conn.fetch("""
            SELECT id, event_type, payload
              FROM public.omie_webhook_events
             WHERE processed = FALSE
             ORDER BY received_at ASC
             LIMIT 100;
        """)
        for r in rows:
            evt_id = r["id"]
            etype  = r["event_type"] or ""
            payload = r["payload"] or {}
            try:
                if etype == "pedido":
                    idp    = _pick(payload, "id_pedido", "codigo_pedido", "codigo")
                    numero = _pick(payload, "numero_pedido", "numero")
                    status = "consultado"

                    detalhe = None
                    if idp:
                        resp = await omie_consultar_pedido(int(idp))
                        if not resp.get("ok"):
                            status = "erro_consulta"
                        else:
                            detalhe = resp.get("body")

                        await _upsert_pedido(conn, id_pedido=int(idp), numero=numero, status=status, detalhe=detalhe)

                await conn.execute(
                    "UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=NOW(), status='done' WHERE id=$1;",
                    evt_id
                )
                processed += 1
                touched_ids.append(evt_id)
            except Exception:
                errors += 1
                await conn.execute(
                    "UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=NOW(), status='error', http_status=500 WHERE id=$1;",
                    evt_id
                )

    return {"processed": processed, "errors": errors, "events": touched_ids}

# =========================
# Error handler
# =========================
@app.exception_handler(Exception)
async def all_errors_handler(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"detail": "internal_error"})
