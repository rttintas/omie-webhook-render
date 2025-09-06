# app_combined.py — Pedidos + XML com reprocesso de ambos

import os
import json
from typing import Any, Dict, Optional, List

import asyncpg
import httpx
from fastapi import FastAPI, Request, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse

# =========================================
# Config
# =========================================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Faltou DATABASE_URL no ambiente")

# Credenciais Omie
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT = float(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

# Tokens de webhook
TOKEN_PEDIDOS = os.getenv("OMIE_WEBHOOK_TOKEN", "um-segredo-forte")
TOKEN_XML = os.getenv("OMIE_WEBHOOK_TOKEN_XML") or os.getenv("OMIE_XML_TOKEN", "tiago-nati")

# Segredo admin (para rotas /admin)
ADMIN_SECRET = os.getenv("ADMIN_SECRET") or os.getenv("ADMIN_JOB_SECRET", "julia-matheus")

# Onde salvar XML (usaremos omie_nfe_xml)
XML_WEBHOOK_SAVE = os.getenv("XML_WEBHOOK_SAVE", "omie_nfe_xml")

app = FastAPI(title="RTT Omie - Combined (Pedidos + XML)")

# =========================================
# Startup/Shutdown e Pool
# =========================================
@app.on_event("startup")
async def startup() -> None:
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with app.state.pool.acquire() as conn:
        await ensure_schema(conn)

@app.on_event("shutdown")
async def shutdown() -> None:
    pool = getattr(app.state, "pool", None)
    if pool:
        await pool.close()

async def get_pool() -> asyncpg.pool.Pool:
    return app.state.pool

# =========================================
# Schema
# =========================================
async def ensure_schema(conn: asyncpg.Connection) -> None:
    # Auditoria/fila de eventos
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.omie_webhook_events (
            id            BIGSERIAL PRIMARY KEY,
            source        TEXT,
            event_type    TEXT,
            event_ts      TIMESTAMPTZ DEFAULT NOW(),
            event_id      TEXT,
            payload       JSONB,
            processed     BOOLEAN DEFAULT FALSE,
            processed_at  TIMESTAMPTZ,
            received_at   TIMESTAMPTZ DEFAULT NOW(),
            raw_headers   JSONB,
            http_status   INTEGER,
            topic         TEXT,
            route         TEXT,
            status        TEXT
        );
        """
    )
    # Garante colunas novas se a tabela já existia
    for col, typ in (("topic", "TEXT"), ("route", "TEXT"), ("status", "TEXT")):
        await conn.execute(f'ALTER TABLE public.omie_webhook_events ADD COLUMN IF NOT EXISTS "{col}" {typ};')

    # Pedidos
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.omie_pedido (
            id_pedido      BIGINT PRIMARY KEY,
            numero_pedido  TEXT,
            status         TEXT,
            recebido_em    TIMESTAMPTZ DEFAULT NOW(),
            detalhe        JSONB
        );
        """
    )

    # XML de NFe (guardamos base64 + payload)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.omie_nfe_xml (
            chave_nfe    TEXT PRIMARY KEY,
            xml_base64   TEXT,
            recebido_em  TIMESTAMPTZ DEFAULT NOW(),
            payload      JSONB
        );
        """
    )

# =========================================
# Helpers
# =========================================
def _pick(d: Optional[Dict[str, Any]], *candidatos: str) -> Optional[Any]:
    if not d:
        return None
    for k in candidatos:
        if k in d and d[k] not in (None, "", "NULL", "null"):
            return d[k]
    return None

async def _insert_event(
    conn: asyncpg.Connection,
    *,
    source: str,
    event_type: str,
    event_id: Optional[str],
    payload: Optional[Dict[str, Any]],
    headers: Optional[Dict[str, Any]],
    http_status: Optional[int],
    topic: Optional[str],
    route: Optional[str],
    status_text: Optional[str],
) -> int:
    """
    Insere o evento; a ordem dos placeholders ($1..$9) bate com as colunas.
    """
    # CORREÇÃO: Converta headers para string JSON antes de inserir
    headers_json = json.dumps(headers) if headers else None
    
    row_id = await conn.fetchval(
        """
        INSERT INTO public.omie_webhook_events
            (source, event_type, event_ts, event_id, payload,
             processed, processed_at, received_at,
             raw_headers, http_status, topic, route, status)
        VALUES ($1, $2, NOW(), $3, $4,
                FALSE, NULL, NOW(),
                $5, $6, $7, $8, $9)
        RETURNING id;
        """,
        source,
        event_type,
        str(event_id) if event_id is not None else None,
        payload,                              # JSONB aceita dict diretamente
        headers_json,                         # CORREÇÃO: raw_headers como string JSON
        int(http_status) if http_status is not None else None,
        topic,
        route,
        status_text,
    )
    return int(row_id)

async def _upsert_pedido(
    conn: asyncpg.Connection,
    *,
    id_pedido: int,
    numero: Optional[str],
    status: Optional[str],
    detalhe: Optional[Dict[str, Any]] = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO public.omie_pedido (id_pedido, numero_pedido, status, recebido_em, detalhe)
        VALUES ($1, $2, $3, NOW(), $4)
        ON CONFLICT (id_pedido)
        DO UPDATE SET
            numero_pedido = EXCLUDED.numero_pedido,
            status        = EXCLUDED.status,
            detalhe       = EXCLUDED.detalhe;
        """,
        int(id_pedido),
        numero,
        status,
        detalhe,
    )

async def _save_xml_payload(conn: asyncpg.Connection, payload: Dict[str, Any]) -> None:
    """
    Salva XML de NFe (base64) + payload no destino definido por XML_WEBHOOK_SAVE.
    """
    chave = _pick(payload, "nfe_chave", "chave_nfe", "chave")
    xml_b64 = _pick(payload, "nfe_xml", "xml", "xml_base64")

    # Normaliza tipos
    chave = str(chave) if chave is not None else None
    xml_b64 = str(xml_b64) if xml_b64 is not None else None

    if not chave:
        raise ValueError("payload de XML sem chave_nfe")

    await conn.execute(
        """
        INSERT INTO public.omie_nfe_xml (chave_nfe, xml_base64, recebido_em, payload)
        VALUES ($1, $2, NOW(), $3)
        ON CONFLICT (chave_nfe)
        DO UPDATE SET
            xml_base64 = EXCLUDED.xml_base64,
            payload    = EXCLUDED.payload;
        """,
        chave,
        xml_b64,
        payload,
    )

# =========================================
# Omie – ConsultarPedido
# =========================================
async def omie_consultar_pedido(codigo_pedido: int) -> Dict[str, Any]:
    """Chama ConsultarPedido na Omie e retorna dict com ok/erro sem levantar exceção."""
    if not OMIE_APP_KEY or not OMIE_APP_SECRET:
        return {"ok": False, "motivo": "sem_credencial"}

    url = "https://app.omie.com.br/api/v1/produtos/pedido/"
    body = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{"codigo_pedido": int(codigo_pedido)}],
    }

    try:
        async with httpx.AsyncClient(timeout=OMIE_TIMEOUT) as client:
            r = await client.post(url, json=body)
            status = r.status_code
            try:
                data = r.json()
            except Exception:
                data = {"raw": r.text}

            if status >= 400:
                return {"ok": False, "status": status, "resp": data}
            if isinstance(data, dict) and any(k in data for k in ("faultstring", "faultcode", "fault")):
                return {"ok": False, "status": status, "resp": data}
            return {"ok": True, "status": status, "resp": data}
    except Exception as e:
        return {"ok": False, "exc": str(e)}

# =========================================
# Health
# =========================================
@app.get("/healthz")
async def healthz():
    return {"status": "healthy", "components": ["pedidos", "nfe_xml"], "compat": True}

@app.get("/")
async def root():
    return {"ok": True, "service": "omie-combined"}

# =========================================
# Webhook – Pedidos
# =========================================
@app.api_route("/omie/webhook", methods=["GET", "POST"])
async def pedidos_webhook(
    request: Request,
    token: str = Query(...),
    background_tasks: BackgroundTasks = None,  # FastAPI injeta automaticamente
    pool: asyncpg.Pool = Depends(get_pool),
):
    if token != TOKEN_PEDIDOS:
        raise HTTPException(status_code=404, detail="Not Found")

    # Ping
    if request.method == "GET":
        return {"ok": True, "service": "omie_pedidos", "mode": "ping"}

    # POST
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}
    headers = dict(request.headers)

    async with pool.acquire() as conn:
        evt_id = await _insert_event(
            conn,
            source="omie",
            event_type="omie_webhook_received",
            event_id=str(_pick(body, "id_pedido", "codigo_pedido", "codigo") or ""),
            payload=body,
            headers=headers,
            http_status=200,
            topic="omie_pedido",
            route="/omie/webhook",
            status_text="received",
        )

        # Upsert imediato como "pendente_consulta"
        idp = _pick(body, "id_pedido", "codigo_pedido", "codigo")
        numero = _pick(body, "numero_pedido", "numero")
        if idp:
            await _upsert_pedido(
                conn,
                id_pedido=int(idp),
                numero=str(numero) if numero else None,
                status="pendente_consulta",
                detalhe=None,
            )

    # Processa a fila em background
    if background_tasks:
        background_tasks.add_task(run_jobs_once)
    return {"ok": True, "event_id": evt_id}

# =========================================
# Webhook – XML de NFe
# =========================================
@app.api_route("/xml/omie/webhook", methods=["GET", "POST"])
async def xml_webhook(
    request: Request,
    token: str = Query(...),
    pool: asyncpg.Pool = Depends(get_pool),
):
    if token != TOKEN_XML:
        raise HTTPException(status_code=404, detail="Not Found")

    if request.method == "GET":
        return {"ok": True, "service": "omie_xml", "mode": "ping"}

    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}
    headers = dict(request.headers)

    async with pool.acquire() as conn:
        evt_id = await _insert_event(
            conn,
            source="omie",
            event_type="nfe_xml_received",
            event_id=str(_pick(body, "nfe_chave", "chave_nfe", "chave") or ""),
            payload=body,
            headers=headers,
            http_status=200,
            topic="nfe",
            route="/xml/omie/webhook",
            status_text="recebido",
        )

        await _save_xml_payload(conn, body)

        # marca evento como processado
        await conn.execute(
            "UPDATE public.omie_webhook_events "
            "SET processed=TRUE, processed_at=NOW(), status='consumido' "
            "WHERE id=$1;",
            evt_id,
        )

    return {"ok": True, "stored": True}

# =========================================
# ADMIN – reprocessos
# =========================================
@app.api_route("/admin/run-jobs", methods=["GET", "POST"])
async def admin_run_jobs(
    secret: str = Query(...),
    pool: asyncpg.Pool = Depends(get_pool)  # CORREÇÃO: Adicionado pool como dependência
):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=404, detail="Not Found")
    summary = await run_jobs_once()
    return {"ok": True, **summary}

# ---- Reprocesso de XML (em lote pendente)
@app.post("/admin/reprocessar-xml-pendentes")
async def admin_reprocessar_xml_pendentes(
    secret: str = Query(...),
    pool: asyncpg.Pool = Depends(get_pool),
    limit: int = Query(200)
):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=404, detail="Not Found")

    ok = 0
    total = 0
    async with pool.acquire() as conn:
        eventos = await conn.fetch(
            """
            SELECT id, payload
              FROM public.omie_webhook_events
             WHERE processed IS NOT TRUE
               AND (event_type ILIKE 'nfe%%'
                    OR route LIKE '/xml%%'
                    OR payload ? 'nfe_xml'
                    OR topic = 'nfe')
             ORDER BY received_at ASC
             LIMIT $1;
            """,
            limit,
        )
        total = len(eventos)
        for e in eventos:
            try:
                await _save_xml_payload(conn, e["payload"])
                await conn.execute(
                    "UPDATE public.omie_webhook_events "
                    "SET processed=TRUE, processed_at=NOW(), status='consumido' "
                    "WHERE id=$1;",
                    e["id"],
                )
                ok += 1
            except Exception:
                await conn.execute(
                    "UPDATE public.omie_webhook_events "
                    "SET status='erro', http_status=500 "
                    "WHERE id=$1;",
                    e["id"],
                )

    return {"ok": True, "processados_ok": ok, "lidos": total}

# ---- Reprocesso de XML (por evento_id ou chave)
@app.post("/admin/reprocessar-xml")
async def admin_reprocessar_xml(
    secret: str = Query(...),
    evento_id: Optional[int] = Query(None),
    chave: Optional[str] = Query(None),
    pool: asyncpg.Pool = Depends(get_pool),
):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=404, detail="Not Found")
    if not evento_id and not chave:
        raise HTTPException(status_code=400, detail="informe evento_id ou chave")

    async with pool.acquire() as conn:
        row = None
        if evento_id:
            row = await conn.fetchrow(
                "SELECT id, payload FROM public.omie_webhook_events WHERE id=$1;",
                evento_id,
            )
        else:
            row = await conn.fetchrow(
                """
                SELECT id, payload
                  FROM public.omie_webhook_events
                 WHERE (payload->>'nfe_chave' = $1
                        OR payload->>'chave_nfe' = $1
                        OR payload->>'chave' = $1)
                 ORDER BY received_at DESC
                 LIMIT 1;
                """,
                chave,
            )

        if not row:
            return {"ok": False, "msg": "evento não encontrado"}

        await _save_xml_payload(conn, row["payload"])
        await conn.execute(
            "UPDATE public.omie_webhook_events "
            "SET processed=TRUE, processed_at=NOW(), status='consumido' "
            "WHERE id=$1;",
            row["id"],
        )
        return {"ok": True, "event_id": row["id"]}

# =========================================
# Processador da fila (PEDIDOS)
# =========================================
async def run_jobs_once() -> Dict[str, Any]:
    """
    Processa eventos pendentes de PEDIDOS:
      - lê omie_webhook_events
      - consulta pedido na Omie
      - atualiza omie_pedido
    """
    pool = await get_pool()
    processed = 0
    errors = 0
    touched_ids: List[int] = []

    async with pool.acquire() as conn:
        eventos = await conn.fetch(
            """
            SELECT id, payload
              FROM public.omie_webhook_events
             WHERE processed IS NOT TRUE
               AND (topic = 'omie_pedido' OR route LIKE '/omie%%')
             ORDER BY received_at ASC
             LIMIT 200;
            """
        )

        for row in eventos:
            evt_id: int = row["id"]
            payload: Dict[str, Any] = row["payload"] or {}
            try:
                idp = _pick(payload, "id_pedido", "codigo_pedido", "codigo")
                numero = _pick(payload, "numero_pedido", "numero")
                if idp:
                    resp = await omie_consultar_pedido(int(idp))
                    status = "consultado" if resp.get("ok") else "sem_id"
                    detalhe = resp.get("resp") if resp.get("ok") else {"erro": resp}
                    await _upsert_pedido(
                        conn,
                        id_pedido=int(idp),
                        numero=str(numero) if numero else None,
                        status=status,
                        detalhe=detalhe,
                    )
                await conn.execute(
                    "UPDATE public.omie_webhook_events "
                    "SET processed=TRUE, processed_at=NOW(), status='done' "
                    "WHERE id=$1;",
                    evt_id,
                )
                processed += 1
                touched_ids.append(evt_id)
            except Exception:
                errors += 1
                await conn.execute(
                    "UPDATE public.omie_webhook_events "
                    "SET processed=TRUE, processed_at=NOW(), status='error', http_status=500 "
                    "WHERE id=$1;",
                    evt_id,
                )

    return {"processed": processed, "errors": errors, "events": touched_ids}

# =========================================
# Error handler
# =========================================
@app.exception_handler(Exception)
async def all_errors_handler(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"detail": "internal_error"})