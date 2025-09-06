import os
import json
from typing import Any, Dict, Optional, List

import asyncpg
import httpx
from fastapi import FastAPI, Request, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse

# -----------------------------
# Configurações
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")  # exemplo: postgresql://user:pass@host/db
if not DATABASE_URL:
    raise RuntimeError("Faltou DATABASE_URL no ambiente")

# tokens dos webhooks (use os mesmos que você registrou no Omie)
TOKEN_PEDIDOS = os.getenv("TOKEN_PEDIDOS", "um-segredo-forte")
TOKEN_XML = os.getenv("TOKEN_XML", "tiago-nati")

# segredo do job manual (mantive 'julia-matheus' p/ compat)
ADMIN_JOB_SECRET = os.getenv("ADMIN_JOB_SECRET", "julia-matheus")

# credenciais Omie (para consultar detalhes do pedido)
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")

# -----------------------------
# App
# -----------------------------
app = FastAPI(title="omie-webhook-render")

# -----------------------------
# Conexão / Schema
# -----------------------------
async def get_pool():
    if not hasattr(app.state, "pool"):
        app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    return app.state.pool

async def ensure_schema(conn: asyncpg.Connection) -> None:
    # Tabela de fila de eventos do webhook
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
    # Garante colunas novas (caso a tabela já existisse sem elas)
    for col, typ in (("topic", "TEXT"), ("route", "TEXT"), ("status", "TEXT")):
        try:
            await conn.execute(f'ALTER TABLE public.omie_webhook_events ADD COLUMN IF NOT EXISTS "{col}" {typ};')
        except Exception:
            pass

    # Tabela de pedidos (id único)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_pedido (
        id_pedido      BIGINT PRIMARY KEY,
        numero_pedido  TEXT,
        status         TEXT,
        recebido_em    TIMESTAMPTZ DEFAULT NOW()
    );
    """)

    # (Opcional) cache simples do XML da NFe – não mexe nas suas tabelas antigas
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.nf_xml_cache (
        chave_nfe   TEXT PRIMARY KEY,
        xml_text    TEXT,
        recebido_em TIMESTAMPTZ DEFAULT NOW()
    );
    """)

@app.on_event("startup")
async def on_startup():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await ensure_schema(conn)

# -----------------------------
# Utilidades
# -----------------------------
def _to_json(o: Any) -> str:
    return json.dumps(o, ensure_ascii=False)

async def _insert_event(
    conn: asyncpg.Connection,
    *,
    source: str,
    event_type: str,
    route: str,
    token: str,
    payload: Dict[str, Any],
    headers: Dict[str, Any],
    status_text: str = "received",
    event_ts: Optional[str] = None,
    event_id: Optional[str] = None,
    topic: Optional[str] = None,
) -> int:
    row_id = await conn.fetchval(
        """
        INSERT INTO public.omie_webhook_events
            (source, event_type, event_ts, event_id, payload, processed, received_at,
             raw_headers, http_status, topic, route, status)
        VALUES ($1,     $2,         $3,      $4,      $5,      FALSE,    NOW(),
                $6,          200,         $7,    $8,    $9)
        RETURNING id
        """,
        source, event_type, event_ts, event_id, json.dumps(payload, ensure_ascii=False),
        json.loads(json.dumps(dict(headers), ensure_ascii=False)),
        topic or event_type, route, status_text,
    )
    return row_id

async def omie_consultar_pedido(codigo_pedido: int) -> Dict[str, Any]:
    """
    Chama o endpoint de ConsultarPedido do Omie.
    """
    if not OMIE_APP_KEY or not OMIE_APP_SECRET:
        # Sem credencial, apenas retorna "sem consulta"
        return {"ok": False, "motivo": "sem_credencial"}

    url = "https://app.omie.com.br/api/v1/produtos/pedido/"
    payload = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{"codigo_pedido": int(codigo_pedido)}],
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(url, json=payload)
        # Não levantar erro 4xx/5xx – a gente marca como falha mas não quebra o app
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "status_code": r.status_code, "erro": str(e), "body": r.text}

        data = r.json()
        # Se o Omie devolve fault / erro no corpo
        if isinstance(data, dict) and "faultstring" in data:
            return {"ok": False, "status_code": 200, "erro": data.get("faultstring"), "body": data}
        return {"ok": True, "status_code": 200, "body": data}

async def _upsert_pedido(conn: asyncpg.Connection, *, id_pedido: int, numero: Optional[str], status: str) -> None:
    await conn.execute(
        """
        INSERT INTO public.omie_pedido (id_pedido, numero_pedido, status, recebido_em)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (id_pedido) DO UPDATE
            SET numero_pedido = EXCLUDED.numero_pedido,
                status        = EXCLUDED.status,
                recebido_em   = EXCLUDED.recebido_em;
        """,
        int(id_pedido), numero, status
    )

# -----------------------------
# Endpoints
# -----------------------------
@app.get("/healthz")
async def healthz():
    return {"status": "healthy", "components": ["pedidos", "nfe_xml"], "compat": True}

@app.get("/")
async def root():
    return {"ok": True, "service": "omie-webhook-render"}

# --------- Pedidos ----------
@app.get("/omie/webhook")
async def omie_webhook_ping(token: str = Query(...)):
    if token != TOKEN_PEDIDOS:
        raise HTTPException(status_code=404, detail="Not Found")
    return {"ok": True, "service": "omie_pedidos", "mode": "ping"}

@app.post("/omie/webhook")
async def omie_webhook_pedidos(
    request: Request,
    token: str = Query(...),
    background: BackgroundTasks = None,
    pool: asyncpg.Pool = Depends(get_pool),
):
    if token != TOKEN_PEDIDOS:
        raise HTTPException(status_code=404, detail="Not Found")

    body = await request.json()
    headers = request.headers

    async with pool.acquire() as conn:
        await _insert_event(
            conn,
            source="omie",
            event_type="pedido",
            route="/omie/webhook",
            token=token,
            payload=body,
            headers=headers,
            status_text="received",
            topic="omie_pedido",
        )

    # Processa já (sem depender de cron) – ignora falhas silenciosamente
    if background:
        background.add_task(run_jobs_once)

    # Extrai alguns campos só para retornar
    numero = body.get("numero_pedido") or body.get("numero")
    id_pedido = body.get("id_pedido") or body.get("codigo_pedido") or body.get("codigo")
    return {"ok": True, "received": True, "numero_pedido": numero, "id_pedido": id_pedido}

# --------- XML NFe ----------
@app.get("/xml/omie/webhook")
async def xml_webhook_ping(token: str = Query(...)):
    if token != TOKEN_XML:
        raise HTTPException(status_code=404, detail="Not Found")
    return {"ok": True, "service": "omie_xml", "mode": "ping"}

@app.post("/xml/omie/webhook")
async def omie_webhook_xml(
    request: Request,
    token: str = Query(...),
    background: BackgroundTasks = None,
    pool: asyncpg.Pool = Depends(get_pool),
):
    if token != TOKEN_XML:
        raise HTTPException(status_code=404, detail="Not Found")

    body = await request.json()
    headers = request.headers

    async with pool.acquire() as conn:
        # salva na fila de eventos
        await _insert_event(
            conn,
            source="omie",
            event_type="nfe_xml",
            route="/xml/omie/webhook",
            token=token,
            payload=body,
            headers=headers,
            status_text="received",
            topic="omie_nfe",
        )
        # guarda o XML numa tabela de cache simples (não quebra nada seu)
        chave = body.get("nfe_chave") or body.get("chave_nfe")
        xml_text = body.get("nfe_xml") or body.get("xml")
        if chave and xml_text:
            await conn.execute(
                """
                INSERT INTO public.nf_xml_cache (chave_nfe, xml_text, recebido_em)
                VALUES ($1, $2, NOW())
                ON CONFLICT (chave_nfe) DO UPDATE
                   SET xml_text = EXCLUDED.xml_text,
                       recebido_em = EXCLUDED.recebido_em;
                """,
                chave, xml_text
            )

    if background:
        background.add_task(run_jobs_once)

    return {"ok": True, "received": True, "chave": body.get("nfe_chave")}

# --------- Job manual (kicker) ----------
@app.post("/admin/run-jobs")
async def admin_run_jobs(secret: str = Query(...)):
    if secret != ADMIN_JOB_SECRET:
        raise HTTPException(status_code=404, detail="Not Found")

    await run_jobs_once()
    return {"ok": True, "ran": True}

# -----------------------------
# Processamento de eventos pendentes
# -----------------------------
async def run_jobs_once():
    """
    Lê eventos NÃO processados e:
      - para pedidos: consulta o Omie (se tiver credencial) e dá UPSERT em omie_pedido
      - marca processed = true
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        pendentes: List[asyncpg.Record] = await conn.fetch(
            """
            SELECT id, event_type, payload
              FROM public.omie_webhook_events
             WHERE processed = FALSE
             ORDER BY received_at ASC
             LIMIT 100;
            """
        )

        for rec in pendentes:
            try:
                etype = rec["event_type"]
                payload = rec["payload"] or {}
                if etype == "pedido":
                    # tenta achar ID do pedido
                    idp = payload.get("id_pedido") or payload.get("codigo_pedido") or payload.get("codigo")
                    numero = payload.get("numero_pedido") or payload.get("numero")
                    status = "consultado"

                    if idp:
                        det = await omie_consultar_pedido(int(idp))
                        if not det.get("ok"):
                            # se falhou, ainda assim grava o evento no pedido com status do erro
                            status = "erro_consulta"
                        # upsert do pedido
                        await _upsert_pedido(conn, id_pedido=int(idp), numero=numero, status=status)

                # marca como processado
                await conn.execute(
                    "UPDATE public.omie_webhook_events SET processed = TRUE, processed_at = NOW(), status = 'done' WHERE id = $1",
                    rec["id"]
                )
            except Exception:
                # marca como processado com erro (para não travar fila)
                await conn.execute(
                    "UPDATE public.omie_webhook_events SET processed = TRUE, processed_at = NOW(), status = 'error' WHERE id = $1",
                    rec["id"]
                )

# -----------------------------
# Error handler (torna 500 mais limpo p/ Omie)
# -----------------------------
@app.exception_handler(Exception)
async def all_errors_handler(request: Request, exc: Exception):
    # Não vaza stack pro Omie; registra simples
    return JSONResponse(status_code=500, content={"detail": "internal_error"})
