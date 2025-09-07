# app_combined.py
# Serviço único FastAPI (webhooks + processamento)
# Rotas:
# - GET  /healthz
# - GET  /health
# - GET  /db-ping
# - POST /omie/webhook?token=...
# - POST /xml/omie/webhook?token=...
# - POST /admin/run-jobs?secret=...

import os
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# ENV
# ------------------------------------------------------------------------------
DATABASE_URL           = os.getenv("DATABASE_URL", "")
ADMIN_RUNJOBS_SECRET   = os.getenv("ADMIN_RUNJOBS_SECRET", "julia-matheus")

WEBHOOK_TOKEN_PED      = os.getenv("OMIE_WEBHOOK_TOKEN", "")
WEBHOOK_TOKEN_XML      = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "")

OMIE_APP_KEY           = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET        = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT_SECONDS   = float(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

OMIE_PEDIDO_URL        = os.getenv("OMIE_PEDIDO_URL", "https://app.omie.com.br/api/v1/produtos/pedido/")
OMIE_XML_URL           = os.getenv("OMIE_XML_URL",    "https://app.omie.com.br/api/v1/nfe/nfetool/")
OMIE_XML_LIST_CALL     = os.getenv("OMIE_XML_LIST_CALL", "ListarDocumentos")

NFE_LOOKBACK_DAYS      = int(os.getenv("NFE_LOOKBACK_DAYS",  "7"))
NFE_LOOKAHEAD_DAYS     = int(os.getenv("NFE_LOOKAHEAD_DAYS", "0"))

ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "true").lower() in ("1", "true", "yes", "y")

if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Postgres).")

# ------------------------------------------------------------------------------
# App + Pool
# ------------------------------------------------------------------------------
router = APIRouter()

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=60,
        max_inactive_connection_lifetime=300,
    )
    # sanity check e criação de tabelas (não altera tipos existentes)
    async with app.state.pool.acquire() as conn:
        await conn.execute("SELECT 1;")
        await _ensure_tables(conn)
    try:
        yield
    finally:
        await app.state.pool.close()

app = FastAPI(title="Omie Webhooks + Jobs (Serviço Único)", lifespan=lifespan)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _as_json(obj: Any) -> Dict[str, Any]:
    try:
        if isinstance(obj, (dict, list)):
            return obj if isinstance(obj, dict) else {"_list": obj}
        return json.loads(str(obj))
    except Exception:
        return {}

def _pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d.get(k)
    return None

def _safe_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _parse_dt_iso(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        s = str(v).strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _event_block(body: Dict[str, Any]) -> Dict[str, Any]:
    # Alguns webhooks vêm como {"payload": {...}}; outros já trazem o evento na raiz.
    if not isinstance(body, dict):
        return {}
    ev = body.get("event") or body.get("evento")
    if isinstance(ev, dict):
        return ev
    return body

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
        processed    boolean,
        processed_at timestamptz,
        error        text
    );
    CREATE INDEX IF NOT EXISTS idx_owe_received_at ON public.omie_webhook_events (received_at);
    CREATE INDEX IF NOT EXISTS idx_owe_processed   ON public.omie_webhook_events (processed);
    CREATE INDEX IF NOT EXISTS idx_owe_route       ON public.omie_webhook_events (route);
    CREATE INDEX IF NOT EXISTS idx_owe_topic       ON public.omie_webhook_events (topic);
    """)

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
    );
    """)

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
    );
    """)

# ------------------------------------------------------------------------------
# Health
# ------------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    return {"status": "healthy", "components": ["pedidos", "nfe_xml"], "compat": True}

@router.get("/health")
async def health():
    return {"ok": True, "ts": _now_utc().isoformat()}

@router.get("/db-ping")
async def db_ping():
    async with app.state.pool.acquire() as conn:
        v = await conn.fetchval("SELECT 1;")
    return {"db": int(v)}

# ------------------------------------------------------------------------------
# Webhooks
# ------------------------------------------------------------------------------
@router.post("/omie/webhook")
async def pedidos_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_PED:
        raise HTTPException(status_code=403, detail="invalid token")

    try:
        body = await request.json()
    except Exception:
        body = {}

    body_dict  = _as_json(body)
    topic      = _safe_text(_pick(body_dict, "topic", "TipoEvento", "evento")) or "omie_webhook_received"

    async with app.state.pool.acquire() as conn:
        # headers/payload em ::jsonb (json.dumps garante str)
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at)
            VALUES ('omie', $1, '/omie/webhook', 200, $2, NULL, $3::jsonb, $4::jsonb, now(), now());
        """,
        "omie_webhook_received",
        topic,
        json.dumps(dict(request.headers)),
        json.dumps(body_dict),
        )

    return JSONResponse({"ok": True})

@router.post("/xml/omie/webhook")
async def nfe_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=403, detail="invalid token")

    try:
        body = await request.json()
    except Exception:
        body = {}

    body_dict  = _as_json(body)
    topic      = _safe_text(_pick(body_dict, "topic", "TipoEvento", "evento")) or "nfe_xml_received"

    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at)
            VALUES ('omie', $1, '/xml/omie/webhook', 200, $2, NULL, $3::jsonb, $4::jsonb, now(), now());
        """,
        "nfe_xml_received",
        topic,
        json.dumps(dict(request.headers)),
        json.dumps(body_dict),
        )

    return JSONResponse({"ok": True})

# ------------------------------------------------------------------------------
# Extração/Bússola
# ------------------------------------------------------------------------------
def _extract_pedido_fields(ev: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    return (
        _safe_text(_pick(ev, "idPedido", "id_pedido", "id", "idVenda")),
        _safe_text(_pick(ev, "numeroPedido", "numero", "nPedido")),
    )

def _extract_nfe_fields(ev: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    chave     = _safe_text(_pick(ev, "nfe_chave", "chave_nfe", "chave"))
    numero    = _safe_text(_pick(ev, "numero_nf", "nNumero", "numero"))
    serie     = _safe_text(_pick(ev, "serie", "cSerie"))
    data_emis = _safe_text(_pick(ev, "data_emis", "dEmissao", "emitida_em"))
    xml_b64   = _safe_text(_pick(ev, "nfe_xml", "xml", "xml_base64"))

    nfe_blk = ev.get("nfe")
    if isinstance(nfe_blk, dict):
        xml_b64   = xml_b64   or _safe_text(_pick(nfe_blk, "nfe_xml", "xml"))
        chave     = chave     or _safe_text(_pick(nfe_blk, "nfe_chave", "chave"))
        numero    = numero    or _safe_text(_pick(nfe_blk, "numero_nf", "nNumero"))
        serie     = serie     or _safe_text(_pick(nfe_blk, "serie", "cSerie"))
        data_emis = data_emis or _safe_text(_pick(nfe_blk, "data_emis", "dEmissao"))
    return chave, numero, serie, data_emis, xml_b64

async def _fetch_xml_por_chave(client: httpx.AsyncClient, chave: str, data_emis: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not (OMIE_APP_KEY and OMIE_APP_SECRET and chave):
        return None, None
    dt   = _parse_dt_iso(data_emis) or _now_utc()
    dt_i = (dt - timedelta(days=NFE_LOOKBACK_DAYS)).date().isoformat()
    dt_f = (dt + timedelta(days=NFE_LOOKAHEAD_DAYS)).date().isoformat()

    payload = {
        "call": OMIE_XML_LIST_CALL,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "pagina": 1,
            "registros_por_pagina": 50,
            "apenas_importadas": "N",
            "filtrar_por_data": "S",
            "data_emissao_inicial": dt_i,
            "data_emissao_final": dt_f,
            "chave_nfe": chave
        }]
    }
    r = await client.post(OMIE_XML_URL, json=payload, timeout=OMIE_TIMEOUT_SECONDS)
    r.raise_for_status()
    data = r.json()
    try:
        docs = data.get("documentos", []) or data.get("lista", []) or []
        if docs:
            item = docs[0]
            xml_b64 = _safe_text(_pick(item, "xml", "xml_base64", "xmlNFe", "arquivo_xml"))
            return item, xml_b64
    except Exception:
        pass
    return data if isinstance(data, dict) else None, None

async def _fetch_pedido_por_id(client: httpx.AsyncClient, id_pedido: str) -> Optional[Dict[str, Any]]:
    if not (OMIE_APP_KEY and OMIE_APP_SECRET and id_pedido):
        return None
    payload = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{"id_pedido": int(str(id_pedido))}]
    }
    r = await client.post(OMIE_PEDIDO_URL, json=payload, timeout=OMIE_TIMEOUT_SECONDS)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else None

# ------------------------------------------------------------------------------
# Jobs
# ------------------------------------------------------------------------------
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")

    processed: int = 0
    errors: int    = 0
    log: List[Dict[str, Any]] = []

    async with app.state.pool.acquire() as conn, httpx.AsyncClient(timeout=OMIE_TIMEOUT_SECONDS) as client:
        rows = await conn.fetch("""
            SELECT id, route, payload
            FROM public.omie_webhook_events
            WHERE processed IS NOT TRUE
              AND (received_at AT TIME ZONE 'America/Sao_Paulo')::date = (now() AT TIME ZONE 'America/Sao_Paulo')::date
            ORDER BY id ASC
            LIMIT 500;
        """)

        for r in rows:
            ev_id   = r["id"]
            route   = r["route"] or ""
            payload = _as_json(r["payload"])
            ev      = _event_block(_as_json(payload.get("payload") or payload))

            try:
                # ------------------- NF-e -------------------
                if "/xml/omie/webhook" in route:
                    chave, numero, serie, data_emis, xml_b64 = _extract_nfe_fields(ev)

                    if not xml_b64 and chave:
                        try:
                            _doc, xml_b64 = await _fetch_xml_por_chave(client, chave, data_emis)
                        except Exception as ex:
                            log.append({"nfe_fetch_err": str(ex)})

                    if chave or xml_b64:
                        await conn.execute("""
                            INSERT INTO public.omie_nfe_xml
                                (chave_nfe, numero, serie, emitida_em, xml_base64, recebido_em, created_at, updated_at)
                            VALUES ($1, $2, $3, $4, $5, now(), now(), now())
                            ON CONFLICT (chave_nfe) DO UPDATE
                              SET numero     = COALESCE(EXCLUDED.numero,  public.omie_nfe_xml.numero),
                                  serie      = COALESCE(EXCLUDED.serie,   public.omie_nfe_xml.serie),
                                  emitida_em = COALESCE(EXCLUDED.emitida_em, public.omie_nfe_xml.emitida_em),
                                  xml_base64 = COALESCE(EXCLUDED.xml_base64, public.omie_nfe_xml.xml_base64),
                                  updated_at = now();
                        """,
                        chave,
                        numero,
                        serie,
                        _parse_dt_iso(data_emis) or None,
                        xml_b64
                        )

                    await conn.execute("""
                        UPDATE public.omie_webhook_events
                           SET processed = TRUE, processed_at = now(), error = NULL
                         WHERE id = $1;
                    """, ev_id)
                    processed += 1
                    log.append({"nfe_ok": chave or "[sem_chave]"})
                    continue

                # ------------------- Pedido -----------------
                if "/omie/webhook" in route:
                    id_pedido, num_ped = _extract_pedido_fields(ev)

                    detalhe = None
                    if ENRICH_PEDIDO_IMEDIATO and id_pedido:
                        try:
                            detalhe = await _fetch_pedido_por_id(client, id_pedido)
                        except Exception as ex:
                            log.append({"pedido_enrich_err": str(ex)})

                    valor_total = _safe_text(_pick(ev, "valorTotal", "valor_total"))
                    situacao    = _safe_text(_pick(ev, "situacao", "status"))
                    qtd_itens   = None
                    if isinstance(detalhe, dict):
                        try:
                            itens = detalhe.get("det", detalhe.get("detalhe", []))
                            if isinstance(itens, list):
                                qtd_itens = len(itens)
                        except Exception:
                            qtd_itens = None

                    # >>> PATCH AQUI: detalhe serializado + ::jsonb
                    await conn.execute("""
                        INSERT INTO public.omie_pedido
                            (id_pedido_omie, numero, valor_total, situacao, quantidade_itens, cliente_codigo, detalhe, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now(), now())
                        ON CONFLICT (id_pedido_omie) DO UPDATE
                          SET numero           = EXCLUDED.numero,
                              valor_total      = COALESCE(EXCLUDED.valor_total, public.omie_pedido.valor_total),
                              situacao         = COALESCE(EXCLUDED.situacao, public.omie_pedido.situacao),
                              quantidade_itens = COALESCE(EXCLUDED.quantidade_itens, public.omie_pedido.quantidade_itens),
                              cliente_codigo   = COALESCE(EXCLUDED.cliente_codigo, public.omie_pedido.cliente_codigo),
                              detalhe          = COALESCE(EXCLUDED.detalhe, public.omie_pedido.detalhe),
                              updated_at       = now();
                    """,
                    int(str(id_pedido)) if id_pedido else None,
                    num_ped,
                    valor_total,
                    situacao,
                    qtd_itens,
                    _safe_text(_pick(ev, "idCliente", "codigo_cliente")),
                    json.dumps(detalhe if isinstance(detalhe, dict) else _as_json(ev))
                    )

                    await conn.execute("""
                        UPDATE public.omie_webhook_events
                           SET processed = TRUE, processed_at = now(), error = NULL
                         WHERE id = $1;
                    """, ev_id)
                    processed += 1
                    log.append({"pedido_ok": id_pedido or num_ped or "[sem_id]"})
                    continue

                # rota não reconhecida
                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, processed_at = now(), error = $1
                     WHERE id = $2;
                """, "rota_desconhecida", ev_id)
                processed += 1
                log.append({"ignorado": route})

            except Exception as ex:
                errors += 1
                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, processed_at = now(), error = $1
                     WHERE id = $2;
                """, f"erro:{ex}", ev_id)
                if "/xml/omie/webhook" in route:
                    log.append({"nfe_err": str(ex)})
                else:
                    log.append({"pedido_err": str(ex)})

    return JSONResponse({"ok": True, "processed": processed, "errors": errors, "events": log})

# ------------------------------------------------------------------------------
# Mount
# ------------------------------------------------------------------------------
app.include_router(router)

# ------------------------------------------------------------------------------
# Execução local
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app_combined:app", host="0.0.0.0", port=port, reload=True)
