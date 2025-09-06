# app_combined.py — Omie Webhooks + Reprocesso (Pedidos + NFe XML)

import os
import json
import base64
from datetime import datetime
import re
from typing import Any, Dict

import aiohttp
import asyncpg
from fastapi import FastAPI, APIRouter, Request, HTTPException, Query
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
DATABASE_URL     = os.getenv("DATABASE_URL") or os.getenv("DB_URL") or ""
ADMIN_SECRET     = os.getenv("ADMIN_SECRET", "julia-matheus")
OMIE_APP_KEY     = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET  = os.getenv("OMIE_APP_SECRET") or os.getenv("OMIE_APP_HASH") or ""
OMIE_BASE        = "https://app.omie.com.br/api/v1"
TZ               = os.getenv("TIMEZONE", "America/Sao_Paulo")

TOKEN_PEDIDOS    = os.getenv("OMIE_WEBHOOK_TOKEN", "um-segredo-forte")
TOKEN_XML        = os.getenv("OMIE_WEBHOOK_TOKEN_XML") or os.getenv("OMIE_XML_TOKEN", "tiago-nati")

if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Postgres).")

# ------------------------------------------------------------------------------
# App
# ------------------------------------------------------------------------------
app    = FastAPI(title="Omie Webhooks + Jobs")
router = APIRouter()

# -- asyncpg pool com codecs JSON/JSONB
async def _setup_json_codecs(conn: asyncpg.Connection):
    await conn.set_type_codec("json",  encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=1, max_size=5, init=_setup_json_codecs
    )
    async with app.state.pool.acquire() as conn:
        await _run_migrations(conn)

@app.on_event("shutdown")
async def shutdown():
    pool = getattr(app.state, "pool", None)
    if pool:
        await pool.close()

# ------------------------------------------------------------------------------
# Migrações
# ------------------------------------------------------------------------------
async def _run_migrations(conn: asyncpg.Connection):
    # Eventos
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_webhook_events (
        id           bigserial PRIMARY KEY,
        source       text NOT NULL DEFAULT 'omie',
        event_type   text NOT NULL,
        event_ts     timestamptz NOT NULL DEFAULT now(),
        event_id     text,
        route        text,
        processed    boolean NOT NULL DEFAULT FALSE,
        status       text,
        received_at  timestamptz NOT NULL DEFAULT now(),
        payload      jsonb
    );
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_owe_event_ts  ON public.omie_webhook_events(event_ts);")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_owe_route     ON public.omie_webhook_events(route);")
    await conn.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'omie_webhook_events_event_id_uniq') THEN
            ALTER TABLE public.omie_webhook_events
              ADD CONSTRAINT omie_webhook_events_event_id_uniq UNIQUE (event_id);
        END IF;
    END$$;
    """)

    # Pedidos
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_pedido (
        id                bigserial PRIMARY KEY,
        id_pedido_omie    bigint UNIQUE,
        numero            text,
        valor_total       numeric(15,2),
        situacao          text,
        quantidade_itens  integer,
        cliente_codigo    text,
        detalhe           jsonb,
        created_at        timestamptz DEFAULT now(),
        updated_at        timestamptz,
        recebido_em       timestamptz DEFAULT now()
    );
    """)
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS detalhe jsonb;")

    # NF-e XML
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe_xml (
        id           bigserial PRIMARY KEY,
        chave_nfe    text UNIQUE,
        numero       text,
        serie        text,
        emitida_em   timestamptz,
        recebido_em  timestamptz DEFAULT now(),
        xml_base64   text,
        created_at   timestamptz DEFAULT now(),
        updated_at   timestamptz
    );
    """)
    await conn.execute("ALTER TABLE public.omie_nfe_xml ADD COLUMN IF NOT EXISTS xml_base64 text;")

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _as_dict(x: Any) -> Dict[str, Any]:
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return {}
    return {}

def _event_block(body: dict) -> dict:
    if not isinstance(body, dict):
        return {}
    e = body.get("event")
    if isinstance(e, dict):
        return e
    e = body.get("evento")
    if isinstance(e, dict):
        return e
    return body

def _pick(d: dict, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, "", "NULL", "null"):
            return d[k]
    return default

def _yyyymmdd_from_iso(dt_iso: str) -> str:
    try:
        dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return ""

def _parse_emitida_em(dt_iso: str):
    try:
        return datetime.fromisoformat(dt_iso.replace("Z", "+00:00"))
    except Exception:
        return None

def _extract_nfe_chave_from_payload(body: dict) -> str | None:
    e = _event_block(body)
    chave = _pick(e, "nfe_chave", "nChave", "chave_nfe", "chave")
    if chave:
        return str(chave)
    # tenta extrair dos links
    for fld in ("nfe_xml", "nfe_danfe", "xml", "xml_base64", "danfe"):
        v = _pick(e, fld) or _pick(body, fld)
        if isinstance(v, str):
            m = re.search(r"(\d{44})", v)
            if m:
                return m.group(1)
    return None

# ------------------------------------------------------------------------------
# Omie API
# ------------------------------------------------------------------------------
async def _omie_call(session: aiohttp.ClientSession, endpoint: str, call: str, payload: dict):
    """Chamada Omie (POST JSON)."""
    url = f"{OMIE_BASE}/{endpoint}"
    body = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [payload],
    }
    async with session.post(url, json=body, timeout=aiohttp.ClientTimeout(total=60)) as r:
        r.raise_for_status()
        data = await r.json()
        if isinstance(data, dict) and any(k in data for k in ("fault", "faultcode", "faultstring")):
            raise RuntimeError(f"Omie {endpoint}/{call}: {data}")
        return data

async def _fetch_pedido_por_codigo(session: aiohttp.ClientSession, codigo_pedido: int) -> dict:
    data = await _omie_call(session, "produtos/pedido/", "ConsultarPedido",
                            {"codigo_pedido": int(codigo_pedido)})
    return data.get("pedido_venda_produto") or data

async def _fetch_xml_por_chave(session: aiohttp.ClientSession, nfe_chave: str, data_emis_iso: str | None) -> str | None:
    """
    /api/v1/contador/xml/ -> ListarDocumentos: encontra pelo nChave e retorna cXml (base64).
    """
    d_ini = d_fim = _yyyymmdd_from_iso(data_emis_iso or "")
    payload = {
        "nPagina": 1,
        "nRegPorPagina": 200,
        "cModelo": "55",
        "dEmiInicial": d_ini or "",
        "dEmiFinal": d_fim or "",
    }
    lista = await _omie_call(session, "contador/xml/", "ListarDocumentos", payload)
    docs = (lista or {}).get("documentosEncontrados") or []
    for doc in docs:
        if str(doc.get("nChave")) == str(nfe_chave):
            cxml = doc.get("cXml")
            if not cxml:
                return None
            # se vier texto XML, codifica em base64
            if isinstance(cxml, str) and cxml.lstrip().startswith("<?xml"):
                return base64.b64encode(cxml.encode("utf-8")).decode("ascii")
            return cxml
    return None

# ------------------------------------------------------------------------------
# Health
# ------------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    return {"status": "healthy", "components": ["pedidos", "nfe_xml"], "compat": True}

@app.get("/")
async def root():
    return {"ok": True, "service": "omie-combined"}

# ------------------------------------------------------------------------------
# Webhooks
# ------------------------------------------------------------------------------
@router.post("/omie/webhook")
async def pedidos_webhook(request: Request, token: str = Query(...)):
    """Recebe VendaProduto.* e guarda bruto na fila."""
    if token != TOKEN_PEDIDOS:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    event_id = str(body.get("messageId") or body.get("id") or "")[:64] or None
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO public.omie_webhook_events (source, event_type, event_id, route, payload)
            VALUES ('omie', 'omie_webhook_received', $1, '/omie/webhook', $2::jsonb)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            event_id, json.dumps(body),
        )
    return {"ok": True}

@router.post("/xml/omie/webhook")
async def xml_webhook(request: Request, token: str = Query(...)):
    """Recebe NFe.NotaAutorizada/Cancelada e guarda bruto na fila."""
    if token != TOKEN_XML:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    event_id = str(body.get("messageId") or body.get("id") or "")[:64] or None
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO public.omie_webhook_events (source, event_type, event_id, route, payload)
            VALUES ('omie', 'nfe_xml_received', $1, '/xml/omie/webhook', $2::jsonb)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            event_id, json.dumps(body),
        )
    return {"ok": True}

# ------------------------------------------------------------------------------
# Admin: processar fila
# ------------------------------------------------------------------------------
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    processed = 0
    errors = 0
    log: list[dict] = []

    async with app.state.pool.acquire() as conn, aiohttp.ClientSession() as session:
        # 1) Pedidos
        rows = await conn.fetch("""
            SELECT id, payload
              FROM public.omie_webhook_events
             WHERE processed IS NOT TRUE
               AND route LIKE '/omie/webhook%%'
             ORDER BY received_at ASC
             LIMIT 200;
        """)
        for r in rows:
            try:
                ev = _as_dict(r["payload"])
                e  = _event_block(ev)

                codigo_pedido = (
                    e.get("idPedido")
                    or e.get("id_pedido")
                    or e.get("codigo_pedido")
                    or ev.get("idPedido")
                    or ev.get("id_pedido")
                    or ev.get("codigo_pedido")
                )
                if not codigo_pedido:
                    raise RuntimeError("evento sem idPedido/codigo_pedido")

                pedido = await _fetch_pedido_por_codigo(session, int(codigo_pedido))

                cab = pedido.get("cabecalho", {}) if isinstance(pedido, dict) else {}
                tot = pedido.get("total_pedido", {}) if isinstance(pedido, dict) else {}

                id_pedido_omie   = int(cab.get("codigo_pedido", codigo_pedido))
                numero           = str(cab.get("numero_pedido") or "")
                valor_total      = float(tot.get("valor_total_pedido") or 0)
                situacao         = str(cab.get("etapa") or "")
                quantidade_itens = int(cab.get("quantidade_itens") or 0)
                cliente_codigo   = str(cab.get("codigo_cliente") or "")

                await conn.execute("""
                    INSERT INTO public.omie_pedido
                      (id_pedido_omie, numero, valor_total, situacao, quantidade_itens,
                       cliente_codigo, detalhe, recebido_em, created_at)
                    VALUES
                      ($1, $2, $3, $4, $5, $6, $7::jsonb, now(), now())
                    ON CONFLICT (id_pedido_omie) DO UPDATE
                      SET numero = EXCLUDED.numero,
                          valor_total = EXCLUDED.valor_total,
                          situacao = EXCLUDED.situacao,
                          quantidade_itens = EXCLUDED.quantidade_itens,
                          cliente_codigo = EXCLUDED.cliente_codigo,
                          detalhe = EXCLUDED.detalhe,
                          updated_at = now();
                """, id_pedido_omie, numero, valor_total, situacao, quantidade_itens,
                     cliente_codigo, json.dumps(pedido))

                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, status = 'consumido'
                     WHERE id = $1;
                """, r["id"])

                processed += 1
                log.append({"pedido_ok": id_pedido_omie})

            except Exception as ex:
                errors += 1
                log.append({"pedido_err": str(ex)})
                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, status = $2
                     WHERE id = $1;
                """, r["id"], f"erro:{ex}")

        # 2) NFe XML
        rows = await conn.fetch("""
            SELECT id, payload
              FROM public.omie_webhook_events
             WHERE processed IS NOT TRUE
               AND route LIKE '/xml/omie/webhook%%'
             ORDER BY received_at ASC
             LIMIT 200;
        """)
        for r in rows:
            try:
                ev = _as_dict(r["payload"])
                e  = _event_block(ev)

                nfe_chave = _pick(e, "nfe_chave", "nChave", "chave_nfe", "chave")
                if not nfe_chave:
                    nfe_chave = _extract_nfe_chave_from_payload(ev)
                if not nfe_chave:
                    raise RuntimeError("evento NFe sem chave")

                data_emis  = _pick(e, "data_emis", "dEmissao")
                numero_nf  = (_pick(e, "numero_nf", "nNumero") or "")
                serie      = (_pick(e, "serie", "cSerie") or "")
                emitida_em = _parse_emitida_em(data_emis) if data_emis else None

                xml_b64 = await _fetch_xml_por_chave(session, str(nfe_chave), data_emis)
                if not xml_b64:
                    raise RuntimeError("XML não encontrado na API (ListarDocumentos)")

                await conn.execute("""
                    INSERT INTO public.omie_nfe_xml
                      (chave_nfe, numero, serie, emitida_em, recebido_em, xml_base64, created_at)
                    VALUES
                      ($1, $2, $3, $4, now(), $5, now())
                    ON CONFLICT (chave_nfe) DO UPDATE
                      SET numero     = EXCLUDED.numero,
                          serie      = EXCLUDED.serie,
                          xml_base64 = EXCLUDED.xml_base64,
                          updated_at = now();
                """, str(nfe_chave), str(numero_nf), str(serie), emitida_em, xml_b64)

                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, status = 'consumido'
                     WHERE id = $1;
                """, r["id"])

                processed += 1
                log.append({"nfe_ok": str(nfe_chave)})

            except Exception as ex:
                errors += 1
                log.append({"nfe_err": str(ex)})
                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, status = $2
                     WHERE id = $1;
                """, r["id"], f"erro:{ex}")

    return {"ok": True, "processed": processed, "errors": errors, "events": log}

# ------------------------------------------------------------------------------
# Admin: checagem rápida de hoje
# ------------------------------------------------------------------------------
@router.get("/admin/check-today")
async def check_today(secret: str = Query(...)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    async with app.state.pool.acquire() as conn:
        webhooks_count = await conn.fetchval("""
            SELECT COUNT(*) FROM public.omie_webhook_events
             WHERE (event_ts AT TIME ZONE 'America/Sao_Paulo')::date =
                   (now() AT TIME ZONE 'America/Sao_Paulo')::date;
        """)
        pedidos = await conn.fetch("""
            SELECT id_pedido_omie, numero, valor_total, situacao, quantidade_itens,
                   LEFT(COALESCE(detalhe::text,''), 160) AS preview, created_at
              FROM public.omie_pedido
             WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date =
                   (now() AT TIME ZONE 'America/Sao_Paulo')::date
             ORDER BY created_at DESC
             LIMIT 50;
        """)
        nfe = await conn.fetch("""
            SELECT chave_nfe, numero, serie, emitida_em,
                   CASE WHEN xml_base64 IS NULL THEN 'SEM_XML' ELSE 'OK_XML' END AS xml_status,
                   LENGTH(COALESCE(xml_base64,'')) AS xml_len, recebido_em
              FROM public.omie_nfe_xml
             WHERE (recebido_em AT TIME ZONE 'America/Sao_Paulo')::date =
                   (now() AT TIME ZONE 'America/Sao_Paulo')::date
             ORDER BY recebido_em DESC
             LIMIT 50;
        """)
    return {
        "webhooks_today": webhooks_count,
        "pedidos_today": [dict(r) for r in pedidos],
        "nfe_today": [dict(r) for r in nfe],
    }

# ------------------------------------------------------------------------------
# Erros
# ------------------------------------------------------------------------------
@app.exception_handler(Exception)
async def all_errors_handler(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"detail": "internal_error"})

# ------------------------------------------------------------------------------
# Mount
# ------------------------------------------------------------------------------
app.include_router(router)

# ------------------------------------------------------------------------------
# Local run
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
