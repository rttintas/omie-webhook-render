# app_combined.py
# FastAPI + asyncpg + httpx
# - /healthz                           -> health
# - POST /omie/webhook?token=...       -> webhooks de PEDIDOS (Omie)
# - POST /xml/omie/webhook?token=...   -> webhooks de NF-e (Omie)
# - POST /admin/run-jobs?secret=...    -> reprocessa eventos pendentes (de hoje)

import os
import json
import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# Env
# ------------------------------------------------------------------------------
DATABASE_URL          = os.getenv("DATABASE_URL")
WEBHOOK_TOKEN_PED     = os.getenv("OMIE_WEBHOOK_TOKEN", "")
WEBHOOK_TOKEN_XML     = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "")
ADMIN_RUNJOBS_SECRET  = os.getenv("ADMIN_RUNJOBS_SECRET", "julia-matheus")

OMIE_APP_KEY          = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET       = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT_SECONDS  = int(os.getenv("OMIE_TIMEOUT_SECONDS", "25"))

# endpoints Omie (podem ser sobrescritos por env)
OMIE_PEDIDO_URL       = os.getenv("OMIE_PEDIDO_URL", "https://app.omie.com.br/api/v1/produtos/pedido/")
OMIE_XML_URL          = os.getenv("OMIE_XML_URL",     "https://app.omie.com.br/api/v1/nfe/nfetool/")

# qual call usar p/ listar/buscar documentos na API de NFe
# (ex.: "ListarDocumentos" – pode variar por conta/ambiente)
OMIE_XML_LIST_CALL    = os.getenv("OMIE_XML_LIST_CALL", "ListarDocumentos")

# janelas para busca de XML quando não há data no evento
NFE_LOOKBACK_DAYS     = int(os.getenv("NFE_LOOKBACK_DAYS",  "7"))
NFE_LOOKAHEAD_DAYS    = int(os.getenv("NFE_LOOKAHEAD_DAYS", "0"))

# enriquecer pedido no momento do run_jobs
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "true").lower() in ("1","true","yes","y")

if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Postgres).")

# ------------------------------------------------------------------------------
# App
# ------------------------------------------------------------------------------
app    = FastAPI(title="Omie Webhooks + Jobs")
router = APIRouter()


# ------------------------------------------------------------------------------
# Helpers genéricos
# ------------------------------------------------------------------------------
def _now_tz() -> datetime:
    return datetime.now(timezone.utc)

def _as_json(obj: Any) -> Dict[str, Any]:
    """Converte payload possível (str/dict/None) em dict seguro."""
    if obj is None:
        return {}
    if isinstance(obj, (dict, list)):
        return obj  # type: ignore[return-value]
    if isinstance(obj, (bytes, bytearray)):
        try:
            return json.loads(obj.decode("utf-8"))
        except Exception:
            return {}
    if isinstance(obj, str):
        obj = obj.strip()
        if not obj:
            return {}
        try:
            return json.loads(obj)
        except Exception:
            # às vezes chega como "{...}" mas com aspas simples
            try:
                return json.loads(obj.replace("'", '"'))
            except Exception:
                return {}
    return {}

def _pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _event_block(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Alguns webhooks vêm como {"event": {...}}, outros já vêm direto no corpo.
    """
    if not isinstance(body, dict):
        return {}
    ev = body.get("event")
    if isinstance(ev, dict):
        return ev
    # às vezes "payload" já é o evento
    return body

def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    try:
        # tenta ISO
        return datetime.fromisoformat(str(v).replace("Z","+00:00"))
    except Exception:
        pass
    # tenta apenas data
    try:
        d = datetime.strptime(str(v), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None

def _safe_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _date_range_by_emitida(data_emis_str: Optional[str]) -> Tuple[datetime, datetime]:
    """
    Se temos data de emissão, usa +/- 1 dia.
    Se não temos, usa LOOKBACK/LOOKAHEAD de env.
    """
    if data_emis_str:
        d0 = _parse_dt(data_emis_str)
        if d0:
            start = (d0 - timedelta(days=1)).astimezone(timezone.utc)
            end   = (d0 + timedelta(days=1)).astimezone(timezone.utc)
            return start, end

    now = _now_tz()
    start = (now - timedelta(days=NFE_LOOKBACK_DAYS))
    end   = (now + timedelta(days=NFE_LOOKAHEAD_DAYS))
    return start, end


# ------------------------------------------------------------------------------
# Helpers HTTP/Omie
# ------------------------------------------------------------------------------
async def _omie_post(client: httpx.AsyncClient, url: str, call: str, params: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [params],
    }
    r = await client.post(url, json=payload, timeout=OMIE_TIMEOUT_SECONDS)
    r.raise_for_status()
    return r.json()

async def _fetch_pedido_by_id(client: httpx.AsyncClient, id_pedido: Any) -> Optional[Dict[str, Any]]:
    """
    Tenta buscar detalhes do pedido por id (varia conforme conta – aqui tentamos duas chamadas comuns).
    """
    id_pedido = int(str(id_pedido))
    # 1) ConsultarPedido
    try:
        j = await _omie_post(client, OMIE_PEDIDO_URL, "ConsultarPedido", {"nCodPed": id_pedido})
        if isinstance(j, dict) and j:
            return j
    except Exception:
        pass
    # 2) ListarPedidos (filtrando por id)
    try:
        j = await _omie_post(client, OMIE_PEDIDO_URL, "ListarPedidos", {
            "pagina": 1,
            "registros_por_pagina": 1,
            "apenas_importado_api": "N",
            "filtrar_por_codigo": id_pedido,
        })
        if isinstance(j, dict) and j.get("pedido_venda_produto"):
            return j["pedido_venda_produto"][0]
    except Exception:
        pass
    return None


# ------------------------------------------------------------------------------
# --- NF-e helpers --------------------------------------------------------------
# ------------------------------------------------------------------------------
def _extract_nfe_fields_from_event(ev: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Retorna (chave, numero, serie, data_emis, xml_base64) a partir do evento,
    cobrindo os 3 cenários:
      1) XML já no evento (campo 'nfe_xml' ou 'xml')
      2) vem 'nfe_chave' / 'chave' / 'chave_nfe'
      3) vem só 'id_nf'
    """
    # XML direto?
    xml_b64  = _safe_text(_pick(ev, "nfe_xml", "xml"))
    chave    = _safe_text(_pick(ev, "nfe_chave", "chave_nfe", "chave"))
    numero   = _safe_text(_pick(ev, "numero_nf", "nNumero"))
    serie    = _safe_text(_pick(ev, "serie", "cSerie"))
    data_emis= _safe_text(_pick(ev, "data_emis", "dEmissao"))
    # pode vir só o id da nota
    id_nf    = _safe_text(_pick(ev, "id_nf", "idNf", "idNota"))

    # algumas integrações trazem o bloco "nfe": {...}
    nfe_blk  = ev.get("nfe")
    if isinstance(nfe_blk, dict):
        xml_b64   = xml_b64  or _safe_text(_pick(nfe_blk, "nfe_xml", "xml"))
        chave     = chave    or _safe_text(_pick(nfe_blk, "nfe_chave", "chave"))
        numero    = numero   or _safe_text(_pick(nfe_blk, "numero_nf", "nNumero"))
        serie     = serie    or _safe_text(_pick(nfe_blk, "serie", "cSerie"))
        data_emis = data_emis or _safe_text(_pick(nfe_blk, "data_emis", "dEmissao"))
        id_nf     = id_nf    or _safe_text(_pick(nfe_blk, "id_nf", "idNf", "idNota"))

    # também há payloads em que o id_nf está no topo (fora de 'event')
    id_nf_top = _safe_text(_pick(ev, "id_nf"))
    if id_nf_top:
        id_nf = id_nf or id_nf_top

    return chave, numero, serie, data_emis, (xml_b64 or None)


async def _fetch_xml_por_chave(client: httpx.AsyncClient, chave: str, data_emis: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Busca o XML (base64) na API da Omie se tivermos apenas a chave.
    Retorna (obj_documento, xml_base64)
    """
    start, end = _date_range_by_emitida(data_emis)
    try:
        payload = {
            "pagina": 1,
            "registros_por_pagina": 50,
            "ordenar_por": "DATA_EMISSAO",
            "ordem_descrescente": "N",
            "filtrar_por_chave": chave,
            "emissao_de": start.strftime("%Y-%m-%d"),
            "emissao_ate": end.strftime("%Y-%m-%d"),
        }
        j = await _omie_post(client, OMIE_XML_URL, OMIE_XML_LIST_CALL, payload)
        docs = j.get("documentos") or j.get("lista_documentos") or j.get("documento") or []
        if isinstance(docs, dict):
            docs = [docs]
        for d in docs:
            # tentamos campos comuns
            xml_64 = d.get("xml") or d.get("xml_base64") or d.get("xmlNFe")
            if xml_64:
                return d, str(xml_64)
        return None, None
    except Exception:
        return None, None


async def _fetch_xml_por_id_nf(client: httpx.AsyncClient, id_nf: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Quando o evento vem só com id_nf, tenta localizar o documento/nota por id.
    """
    try:
        payload = {
            "pagina": 1,
            "registros_por_pagina": 1,
            "ordenar_por": "DATA_EMISSAO",
            "ordem_descrescente": "N",
            "id_nfe": id_nf
        }
        j = await _omie_post(client, OMIE_XML_URL, OMIE_XML_LIST_CALL, payload)
        docs = j.get("documentos") or j.get("lista_documentos") or j.get("documento") or []
        if isinstance(docs, dict):
            docs = [docs]
        if docs:
            d = docs[0]
            xml_64 = d.get("xml") or d.get("xml_base64") or d.get("xmlNFe")
            return d, (str(xml_64) if xml_64 else None)
        return None, None
    except Exception:
        return None, None


# ------------------------------------------------------------------------------
# Startup: pool + migrations simples
# ------------------------------------------------------------------------------
@app.on_event("startup")
async def _startup():
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with app.state.pool.acquire() as conn:
        await _run_migrations(conn)


async def _run_migrations(conn: asyncpg.Connection):
    # eventos
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_webhook_events(
        id              bigserial PRIMARY KEY,
        source          text,
        event_type      text,
        route           text,
        http_status     integer,
        topic           text,
        status          text,
        raw_headers     jsonb,
        payload         jsonb,
        event_ts        timestamptz,
        received_at     timestamptz DEFAULT now(),
        processed       boolean     DEFAULT false,
        processed_at    timestamptz
    );
    """)
    # pedidos
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_pedido(
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
    # NF xml
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe_xml(
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

    body = _as_json(body)
    event = _event_block(_as_json(body.get("payload") or body))
    topic = _safe_text(_pick(body, "topic", "TipoEvento", "evento")) or "omie_webhook_received"

    async with app.state.pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO public.omie_webhook_events(source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at)
        VALUES('omie', $1, '/omie/webhook', 200, $2, NULL, $3, $4, now(), now());
        """, "omie_webhook_received", topic, _as_json(dict(request.headers)), _as_json(body))

    return JSONResponse({"ok": True})


@router.post("/xml/omie/webhook")
async def xml_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=403, detail="invalid token")
    try:
        body = await request.json()
    except Exception:
        body = {}

    body = _as_json(body)
    event = _event_block(_as_json(body.get("payload") or body))
    topic = _safe_text(_pick(body, "topic", "TipoEvento", "evento")) or "nfe_xml_received"

    async with app.state.pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO public.omie_webhook_events(source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at)
        VALUES('omie', $1, '/xml/omie/webhook', 200, $2, NULL, $3, $4, now(), now());
        """, "nfe_xml_received", topic, _as_json(dict(request.headers)), _as_json(body))

    return JSONResponse({"ok": True})


# ------------------------------------------------------------------------------
# Jobs
# ------------------------------------------------------------------------------
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    processed = 0
    errors    = 0
    log: list = []

    async with app.state.pool.acquire() as conn, httpx.AsyncClient(timeout=OMIE_TIMEOUT_SECONDS) as client:
        # pega eventos de hoje que ainda não foram processados
        rows = await conn.fetch("""
            SELECT id, route, payload
            FROM public.omie_webhook_events
            WHERE processed IS NOT TRUE
              AND (received_at AT TIME ZONE 'America/Sao_Paulo')::date = (now() AT TIME ZONE 'America/Sao_Paulo')::date
            ORDER BY id ASC
            LIMIT 500;
        """)

        for r in rows:
            ev_id  = r["id"]
            route  = r["route"] or ""
            payload= _as_json(r["payload"])
            ev     = _event_block(_as_json(payload.get("payload") or payload))

            # decide o tipo
            try:
                if "/xml/omie/webhook" in route:
                    # ------------------------------ NFe -------------------------
                    chave, numero, serie, data_emis, xml_b64 = _extract_nfe_fields_from_event(ev)

                    if not xml_b64:
                        if chave:
                            _doc, xml_b64 = await _fetch_xml_por_chave(client, chave, data_emis)
                        else:
                            id_nf = _safe_text(_pick(ev, "id_nf", "idNf", "idNota"))
                            if id_nf:
                                _doc, xml_b64 = await _fetch_xml_por_id_nf(client, id_nf)

                    if not (chave or numero or serie):
                        # tenta achar dentro do documento retornado
                        # (não faz mal se None)
                        pass

                    if not (chave or xml_b64):
                        raise RuntimeError("evento NFe sem chave e sem XML")

                    await conn.execute("""
                        INSERT INTO public.omie_nfe_xml (chave_nfe, numero, serie, emitida_em, xml_base64, recebido_em, updated_at)
                        VALUES ($1, $2, $3, $4, $5, now(), now())
                        ON CONFLICT (chave_nfe) DO UPDATE
                          SET numero     = EXCLUDED.numero,
                              serie      = EXCLUDED.serie,
                              xml_base64 = COALESCE(EXCLUDED.xml_base64, public.omie_nfe_xml.xml_base64),
                              updated_at = now();
                    """, str(chave or ""), str(numero or ""), str(serie or ""), _parse_dt(data_emis), (xml_b64 or ""))

                    await conn.execute("""
                        UPDATE public.omie_webhook_events
                           SET processed = TRUE, processed_at = now(), status = '/xml: ok'
                         WHERE id = $1;
                    """, ev_id)

                    processed += 1
                    log.append({"nfe_ok": (chave or "sem_chave")})

                else:
                    # ------------------------------ Pedido ----------------------
                    id_pedido = _pick(ev, "idPedido", "codigo_pedido", "codigo_pedido_omie", "id_pedido")
                    if not id_pedido:
                        raise RuntimeError("evento sem idPedido/codigo_pedido")

                    detalhe = None
                    if ENRICH_PEDIDO_IMEDIATO:
                        detalhe = await _fetch_pedido_by_id(client, id_pedido)

                    # extrai campos básicos (se não tiver detalhe, tenta do evento)
                    numero = _safe_text(
                        (detalhe or {}).get("cabecalho", {}).get("numero_pedido") if isinstance(detalhe, dict) else None
                    ) or _safe_text(_pick(ev, "numero", "numero_pedido"))

                    valor_total = None
                    if isinstance(detalhe, dict):
                        try:
                            valor_total = (detalhe.get("cabecalho") or {}).get("valor_total")
                        except Exception:
                            valor_total = None

                    situacao = _safe_text(_pick(ev, "situacao", "status", "etapa"))
                    qtd_itens = None
                    if isinstance(detalhe, dict):
                        try:
                            itens = detalhe.get("det", detalhe.get("detalhe", []))
                            if isinstance(itens, list):
                                qtd_itens = len(itens)
                        except Exception:
                            qtd_itens = None

                    await conn.execute("""
                        INSERT INTO public.omie_pedido (id_pedido_omie, numero, valor_total, situacao, quantidade_itens, cliente_codigo, detalhe, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
                        ON CONFLICT (id_pedido_omie) DO UPDATE
                          SET numero = EXCLUDED.numero,
                              valor_total = COALESCE(EXCLUDED.valor_total, public.omie_pedido.valor_total),
                              situacao = COALESCE(EXCLUDED.situacao, public.omie_pedido.situacao),
                              quantidade_itens = COALESCE(EXCLUDED.quantidade_itens, public.omie_pedido.quantidade_itens),
                              cliente_codigo = COALESCE(EXCLUDED.cliente_codigo, public.omie_pedido.cliente_codigo),
                              detalhe = COALESCE(EXCLUDED.detalhe, public.omie_pedido.detalhe),
                              updated_at = now();
                    """,
                    int(str(id_pedido)),
                    numero,
                    valor_total,
                    situacao,
                    qtd_itens,
                    _safe_text(_pick(ev, "idCliente", "codigo_cliente")),
                    detalhe if isinstance(detalhe, dict) else _as_json(ev))

                    await conn.execute("""
                        UPDATE public.omie_webhook_events
                           SET processed = TRUE, processed_at = now(), status = '/omi: ok'
                         WHERE id = $1;
                    """, ev_id)

                    processed += 1
                    log.append({"pedido_ok": int(str(id_pedido))})

            except Exception as ex:
                errors += 1
                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, processed_at = now(), status = $1
                     WHERE id = $2;
                """, f"erro:{ex}", ev_id)
                # registra no log de retorno
                if "/xml/omie/webhook" in route:
                    log.append({"nfe_err": str(ex)})
                else:
                    log.append({"pedido_err": str(ex)})

    return JSONResponse({"ok": True, "processed": processed, "errors": errors, "events": log})


# ------------------------------------------------------------------------------
# Mount
# ------------------------------------------------------------------------------
app.include_router(router)
