# app_combined.py — serviço único (webhooks + jobs) para Omie (Pedidos + NF-e)
# Start command (Render): uvicorn app_combined:app --host 0.0.0.0 --port $PORT

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# Configuração de log
# ------------------------------------------------------------------------------
logger = logging.getLogger("app_combined")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ------------------------------------------------------------------------------
# ENV
# ------------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL")

WEBHOOK_TOKEN_PED = os.getenv("OMIE_WEBHOOK_TOKEN", "")          # ex: t1
WEBHOOK_TOKEN_XML = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "")      # ex: t2
ADMIN_RUNJOBS_SECRET = os.getenv("ADMIN_RUNJOBS_SECRET", "julia-matheus")

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT_SECONDS = int(os.getenv("OMIE_TIMEOUT_SECONDS", "25"))

# Pedidos
OMIE_PEDIDO_URL = os.getenv("OMIE_PEDIDO_URL", "https://app.omie.com.br/api/v1/produtos/pedido/")
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "true").lower() in ("1", "true", "yes", "y")

# NF-e (contador/xml é o recomendado)
OMIE_XML_PROVIDER = os.getenv("OMIE_XML_PROVIDER", "contador")  # 'contador' ou 'nfetool'
OMIE_CONTADOR_XML_URL = os.getenv("OMIE_CONTADOR_XML_URL", "https://app.omie.com.br/api/v1/contador/xml/")
OMIE_XML_URL = os.getenv("OMIE_XML_URL", "https://app.omie.com.br/api/v1/nfe/nfetool/")
OMIE_XML_LIST_CALL = os.getenv("OMIE_XML_LIST_CALL", "ListarDocumentos")
NFE_LOOKBACK_DAYS = int(os.getenv("NFE_LOOKBACK_DAYS", "15"))
NFE_LOOKAHEAD_DAYS = int(os.getenv("NFE_LOOKAHEAD_DAYS", "1"))

# ------------------------------------------------------------------------------
# App / Pool
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
    async with app.state.pool.acquire() as conn:
        await conn.execute("SELECT 1;")
        await _ensure_schema(conn)
    try:
        yield
    finally:
        await app.state.pool.close()

app = FastAPI(title="Omie Webhooks + Jobs (combined)", lifespan=lifespan)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _now_tz() -> datetime:
    return datetime.now(timezone.utc)

def _tz_sp() -> timezone:
    return timezone(timedelta(hours=-3))  # America/Sao_Paulo (fixo)

def _as_json(obj: Any) -> Dict[str, Any]:
    try:
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, (list, tuple)):
            return {"_list": list(obj)}
        if isinstance(obj, (bytes, bytearray)):
            return json.loads(obj.decode("utf-8", errors="ignore"))
        return json.loads(str(obj))
    except Exception:
        return {}

def _pick(d: Dict[str, Any], *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _safe_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v)
    fmts = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]
    for f in fmts:
        try:
            if f.endswith("Z") and s.endswith("Z"):
                dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            else:
                dt = datetime.strptime(s, f)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _parse_dt_any(v: Any) -> Optional[datetime]:
    return _parse_dt(v)

def _date_range_by_emitida(data_emis_str: Optional[str]) -> Tuple[datetime, datetime]:
    if data_emis_str:
        d0 = _parse_dt_any(data_emis_str)
        if d0:
            start = (d0 - timedelta(days=1)).astimezone(timezone.utc)
            end = (d0 + timedelta(days=1)).astimezone(timezone.utc)
            return start, end
    end = _now_tz() + timedelta(days=NFE_LOOKAHEAD_DAYS)
    start = _now_tz() - timedelta(days=NFE_LOOKBACK_DAYS)
    return start, end

def _build_omie_body(call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"call": call, "app_key": OMIE_APP_KEY, "app_secret": OMIE_APP_SECRET, "param": [payload]}

async def _omie_post(client: httpx.AsyncClient, url: str, call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = _build_omie_body(call, payload)
    res = await client.post(url, json=body, timeout=OMIE_TIMEOUT_SECONDS)
    res.raise_for_status()
    j = res.json() if res.content else {}
    if isinstance(j, dict) and j.get("faultstring"):
        raise RuntimeError(f"Omie error: {j.get('faultstring')}")
    return j

# ------------------------------------------------------------------------------
# Normalização do bloco de evento
# ------------------------------------------------------------------------------
def _event_block(body: Dict[str, Any]) -> Dict[str, Any]:
    """Devolve o bloco de evento em qualquer formato comum da Omie."""
    b = _as_json(body)

    # Se vier como {"payload":"{...json...}"}, tenta decodificar
    if isinstance(b.get("payload"), (str, bytes, bytearray)):
        try:
            b["payload"] = json.loads(b["payload"])
        except Exception:
            pass

    # Candidatos diretos
    for key in ("evento", "event", "payload"):
        cand = b.get(key)
        if isinstance(cand, dict):
            # Às vezes ainda vem outro 'event' por dentro
            if isinstance(cand.get("event"), dict):
                return cand["event"]
            if isinstance(cand.get("evento"), dict):
                return cand["evento"]
            return cand

    # Fallback: raiz já tem 'event' / 'evento'
    if isinstance(b.get("event"), dict):
        return b["event"]
    if isinstance(b.get("evento"), dict):
        return b["evento"]

    # Sem nós conhecidos: devolve o próprio corpo
    return b

# ------------------------------------------------------------------------------
# Extração de campos (NF-e)
# ------------------------------------------------------------------------------
def _extract_nfe_fields_from_event(ev: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    xml_b64 = _safe_text(_pick(ev, "xml_base64", "xml", "cXml", "nfe_xml"))
    chave = _safe_text(_pick(ev, "nChave", "chave_nfe", "chave", "nfe_chave"))
    numero = _safe_text(_pick(ev, "nNumero", "numero_nf", "numero"))
    serie = _safe_text(_pick(ev, "cSerie", "serie"))
    data_emis = _safe_text(_pick(ev, "dEmissao", "data_emis", "dhEmi"))

    nfe_blk = ev.get("nfe")
    if isinstance(nfe_blk, dict):
        xml_b64 = xml_b64 or _safe_text(_pick(nfe_blk, "xml_base64", "xml", "cXml", "nfe_xml"))
        chave = chave or _safe_text(_pick(nfe_blk, "nChave", "chave_nfe", "chave", "nfe_chave"))
        numero = numero or _safe_text(_pick(nfe_blk, "nNumero", "numero_nf", "numero"))
        serie = serie or _safe_text(_pick(nfe_blk, "cSerie", "serie"))
        data_emis = data_emis or _safe_text(_pick(nfe_blk, "dEmissao", "data_emis", "dhEmi"))
    return chave, numero, serie, data_emis, xml_b64

async def _fetch_xml_por_chave(client: httpx.AsyncClient, chave: str, data_emis: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not (OMIE_APP_KEY and OMIE_APP_SECRET and chave):
        return None, None

    dt_ref = _parse_dt_any(data_emis) or _now_tz()
    dt_ini = (dt_ref - timedelta(days=NFE_LOOKBACK_DAYS))
    dt_fim = (dt_ref + timedelta(days=NFE_LOOKAHEAD_DAYS))

    # Provedor 'contador'
    if OMIE_XML_PROVIDER.lower() == "contador":
        payload = {
            "nPagina": 1,
            "nRegPorPagina": 50,
            "cModelo": "55",
            "dEmiInicial": dt_ini.strftime("%Y-%m-%d"),
            "dEmiFinal": dt_fim.strftime("%Y-%m-%d"),
            "nChave": chave,
        }
        j = await _omie_post(client, OMIE_CONTADOR_XML_URL, OMIE_XML_LIST_CALL, payload)

        docs: List[Dict[str, Any]] = []
        for key in ("documentos", "lista_documentos", "documento", "lista", "docs"):
            v = j.get(key)
            if isinstance(v, list):
                docs = v
                break
            if isinstance(v, dict):
                docs = [v]
                break

        target = None
        for d in docs:
            try:
                if str(d.get("nChave") or "").strip() == chave:
                    target = d
                    break
            except Exception:
                continue
        if not target and docs:
            target = docs[0]

        if isinstance(target, dict):
            numero = target.get("nNumero")
            serie = target.get("cSerie")
            dEmissao = target.get("dEmissao")
            hEmissao = target.get("hEmissao")
            emitida_em = None
            if dEmissao:
                emitida_em = _parse_dt_any(f"{dEmissao} {hEmissao}" if hEmissao else dEmissao)
            xml_text = target.get("cXml")
            target_norm = {
                "chave_nfe": chave,
                "numero": numero,
                "serie": serie,
                "emitida_em": emitida_em.isoformat() if emitida_em else None,
                "xml": xml_text,
                "_raw": target,
            }
            return target_norm, xml_text
        return None, None

    # Provedor 'nfetool' (legado)
    payload = {
        "pagina": 1,
        "registros_por_pagina": 50,
        "apenas_importadas": "N",
        "filtrar_por_data": "S",
        "data_emissao_inicial": dt_ini.date().isoformat(),
        "data_emissao_final": dt_fim.date().isoformat(),
        "chave_nfe": chave,
    }
    j = await _omie_post(client, OMIE_XML_URL, OMIE_XML_LIST_CALL, payload)
    docs = j.get("documentos", []) or j.get("lista", []) or []
    if docs:
        item = docs[0]
        xml_b64 = _safe_text(_pick(item, "xml", "xml_base64", "xmlNFe", "arquivo_xml"))
        return item, xml_b64
    return None, None

async def _fetch_xml_por_id_nf(client: httpx.AsyncClient, id_nf: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        payload = {"nIdNF": int(id_nf)}
    except Exception:
        payload = {"nIdNF": id_nf}
    try:
        j = await _omie_post(client, OMIE_CONTADOR_XML_URL, OMIE_XML_LIST_CALL, payload)
        xml_64 = j.get("cXml") or j.get("xml") or j.get("xml_base64")
        return j, str(xml_64) if xml_64 else None
    except Exception:
        return None, None

# ------------------------------------------------------------------------------
# Extração de campos (Pedido)
# ------------------------------------------------------------------------------
def _extract_pedido_fields(ev: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # Se ainda veio {"event": {...}} ou {"evento": {...}}, entra nele
    if isinstance(ev.get("event"), dict):
        ev = ev["event"]
    elif isinstance(ev.get("evento"), dict):
        ev = ev["evento"]

    id_ped = _safe_text(_pick(ev, "codigo_pedido", "idPedido", "id_pedido", "codigoPedido"))
    numero = _safe_text(_pick(ev, "numero_pedido", "numeroPedido", "numero"))
    integ = _safe_text(_pick(ev, "codigo_pedido_integracao", "codigoPedidoIntegracao"))

    pvp = ev.get("pedido_venda_produto")
    if isinstance(pvp, dict):
        cab = pvp.get("cabecalho") if isinstance(pvp.get("cabecalho"), dict) else {}
        id_ped = id_ped or _safe_text(_pick(cab, "codigo_pedido"))
        numero = numero or _safe_text(_pick(cab, "numero_pedido"))
        integ = integ or _safe_text(_pick(cab, "codigo_pedido_integracao"))

    return id_ped, numero, integ

async def _consultar_pedido(client: httpx.AsyncClient, codigo_pedido: int) -> Dict[str, Any]:
    payload = {"codigo_pedido": codigo_pedido}
    j = await _omie_post(client, OMIE_PEDIDO_URL, "ConsultarPedido", payload)
    return j if isinstance(j, dict) else {}

# ------------------------------------------------------------------------------
# DB bootstrap
# ------------------------------------------------------------------------------
async def _ensure_schema(conn: asyncpg.Connection) -> None:
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_webhook_events(
        id           bigserial PRIMARY KEY,
        source       text,
        event_type   text,
        route        text,
        http_status  int,
        topic        text,
        status       text,
        raw_headers  jsonb,
        payload      jsonb,
        event_ts     timestamptz,
        received_at  timestamptz DEFAULT now(),
        processed    boolean DEFAULT false,
        processed_at timestamptz,
        error        text
    );
    CREATE INDEX IF NOT EXISTS idx_owe_received_at ON public.omie_webhook_events (received_at);
    CREATE INDEX IF NOT EXISTS idx_owe_processed   ON public.omie_webhook_events (processed);
    CREATE INDEX IF NOT EXISTS idx_owe_route       ON public.omie_webhook_events (route);
    CREATE INDEX IF NOT EXISTS idx_owe_topic       ON public.omie_webhook_events (topic);
    """)

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_pedido(
        id_pedido_omie   bigint PRIMARY KEY,
        numero           text,
        valor_total      numeric,
        situacao         text,
        quantidade_itens int,
        cliente_codigo   text,
        detalhe          jsonb,
        created_at       timestamptz,
        updated_at       timestamptz
    );
    """)

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe_xml(
        chave_nfe   text PRIMARY KEY,
        numero      text,
        serie       text,
        emitida_em  timestamptz,
        xml_base64  text,
        recebido_em timestamptz,
        created_at  timestamptz,
        updated_at  timestamptz
    );
    """)

# ------------------------------------------------------------------------------
# Health
# ------------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT now() AS now")
        return {"ok": True, "now": str(row["now"])}

# ------------------------------------------------------------------------------
# Webhooks (grava na fila)
# ------------------------------------------------------------------------------
@router.post("/omie/webhook")
async def pedidos_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_PED:
        raise HTTPException(status_code=403, detail="invalid token")

    try:
        body = await request.json()
    except Exception:
        body = {}
    logger.info("📥 Webhook pedido recebido: %s", json.dumps(body, ensure_ascii=False)[:800])

    topic = _safe_text(_pick(_as_json(body), "topic", "TipoEvento", "evento")) or "omie_webhook_received"

    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at, processed)
            VALUES ('omie', 'omie_webhook_received', '/omie/webhook', 200, $1, NULL, $2::jsonb, $3::jsonb, now(), now(), FALSE);
        """, topic, json.dumps(dict(request.headers)), json.dumps(_as_json(body)))
    return JSONResponse({"ok": True})

@router.post("/xml/omie/webhook")
async def webhook_xml(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=403, detail="invalid token")

    try:
        body = await request.json()
    except Exception:
        body = {}
    logger.info("📥 Webhook NFe recebido: %s", json.dumps(body, ensure_ascii=False)[:800])

    topic = _safe_text(_pick(_as_json(body), "topic", "TipoEvento", "evento")) or "nfe_xml_received"

    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at, processed)
            VALUES ('omie', 'nfe_xml_received', '/xml/omie/webhook', 200, $1, NULL, $2::jsonb, $3::jsonb, now(), now(), FALSE);
        """, topic, json.dumps(dict(request.headers)), json.dumps(_as_json(body)))
    return JSONResponse({"ok": True})

# ------------------------------------------------------------------------------
# Run Jobs (cron)
# ------------------------------------------------------------------------------
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    processed = 0
    errors = 0
    log: List[Dict[str, Any]] = []

    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, route, payload
              FROM public.omie_webhook_events
             WHERE processed IS FALSE
               AND received_at::date >= current_date
             ORDER BY id ASC
             LIMIT 500;
        """)

        logger.info("🔍 Encontrados %s eventos para processar", len(rows))

        async with httpx.AsyncClient(timeout=OMIE_TIMEOUT_SECONDS) as client:
            for r in rows:
                ev_id = r["id"]
                route = r["route"] or ""
                body = _as_json(r["payload"])
                ev = _event_block(body.get("payload") or body)

                try:
                    # ---------------------- NF-e ----------------------
                    if "/xml/omie/webhook" in route:
                        chave, numero, serie, data_emis, xml_b64 = _extract_nfe_fields_from_event(ev)

                        if not xml_b64:
                            if chave:
                                _doc, xml_b64 = await _fetch_xml_por_chave(client, chave, data_emis)
                            else:
                                id_nf = _safe_text(_pick(ev, "nIdNF", "id_nf", "idNf", "idNota"))
                                if id_nf:
                                    _doc, xml_b64 = await _fetch_xml_por_id_nf(client, id_nf)

                        if not (chave or numero or serie) and xml_b64:
                            # ok, persiste só o XML
                            pass

                        if not xml_b64 and not chave:
                            raise RuntimeError("NFe sem 'chave' e sem 'xml' para persistir")

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
                        _safe_text(chave) or _safe_text(_pick(ev, "chNFe", "chaveAcesso", "nChave")),
                        _safe_text(numero),
                        _safe_text(serie),
                        _parse_dt(_safe_text(data_emis)),
                        _safe_text(xml_b64))

                        await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now(), error=NULL WHERE id=$1;", ev_id)
                        processed += 1
                        log.append({"nfe_ok": ev_id})
                        continue

                    # ---------------------- Pedido ----------------------
                    codigo_pedido, numero_pedido, codigo_integracao = _extract_pedido_fields(ev)

                    if not codigo_pedido:
                        logger.warning("⚠️  Evento %s sem código de pedido válido", ev_id)
                        await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now(), error=NULL WHERE id=$1;", ev_id)
                        processed += 1
                        log.append({"pedido_skip_sem_codigo": ev_id})
                        continue

                    pv = {}
                    cab = {}
                    det = []
                    if ENRICH_PEDIDO_IMEDIATO:
                        try:
                            pv = (await _consultar_pedido(client, int(str(codigo_pedido)))) or {}
                        except Exception as ex:
                            logger.warning("pedido_enrich_err: %s", ex)

                        cab = (pv.get("pedido_venda_produto") or {}).get("cabecalho") or {}
                        det = (pv.get("pedido_venda_produto") or {}).get("det") or []

                    numero = _safe_text(_pick(cab, "numero_pedido", "numero")) or numero_pedido
                    valor_total = _pick(pv.get("total_pedido") or {}, "valor_total_pedido", "valor_total")
                    situacao = _safe_text(_pick(pv.get("infoCadastro") or {}, "etapa", "situacao", "faturado"))
                    qtde_itens = len(det) if isinstance(det, list) else None
                    cliente_codigo = _safe_text(_pick(cab, "codigo_cliente", "id_cliente", "cliente_codigo"))

                    await conn.execute("""
                        INSERT INTO public.omie_pedido
                            (id_pedido_omie, numero, valor_total, situacao, quantidade_itens, cliente_codigo, detalhe, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now(), now())
                        ON CONFLICT (id_pedido_omie) DO UPDATE
                           SET numero           = COALESCE(EXCLUDED.numero, public.omie_pedido.numero),
                               valor_total      = COALESCE(EXCLUDED.valor_total, public.omie_pedido.valor_total),
                               situacao         = COALESCE(EXCLUDED.situacao, public.omie_pedido.situacao),
                               quantidade_itens = COALESCE(EXCLUDED.quantidade_itens, public.omie_pedido.quantidade_itens),
                               cliente_codigo   = COALESCE(EXCLUDED.cliente_codigo, public.omie_pedido.cliente_codigo),
                               detalhe          = COALESCE(EXCLUDED.detalhe, public.omie_pedido.detalhe),
                               updated_at       = now();
                    """,
                    int(str(codigo_pedido)),
                    numero,
                    valor_total,
                    situacao,
                    qtde_itens,
                    cliente_codigo,
                    json.dumps(pv if isinstance(pv, dict) else {}))

                    await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now(), error=NULL WHERE id=$1;", ev_id)
                    processed += 1
                    log.append({"pedido_ok": {"id": ev_id, "codigo_pedido": codigo_pedido}})
                except Exception as ex:
                    errors += 1
                    logger.warning("Erro processando evento %s (%s): %s", ev_id, route, ex)
                    await conn.execute(
                        "UPDATE public.omie_webhook_events SET error=$1 WHERE id=$2;",
                        f"{type(ex).__name__}: {ex}",
                        ev_id,
                    )

    return JSONResponse({"ok": True, "processed": processed, "errors": errors, "events": log})

# ------------------------------------------------------------------------------
# Mount
# ------------------------------------------------------------------------------
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
