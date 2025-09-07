# app_combined.py — serviço único (webhooks + jobs) com ajustes para Omie (Pedidos + NF-e via contador/xml)
# Start command (Render): uvicorn app_combined:app --host 0.0.0.0 --port $PORT

import os
import json
import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse

# ========================= ENV =========================
DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_RUNJOBS_SECRET = os.getenv("ADMIN_RUNJOBS_SECRET", "julia-matheus")

WEBHOOK_TOKEN_PED = os.getenv("OMIE_WEBHOOK_TOKEN", "")
WEBHOOK_TOKEN_XML = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "")

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT_SECONDS = float(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

# Pedidos
OMIE_PEDIDO_URL = os.getenv("OMIE_PEDIDO_URL", "https://app.omie.com.br/api/v1/produtos/pedido/")
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "true").lower() in ("1", "true", "yes", "y")

# NF-e
OMIE_XML_URL = os.getenv("OMIE_XML_URL", "https://app.omie.com.br/api/v1/contador/xml/")
OMIE_XML_LIST_CALL = os.getenv("OMIE_XML_LIST_CALL", "ListarDocumentos")

# Janela para busca do XML
NFE_LOOKBACK_DAYS = int(os.getenv("NFE_LOOKBACK_DAYS", "7"))
NFE_LOOKAHEAD_DAYS = int(os.getenv("NFE_LOOKAHEAD_DAYS", "0"))

if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Postgres).")

# ==================== APP / POOL =======================
router = APIRouter()

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(
        dsn=DATABASE_URL, 
        min_size=1, 
        max_size=5,
        command_timeout=60, 
        max_inactive_connection_lifetime=300
    )
    async with app.state.pool.acquire() as conn:
        await conn.execute("SELECT 1;")
        await _ensure_tables(conn)
    try:
        yield
    finally:
        await app.state.pool.close()

app = FastAPI(title="Omie Webhooks + Jobs", lifespan=lifespan)

# ==================== HELPERS ==========================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _as_json(obj: Any) -> Dict[str, Any]:
    try:
        if isinstance(obj, (dict, list)):
            return obj
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

def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v)
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d"):
        try:
            if fmt.endswith("Z") and s.endswith("Z"):
                dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            else:
                dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _date_range_by_emitida(data_emis_str: Optional[str]) -> Tuple[datetime, datetime]:
    if data_emis_str:
        d0 = _parse_dt(data_emis_str)
        if d0:
            start = (d0 - timedelta(days=1)).astimezone(timezone.utc)
            end = (d0 + timedelta(days=1)).astimezone(timezone.utc)
            return start, end
    end = _now_utc() + timedelta(days=NFE_LOOKAHEAD_DAYS)
    start = _now_utc() - timedelta(days=NFE_LOOKBACK_DAYS)
    return start, end

def _build_omie_body(call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [payload],
    }

async def _omie_post(client: httpx.AsyncClient, url: str, call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = _build_omie_body(call, payload)
    res = await client.post(url, json=body, timeout=OMIE_TIMEOUT_SECONDS)
    res.raise_for_status()
    j = res.json()
    if isinstance(j, dict) and j.get("faultstring"):
        raise RuntimeError(f"Omie error: {j.get('faultstring')}")
    return j

# ==================== DDL / AUTO-CICATRIZAÇÃO ==========
async def _ensure_tables(conn: asyncpg.Connection) -> None:
    # Eventos
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
        processed    boolean DEFAULT false,
        processed_at timestamptz,
        error        text
    );
    CREATE INDEX IF NOT EXISTS idx_owe_received_at ON public.omie_webhook_events (received_at);
    CREATE INDEX IF NOT EXISTS idx_owe_processed ON public.omie_webhook_events (processed);
    """)
    
    # Pedidos
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
    
    # NF-e
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

# ==================== HEALTH ===========================
@router.get("/healthz")
async def healthz():
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT now() AS now")
        return {"ok": True, "now": str(row["now"])}

# ==================== WEBHOOKS =========================
@router.post("/omie/webhook")
async def pedidos_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_PED:
        raise HTTPException(status_code=403, detail="invalid token")

    try:
        body = await request.json()
    except Exception:
        body = {}
    
    body_dict = _as_json(body)
    topic = _safe_text(_pick(body_dict, "topic", "TipoEvento", "evento")) or "omie_webhook_received"

    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at, processed)
            VALUES ('omie', 'omie_webhook_received', '/omie/webhook', 200, $1, NULL, $2, $3, now(), now(), FALSE);
        """, topic, json.dumps(dict(request.headers)), json.dumps(body_dict))

    return JSONResponse({"ok": True})

@router.post("/xml/omie/webhook")
async def nfe_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=403, detail="invalid token")

    try:
        body = await request.json()
    except Exception:
        body = {}
    
    body_dict = _as_json(body)
    topic = _safe_text(_pick(body_dict, "topic", "TipoEvento", "evento")) or "nfe_xml_received"

    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at, processed)
            VALUES ('omie', 'nfe_xml_received', '/xml/omie/webhook', 200, $1, NULL, $2, $3, now(), now(), FALSE);
        """, topic, json.dumps(dict(request.headers)), json.dumps(body_dict))

    return JSONResponse({"ok": True})

# ==================== EXTRAÇÃO DOS CAMPOS ==================
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

def _extract_pedido_id_from_event(ev: Dict[str, Any]) -> Optional[int]:
    cand = _pick(ev, "codigo_pedido", "codigo_pedido_omie", "idPedido", "id_pedido", "pedido_id", "id_pedido_omie", "codigoPedido", "codigo_ped")
    if cand is None and "cabecalho" in ev and isinstance(ev["cabecalho"], dict):
        cand = _pick(ev["cabecalho"], "codigo_pedido")
    if cand is None and "pedido_venda_produto" in ev and isinstance(ev["pedido_venda_produto"], dict):
        cb = ev["pedido_venda_produto"].get("cabecalho") or {}
        if isinstance(cb, dict):
            cand = _pick(cb, "codigo_pedido")
    if cand is None:
        return None
    try:
        return int(str(cand))
    except Exception:
        return None

async def _fetch_xml_por_chave(client: httpx.AsyncClient, chave: str, data_emis: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    start, end = _date_range_by_emitida(data_emis)
    payload = {
        "nPagina": 1,
        "nRegPorPagina": 50,
        "cModelo": "55",
        "dEmiInicial": start.strftime("%Y-%m-%d"),
        "dEmiFinal": end.strftime("%Y-%m-%d"),
        "nChave": chave,
    }
    
    j = await _omie_post(client, OMIE_XML_URL, OMIE_XML_LIST_CALL, payload)

    docs = []
    for key in ("documentos", "lista_documentos", "documento", "lista", "docs"):
        v = j.get(key)
        if isinstance(v, list):
            docs = v
            break
        if isinstance(v, dict):
            docs = [v]
            break

    for d in docs:
        xml_64 = d.get("cXml") or d.get("xml") or d.get("xml_base64") or d.get("xmlNFe")
        if xml_64:
            return d, str(xml_64)

    return None, None

async def _consultar_pedido(client: httpx.AsyncClient, codigo_pedido: int) -> Dict[str, Any]:
    payload = {"codigo_pedido": codigo_pedido}
    j = await _omie_post(client, OMIE_PEDIDO_URL, "ConsultarPedido", payload)
    return j

# ===================== JOBS ==============================
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")

    processed = 0
    errors = 0
    log = []

    async with app.state.pool.acquire() as conn:
        await conn.set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog', format='text')

        rows = await conn.fetch("""
            SELECT id, route, payload
            FROM public.omie_webhook_events
            WHERE processed IS NOT TRUE
            ORDER BY id ASC
            LIMIT 500;
        """)

        async with httpx.AsyncClient(timeout=OMIE_TIMEOUT_SECONDS) as client:
            for r in rows:
                ev_id = r["id"]
                route = r["route"] or ""
                payload = _as_json(r["payload"])

                try:
                    # NF-e
                    if "/xml/omie/webhook" in route:
                        chave, numero, serie, data_emis, xml_b64 = _extract_nfe_fields_from_event(payload)

                        if not xml_b64 and chave:
                            _doc, xml_b64 = await _fetch_xml_por_chave(client, chave, data_emis)

                        if not xml_b64 and not chave:
                            raise RuntimeError("NFe sem 'chave' e sem 'xml' para persistir")

                        await conn.execute("""
                            INSERT INTO public.omie_nfe_xml (chave_nfe, numero, serie, emitida_em, xml_base64)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (chave_nfe) DO UPDATE
                            SET numero = COALESCE(EXCLUDED.numero, omie_nfe_xml.numero),
                                serie = COALESCE(EXCLUDED.serie, omie_nfe_xml.serie),
                                emitida_em = COALESCE(EXCLUDED.emitida_em, omie_nfe_xml.emitida_em),
                                xml_base64 = COALESCE(EXCLUDED.xml_base64, omie_nfe_xml.xml_base64),
                                updated_at = now();
                        """, chave, numero, serie, _parse_dt(data_emis), xml_b64)

                        await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now() WHERE id=$1", ev_id)
                        processed += 1
                        log.append({"nfe_ok": ev_id})

                    # Pedidos
                    elif "/omie/webhook" in route:
                        codigo_pedido = _extract_pedido_id_from_event(payload)
                        if not codigo_pedido:
                            await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now() WHERE id=$1", ev_id)
                            processed += 1
                            continue

                        if ENRICH_PEDIDO_IMEDIATO:
                            pedido_json = await _consultar_pedido(client, codigo_pedido)
                            pv = pedido_json.get("pedido_venda_produto") or {}
                            cab = pv.get("cabecalho") or {}
                            
                            numero = _safe_text(_pick(cab, "numero_pedido", "numero"))
                            valor_total = _pick(pv.get("total_pedido") or {}, "valor_total_pedido", "valor_total")
                            situacao = _safe_text(_pick(pv.get("infoCadastro") or {}, "etapa", "situacao"))
                            
                            det = pv.get("det") or []
                            qtde_itens = len(det) if isinstance(det, list) else None

                            await conn.execute("""
                                INSERT INTO public.omie_pedido (id_pedido_omie, numero, valor_total, situacao, quantidade_itens, cliente_codigo, detalhe)
                                VALUES ($1, $2, $3, $4, $5, $6, $7)
                                ON CONFLICT (id_pedido_omie) DO UPDATE
                                SET numero = COALESCE(EXCLUDED.numero, omie_pedido.numero),
                                    valor_total = COALESCE(EXCLUDED.valor_total, omie_pedido.valor_total),
                                    situacao = COALESCE(EXCLUDED.situacao, omie_pedido.situacao),
                                    quantidade_itens = COALESCE(EXCLUDED.quantidade_itens, omie_pedido.quantidade_itens),
                                    cliente_codigo = COALESCE(EXCLUDED.cliente_codigo, omie_pedido.cliente_codigo),
                                    detalhe = COALESCE(EXCLUDED.detalhe, omie_pedido.detalhe),
                                    updated_at = now();
                            """, codigo_pedido, numero, valor_total, situacao, qtde_itens, 
                                _safe_text(_pick(cab, "codigo_cliente")), json.dumps(pv))

                        await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now() WHERE id=$1", ev_id)
                        processed += 1
                        log.append({"pedido_ok": ev_id})

                except Exception as ex:
                    errors += 1
                    await conn.execute("UPDATE public.omie_webhook_events SET error=$1 WHERE id=$2", str(ex), ev_id)
                    log.append({"error": str(ex), "id": ev_id})

    return JSONResponse({"ok": True, "processed": processed, "errors": errors, "events": log})

# ================= MOUNT & MAIN =========================
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)