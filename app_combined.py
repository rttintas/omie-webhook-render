# app_combined.py — serviço único (webhooks + jobs) para Omie (Pedidos + NF-e)

import os
import json
import logging
import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL")

WEBHOOK_TOKEN_PED = os.getenv("OMIE_WEBHOOK_TOKEN", "t1")
WEBHOOK_TOKEN_XML = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "t2")
ADMIN_RUNJOBS_SECRET = os.getenv("ADMIN_RUNJOBS_SECRET", "julia-matheus")

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT_SECONDS = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

OMIE_PEDIDO_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_XML_URL = "https://app.omie.com.br/api/v1/contador/xml/"
OMIE_XML_LIST_CALL = "ListarDocumentos"

ENRICH_PEDIDO_IMEDIATO = True
NFE_LOOKBACK_DAYS = 7
NFE_LOOKAHEAD_DAYS = 1

# ------------------------------------------------------------------------------
# App
# ------------------------------------------------------------------------------
router = APIRouter()

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        async with app.state.pool.acquire() as conn:
            await _ensure_tables(conn)
        logger.info("✅ DB conectado e tabelas verificadas")
        yield
    finally:
        if hasattr(app.state, "pool"):
            await app.state.pool.close()
            logger.info("✅ DB pool fechado")

app = FastAPI(title="Omie Webhooks + Jobs", lifespan=lifespan)

# ------------------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _as_json(obj: Any) -> Dict[str, Any]:
    try:
        if isinstance(obj, dict): return obj
        if isinstance(obj, str): return json.loads(obj)
    except Exception as e:
        logger.error(f"Erro ao converter para JSON: {e}")
    return {}

def _safe_text(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    return s or None

def _parse_dt(v: Any) -> Optional[datetime]:
    if not v: return None
    if isinstance(v, datetime): return v
    try:
        # Omie costuma mandar ISO com timezone (ex.: 2025-09-06T00:00:00-03:00)
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None

def _date_range_for_omie(data_emis: Optional[datetime]) -> Tuple[datetime, datetime]:
    if data_emis:
        start = (data_emis - timedelta(days=1)).astimezone(timezone.utc)
        end   = (data_emis + timedelta(days=2)).astimezone(timezone.utc)
    else:
        end   = _now_utc() + timedelta(days=NFE_LOOKAHEAD_DAYS)
        start = _now_utc() - timedelta(days=NFE_LOOKBACK_DAYS)
    return start, end

def _build_omie_body(call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"call": call, "app_key": OMIE_APP_KEY, "app_secret": OMIE_APP_SECRET, "param": [payload]}

async def _omie_post(client: httpx.AsyncClient, url: str, call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = _build_omie_body(call, payload)
    res = await client.post(url, json=body, timeout=OMIE_TIMEOUT_SECONDS)
    res.raise_for_status()
    j = res.json()
    if isinstance(j, dict) and j.get("faultstring"):
        raise RuntimeError(f"Omie error: {j.get('faultstring')}")
    return j

def _pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    if not isinstance(d, dict): return None
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

# ------------------------------------------------------------------------------
# DDL
# ------------------------------------------------------------------------------
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
        processed    boolean DEFAULT false,
        processed_at timestamptz,
        error        text
    );""")

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
    );""")

    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe (
        chave_nfe         text PRIMARY KEY,
        numero            text,
        serie             text,
        emitida_em        timestamptz,
        cnpj_emitente     text,
        cnpj_destinatario text,
        valor_total       numeric(15,2),
        status            text,
        xml               text,
        xml_url           text,
        danfe_url         text,
        last_event_at     timestamptz,
        updated_at        timestamptz,
        recebido_em       timestamptz DEFAULT now(),
        raw               jsonb
    );""")

    await conn.execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='idx_omie_nfe_chave_unique'
      ) THEN
        BEGIN
          CREATE UNIQUE INDEX idx_omie_nfe_chave_unique ON public.omie_nfe (chave_nfe);
        EXCEPTION WHEN duplicate_table THEN
        END;
      END IF;
    END$$;""")

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
    );""")

# ------------------------------------------------------------------------------
# HEALTH
# ------------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    try:
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT now() AS now, count(*) AS pend FROM public.omie_webhook_events WHERE processed=false")
            return {"ok": True, "now": str(row["now"]), "pending_events": row["pend"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ------------------------------------------------------------------------------
# WEBHOOKS
# ------------------------------------------------------------------------------
@router.post("/omie/webhook")
async def pedidos_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_PED:
        raise HTTPException(status_code=403, detail="invalid token")
    try:
        body = await request.json()
    except Exception:
        body = {}
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source,event_type,route,http_status,topic,raw_headers,payload,event_ts,received_at,processed)
            VALUES ('omie','omie_webhook_received','/omie/webhook',200,$1,$2,$3,now(),now(),FALSE);
        """,
        _safe_text(_pick(body, "topic")) or "pedido",
        json.dumps(dict(request.headers), ensure_ascii=False, default=str),
        json.dumps(body, ensure_ascii=False, default=str))
    return JSONResponse({"ok": True, "received": True})

@router.post("/xml/omie/webhook")
async def nfe_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=403, detail="invalid token")
    try:
        body = await request.json()
    except Exception:
        body = {}
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source,event_type,route,http_status,topic,raw_headers,payload,event_ts,received_at,processed)
            VALUES ('omie','nfe_xml_received','/xml/omie/webhook',200,$1,$2,$3,now(),now(),FALSE);
        """,
        _safe_text(_pick(body, "topic")) or "nfe",
        json.dumps(dict(request.headers), ensure_ascii=False, default=str),
        json.dumps(body, ensure_ascii=False, default=str))
    return JSONResponse({"ok": True, "received": True})

# ------------------------------------------------------------------------------
# NF-e helpers
# ------------------------------------------------------------------------------
async def _buscar_links_xml_via_api(client: httpx.AsyncClient, chave: str, data_emis: Optional[datetime]) -> Tuple[Optional[str], Optional[str]]:
    """
    Fallback: consulta a API Contador/XML → ListarDocumentos e tenta achar XML/DANFE pela chave.
    O schema do Omie varia; por isso procuramos de forma tolerante.
    """
    inicio, fim = _date_range_for_omie(data_emis)
    payload = {
        # Estes nomes são comuns; se sua conta usar outros campos, ainda assim tentaremos achar nos resultados.
        "pagina": 1,
        "registros_por_pagina": 100,
        "ordenar_por": "dhEmissao",
        "ordem_desc": "N",
        "filtro": {
            "dhEmissaoInicial": inicio.isoformat(),
            "dhEmissaoFinal":   fim.isoformat(),
            "chaveNFe": chave
        }
    }
    try:
        logger.info("🔎 Buscando XML via Contador/XML.ListarDocumentos …")
        resp = await _omie_post(client, OMIE_XML_URL, OMIE_XML_LIST_CALL, payload)
    except Exception as e:
        logger.warning(f"⚠️ Falha ao consultar Contador/XML: {e}")
        return None, None

    # Tenta localizar listas dentro do retorno (documentos, lista, etc.)
    candidates = []
    if isinstance(resp, dict):
        for v in resp.values():
            if isinstance(v, list):
                candidates.extend(v)

    xml_url = None
    danfe_url = None

    def _val(d: Dict[str, Any], *names: str) -> Optional[str]:
        for n in names:
            if n in d and d[n]:
                return str(d[n])
        return None

    for doc in candidates:
        if not isinstance(doc, dict):
            continue
        ch = _val(doc, "chave", "chaveNFe", "chave_nfe", "nfe_chave")
        if ch and ch.strip() == chave:
            xml_url  = _val(doc, "xml_url", "url_xml", "nfe_xml", "link_xml")
            danfe_url = _val(doc, "danfe_url", "url_danfe", "nfe_danfe", "link_danfe")
            if xml_url:
                break

    if not xml_url:
        logger.warning("⚠️  Não encontrei link do XML na listagem.")
    else:
        logger.info("✅ Link do XML localizado via API.")
    return xml_url, danfe_url

async def processar_nfe(conn: asyncpg.Connection, payload: Dict[str, Any], client: httpx.AsyncClient) -> bool:
    """
    Processa uma NF-e e salva no banco.
    """
    try:
        event = payload.get("event", {}) or {}
        # campos possíveis
        chave     = _safe_text(_pick(event, "nfe_chave", "chave", "chaveNFe", "chave_nfe"))
        numero_nf = _safe_text(_pick(event, "id_nf", "numero", "numero_nfe"))
        serie     = _safe_text(_pick(event, "serie"))
        data_emis = _parse_dt(_pick(event, "data_emis", "dh_emis", "dhEmissao"))
        xml_url   = _safe_text(_pick(event, "nfe_xml", "xml_url", "url_xml"))
        danfe_url = _safe_text(_pick(event, "nfe_danfe", "danfe_url", "url_danfe"))
        cnpj_emit = _safe_text(_pick(event, "empresa_cnpj", "cnpj_emitente"))
        status    = _safe_text(_pick(event, "acao", "status"))

        logger.info(f"🔍 Processando NF-e: Chave={chave}, Número={numero_nf}")

        if not chave:
            logger.warning("❌ NF-e sem chave")
            return False

        if not xml_url:
            logger.warning("⚠️  NF-e sem URL do XML — tentando buscar via Contador/XML …")
            xml_url_api, danfe_url_api = await _buscar_links_xml_via_api(client, chave, data_emis)
            xml_url = xml_url or xml_url_api
            danfe_url = danfe_url or danfe_url_api

        if not xml_url:
            logger.warning("❌ NF-e segue sem URL do XML — não será possível salvar.")
            return False

        # Baixa XML
        try:
            r = await client.get(xml_url, timeout=OMIE_TIMEOUT_SECONDS)
            r.raise_for_status()
            xml_text = r.text
            xml_base64 = base64.b64encode(r.content).decode("utf-8")
            logger.info(f"✅ XML baixado com sucesso para NF-e {chave}")
        except Exception as e:
            logger.error(f"❌ Erro ao baixar XML: {e}")
            return False

        # UPSERT principal
        await conn.execute("""
            INSERT INTO public.omie_nfe
                (chave_nfe, numero, serie, emitida_em, cnpj_emitente, status, xml, xml_url, danfe_url, last_event_at, updated_at, recebido_em, raw)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),now(),now(),$10)
            ON CONFLICT (chave_nfe) DO UPDATE SET
                numero        = EXCLUDED.numero,
                serie         = COALESCE(EXCLUDED.serie, public.omie_nfe.serie),
                emitida_em    = COALESCE(EXCLUDED.emitida_em, public.omie_nfe.emitida_em),
                cnpj_emitente = COALESCE(EXCLUDED.cnpj_emitente, public.omie_nfe.cnpj_emitente),
                status        = COALESCE(EXCLUDED.status, public.omie_nfe.status),
                xml           = EXCLUDED.xml,
                xml_url       = EXCLUDED.xml_url,
                danfe_url     = COALESCE(EXCLUDED.danfe_url, public.omie_nfe.danfe_url),
                last_event_at = now(),
                updated_at    = now(),
                raw           = EXCLUDED.raw;
        """,
        chave, numero_nf, serie, data_emis, cnpj_emit, status, xml_text, xml_url, danfe_url,
        json.dumps(payload, ensure_ascii=False, default=str))

        # cópia base64
        await conn.execute("""
            INSERT INTO public.omie_nfe_xml (chave_nfe, numero, serie, emitida_em, xml_base64, recebido_em, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,now(),now(),now())
            ON CONFLICT (chave_nfe) DO UPDATE SET
                numero     = EXCLUDED.numero,
                serie      = COALESCE(EXCLUDED.serie, public.omie_nfe_xml.serie),
                emitida_em = COALESCE(EXCLUDED.emitida_em, public.omie_nfe_xml.emitida_em),
                xml_base64 = EXCLUDED.xml_base64,
                updated_at = now();
        """,
        chave, numero_nf, serie, data_emis, xml_base64)

        logger.info(f"✅ NF-e {chave} salva no banco")
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao processar NF-e: {e}")
        return False

# ------------------------------------------------------------------------------
# Pedidos helpers
# ------------------------------------------------------------------------------
def _extract_pedido_id_from_event(payload: Dict[str, Any]) -> Optional[int]:
    try:
        event_data = payload.get("event", {}) or {}
        val = _pick(event_data, "idPedido", "codigo_pedido", "id_pedido", "pedido_id", "nCodPed")
        if val is None: return None
        return int(str(val).strip())
    except Exception:
        return None

async def _consultar_pedido(client: httpx.AsyncClient, codigo_pedido: int) -> Dict[str, Any]:
    payload = {"codigo_pedido": codigo_pedido}
    return await _omie_post(client, OMIE_PEDIDO_URL, "ConsultarPedido", payload)

# ------------------------------------------------------------------------------
# Run Jobs
# ------------------------------------------------------------------------------
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    processed = 0
    errors = 0
    entries = []

    try:
        async with app.state.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, route, payload
                FROM public.omie_webhook_events
                WHERE processed = false
                ORDER BY id ASC
                LIMIT 100
            """)
            logger.info(f"🔍 Encontrados {len(rows)} eventos para processamento")

            async with httpx.AsyncClient(timeout=OMIE_TIMEOUT_SECONDS) as client:
                for r in rows:
                    ev_id = r["id"]
                    route = r["route"] or ""
                    payload = _as_json(r["payload"])

                    try:
                        logger.info(f"⚡ Processando evento {ev_id} da rota {route}")

                        if "/xml/omie/webhook" in route:
                            ok = await processar_nfe(conn, payload, client)
                            if ok:
                                await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                                processed += 1
                                entries.append({"nfe_ok": ev_id})
                            else:
                                errors += 1
                                entries.append({"nfe_error": ev_id})
                            continue

                        if "/omie/webhook" in route:
                            codigo_pedido = _extract_pedido_id_from_event(payload)
                            if not codigo_pedido:
                                logger.warning(f"⚠️  Evento {ev_id} sem código de pedido")
                                await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                                processed += 1
                                entries.append({"pedido_skip": ev_id})
                                continue

                            if ENRICH_PEDIDO_IMEDIATO:
                                try:
                                    pedido_data = await _consultar_pedido(client, codigo_pedido)
                                    pedido_venda = pedido_data.get("pedido_venda_produto", {})
                                    cab = pedido_venda.get("cabecalho", {}) or {}

                                    numero = _safe_text(_pick(cab, "numero_pedido", "numero"))
                                    valor_total = _pick(cab, "valor_total", "valor_mercadorias")
                                    situacao = _safe_text(_pick(cab, "etapa", "situacao"))
                                    cliente_codigo = _safe_text(_pick(cab, "codigo_cliente"))

                                    det = pedido_venda.get("det", [])
                                    quantidade_itens = len(det) if isinstance(det, list) else 0

                                    await conn.execute("""
                                        INSERT INTO public.omie_pedido
                                            (id_pedido_omie, numero, valor_total, situacao, quantidade_itens, cliente_codigo, detalhe, created_at, updated_at)
                                        VALUES ($1,$2,$3,$4,$5,$6,$7,now(),now())
                                        ON CONFLICT (id_pedido_omie) DO UPDATE SET
                                            numero           = COALESCE(EXCLUDED.numero, omie_pedido.numero),
                                            valor_total      = COALESCE(EXCLUDED.valor_total, omie_pedido.valor_total),
                                            situacao         = COALESCE(EXCLUDED.situacao, omie_pedido.situacao),
                                            quantidade_itens = COALESCE(EXCLUDED.quantidade_itens, omie_pedido.quantidade_itens),
                                            cliente_codigo   = COALESCE(EXCLUDED.cliente_codigo, omie_pedido.cliente_codigo),
                                            detalhe          = COALESCE(EXCLUDED.detalhe, omie_pedido.detalhe),
                                            updated_at       = now();
                                    """,
                                    codigo_pedido, numero, valor_total, situacao,
                                    quantidade_itens, cliente_codigo,
                                    json.dumps(pedido_venda, ensure_ascii=False, default=str))

                                    logger.info(f"✅ Pedido {codigo_pedido} salvo/atualizado")

                                except Exception as e:
                                    logger.error(f"❌ Erro ao enriquecer pedido {codigo_pedido}: {e}")

                            await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                            processed += 1
                            entries.append({"pedido_ok": codigo_pedido, "event_id": ev_id})
                            continue

                        # rota desconhecida
                        await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                        processed += 1
                        entries.append({"evento_outro": ev_id, "rota": route})

                    except Exception as e:
                        errors += 1
                        err = f"{type(e).__name__}: {e}"
                        await conn.execute("UPDATE public.omie_webhook_events SET error=$1 WHERE id=$2", err, ev_id)
                        logger.error(f"❌ Erro no evento {ev_id}: {err}")

    except Exception as e:
        logger.error(f"❌ Erro geral no run-jobs: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    logger.info(f"✅ Processamento concluído: {processed} processados, {errors} erros")
    return JSONResponse({"ok": True, "processed": processed, "errors": errors, "events": entries})

# ------------------------------------------------------------------------------
# DEBUG
# ------------------------------------------------------------------------------
@router.get("/admin/debug/events")
async def debug_events(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")
    async with app.state.pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, route, processed, error, received_at, payload::text AS payload_text
            FROM public.omie_webhook_events
            ORDER BY id DESC
            LIMIT 20
        """)
        pedidos = await conn.fetch("SELECT * FROM public.omie_pedido ORDER BY created_at DESC LIMIT 10")
        nfes = await conn.fetch("SELECT * FROM public.omie_nfe ORDER BY recebido_em DESC LIMIT 10")
    return JSONResponse({
        "events": [dict(e) for e in events],
        "pedidos": [dict(p) for p in pedidos],
        "nfes": [dict(n) for n in nfes],
    })

@router.get("/admin/debug/tokens")
async def debug_tokens(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")
    return JSONResponse({
        "WEBHOOK_TOKEN_PED": WEBHOOK_TOKEN_PED,
        "WEBHOOK_TOKEN_XML": WEBHOOK_TOKEN_XML,
        "OMIE_APP_KEY": "CONFIGURADO" if OMIE_APP_KEY else "NAO",
        "OMIE_APP_SECRET": "CONFIGURADO" if OMIE_APP_SECRET else "NAO",
        "DATABASE_URL": "CONFIGURADO" if DATABASE_URL else "NAO",
    })

# ------------------------------------------------------------------------------
# Mount
# ------------------------------------------------------------------------------
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
