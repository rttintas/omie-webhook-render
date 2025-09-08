# app_combined.py — serviço único (webhooks + jobs) para Omie (Pedidos + NF-e)

import os
import json
import logging
import base64
import asyncio
import time
import re
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
logger = logging.getLogger("app_combined")

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

# Rate limiting
OMIE_RATE_LIMIT_DELAY = 3.0  # segundos entre requisições
LAST_API_CALL_TIME = 0

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
        # Tenta parsear datas no formato "DD/MM/YYYY" que podem vir da API
        if "/" in str(v):
            return datetime.strptime(str(v), "%d/%m/%Y")
        # Tenta parsear datas no formato ISO (padrão de webhooks)
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None

def _date_range_for_omie(data_emis: Optional[datetime]) -> Tuple[str, str]:
    """
    Retorna datas no formato DD/MM/YYYY para a API Omie.
    Usa um intervalo de 3 dias para garantir a captura da nota em caso de latência.
    """
    if data_emis:
        start = (data_emis - timedelta(days=3)).strftime("%d/%m/%Y")
        end = (data_emis + timedelta(days=3)).strftime("%d/%m/%Y")
    else:
        end = (_now_utc() + timedelta(days=NFE_LOOKAHEAD_DAYS)).strftime("%d/%m/%Y")
        start = (_now_utc() - timedelta(days=NFE_LOOKBACK_DAYS)).strftime("%d/%m/%Y")
    return start, end

def _build_omie_body(call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"call": call, "app_key": OMIE_APP_KEY, "app_secret": OMIE_APP_SECRET, "param": [payload]}

def _extract_retry_time(error_message: str) -> int:
    """Extrai o tempo de espera da mensagem de erro da Omie"""
    match = re.search(r"em (\d+) segundos", error_message)
    if match:
        return int(match.group(1))
    return 120  # Fallback: 2 minutos

async def _omie_post_with_retry(client: httpx.AsyncClient, url: str, call: str, payload: Dict[str, Any], max_retries: int = 2) -> Dict[str, Any]:
    """
    Faz requisições para API Omie com rate limiting inteligente
    """
    global LAST_API_CALL_TIME
    
    for attempt in range(max_retries):
        try:
            current_time = time.time()
            elapsed = current_time - LAST_API_CALL_TIME
            if elapsed < OMIE_RATE_LIMIT_DELAY:
                wait_time = OMIE_RATE_LIMIT_DELAY - elapsed
                await asyncio.sleep(wait_time)
            
            LAST_API_CALL_TIME = time.time()
            
            body = _build_omie_body(call, payload)
            logger.info(f"📤 Request to {url} (attempt {attempt + 1}/{max_retries})")
            
            res = await client.post(url, json=body, timeout=OMIE_TIMEOUT_SECONDS)
            
            logger.info(f"📥 Response status: {res.status_code}")
            
            if res.status_code == 425:
                error_data = res.json()
                retry_after = _extract_retry_time(error_data.get("faultstring", ""))
                logger.warning(f"⏳ API bloqueada. Retentando em {retry_after} segundos...")
                await asyncio.sleep(retry_after)
                continue
                
            res.raise_for_status()
            return res.json()
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 425 and attempt < max_retries - 1:
                error_data = e.response.json()
                retry_after = _extract_retry_time(error_data.get("faultstring", ""))
                logger.warning(f"⏳ API bloqueada (425). Tentativa {attempt + 1}/{max_retries}")
                await asyncio.sleep(retry_after)
                continue
            else:
                error_detail = f"HTTP error {e.response.status_code}: {e.response.text}"
                logger.error(f"❌ HTTP error details: {error_detail}")
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Erro na tentativa {attempt + 1}: {e}")
                await asyncio.sleep(10 * (attempt + 1))
                continue
            else:
                logger.error(f"❌ Unexpected error in _omie_post: {e}")
                raise
    
    raise Exception("Todas as tentativas falharam")

def _pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    if not isinstance(d, dict): return None
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _get_value(doc, *possible_keys):
    """Função auxiliar para extrair valores com chaves alternativas"""
    if not isinstance(doc, dict):
        return None
    for key in possible_keys:
        if key in doc and doc[key] not in (None, "", 0):
            return str(doc[key]).strip()
    return None

# ------------------------------------------------------------------------------
# DDL
# ------------------------------------------------------------------------------
async def _ensure_tables(conn: asyncpg.Connection) -> None:
    # Tabela de eventos de webhook (fila)
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

    # Tabela para Pedidos de Venda
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

    # Tabela para Notas Fiscais (NF-e)
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
    logger.info(f"📥 Webhook de Pedido recebido com token: {token}")
    if token != WEBHOOK_TOKEN_PED:
        logger.error(f"❌ Token inválido: Recebido '{token}', Esperado '{WEBHOOK_TOKEN_PED}'")
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

# ===== FUNÇÃO CORRIGIDA E LIMPA =====
async def processar_nfe(conn: asyncpg.Connection, payload: Dict[str, Any], client: httpx.AsyncClient) -> bool:
    """
    Processa um evento de NF-e, busca os dados na API Omie e salva cada
    documento na tabela 'omie_nfe' com as colunas corretas.
    """
    try:
        event_payload = payload.get("event", {}) or {}
        logger.info("🔍 Processando gatilho de NF-e...")

        # Tenta pegar data do webhook, senão, extrai da chave como plano B
        data_evento = _parse_dt(_pick(event_payload, "data_emis", "dh_emis", "dEmissao"))
        chave_webhook = _safe_text(_pick(event_payload, "nfe_chave", "chave", "chaveNFe"))

        if not data_evento and chave_webhook:
            try:
                ano = int(chave_webhook[2:4])
                mes = int(chave_webhook[4:6])
                data_evento = datetime(2000 + ano, mes, 15)
                logger.info(f"📅 Usando data extraída da chave: {data_evento.strftime('%d/%m/%Y')}")
            except Exception:
                pass

        # Monta a chamada para buscar o bloco de dados na API
        dEmiInicial, dEmiFinal = _date_range_for_omie(data_evento)
        
        api_payload = {
            "nPagina": 1, "nRegPorPagina": 100, "cModelo": "55",
            "dEmiInicial": dEmiInicial, "dEmiFinal": dEmiFinal
        }

        logger.info(f"📄 Consultando API Omie para NF-e no período de {dEmiInicial} a {dEmiFinal}...")
        response_data = await _omie_post_with_retry(client, OMIE_XML_URL, "ListarDocumentos", api_payload)

        documentos = response_data.get("documentosEncontrados", [])
        if not documentos:
            logger.warning("⚠️ Nenhum documento NF-e encontrado na API para o período.")
            return True # Sucesso, pois não há o que processar

        logger.info(f"✅ Encontrados {len(documentos)} documentos. Salvando no banco...")

        # Loop para salvar CADA documento encontrado
        for doc in documentos:
            chave_nfe = _get_value(doc, "nChave", "chaveNFe", "chave_nfe")
            if not chave_nfe:
                continue # Pula item sem chave

            # Mapeamento para as colunas da sua tabela
            numero_nf = _get_value(doc, "nNumero")
            serie_nf = _get_value(doc, "cSerie")
            status_nf = _get_value(doc, "cStatus")
            valor_nf = doc.get("nValor")
            data_emissao_nf = _parse_dt(doc.get("dEmissao")) # A API retorna "dEmissao"
            xml_str = doc.get("cXml")
            json_completo_str = json.dumps(doc, ensure_ascii=False, default=str)
            
            # A API ListarDocumentos não retorna URLs, então salvamos como Nulo
            danfe_url_nf = None
            xml_url_nf = None
            
            await conn.execute("""
                INSERT INTO public.omie_nfe (
                    chave_nfe, numero, serie, status, valor_total, emitida_em,
                    xml, danfe_url, xml_url, raw, last_event_at, updated_at, recebido_em
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), now(), now())
                ON CONFLICT (chave_nfe) DO UPDATE SET
                    numero = EXCLUDED.numero,
                    serie = EXCLUDED.serie,
                    status = EXCLUDED.status,
                    valor_total = EXCLUDED.valor_total,
                    emitida_em = EXCLUDED.emitida_em,
                    xml = EXCLUDED.xml,
                    danfe_url = COALESCE(EXCLUDED.danfe_url, omie_nfe.danfe_url),
                    xml_url = COALESCE(EXCLUDED.xml_url, omie_nfe.xml_url),
                    raw = EXCLUDED.raw,
                    updated_at = now();
            """, chave_nfe, numero_nf, serie_nf, status_nf, valor_nf, data_emissao_nf,
                 xml_str, danfe_url_nf, xml_url_nf, json_completo_str)
            
            logger.info(f"✔️ NF-e Chave: {chave_nfe} (Número: {numero_nf}) salva/atualizada na tabela omie_nfe.")

        return True

    except Exception as e:
        logger.error(f"❌ Erro fatal ao processar e gravar NF-e: {e}", exc_info=True)
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
    return await _omie_post_with_retry(client, OMIE_PEDIDO_URL, "ConsultarPedido", payload)

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
                                # Marca o evento como processado para não tentar de novo infinitamente
                                await conn.execute("UPDATE public.omie_webhook_events SET error=$1, processed=true, processed_at=now() WHERE id=$2", 'Erro no processar_nfe', ev_id)
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
                                    """, codigo_pedido, numero, valor_total, situacao,
                                    quantidade_itens, cliente_codigo,
                                    json.dumps(pedido_venda, ensure_ascii=False, default=str))

                                    logger.info(f"✅ Pedido {codigo_pedido} salvo/atualizado")

                                except Exception as e:
                                    logger.error(f"❌ Erro ao enriquecer pedido {codigo_pedido}: {e}")

                            await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                            processed += 1
                            entries.append({"pedido_ok": codigo_pedido, "event_id": ev_id})
                            continue

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
        "OMIE_APP_KEY": "CONFIGURADO" if OMIE_APP_KEY else "NÃO CONFIGURADO",
        "OMIE_APP_SECRET": "CONFIGURADO" if OMIE_APP_SECRET else "NÃO CONFIGURADO",
        "DATABASE_URL": "CONFIGURADO" if DATABASE_URL else "NÃO CONFIGURADO"
    })

# ------------------------------------------------------------------------------
# Mount
# ------------------------------------------------------------------------------
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))