# app_combined.py — serviço único (webhooks + jobs) para Omie (Pedidos + NF-e)
import os
import json
import logging
import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# Configuração
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variáveis de ambiente
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL")

WEBHOOK_TOKEN_PED = os.getenv("OMIE_WEBHOOK_TOKEN", "t1")
WEBHOOK_TOKEN_XML = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "t2")
ADMIN_RUNJOBS_SECRET = os.getenv("ADMIN_RUNJOBS_SECRET", "julia-matheus")

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT_SECONDS = 30

# URLs da API
OMIE_PEDIDO_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_XML_URL = "https://app.omie.com.br/api/v1/contador/xml/"
OMIE_XML_LIST_CALL = "ListarDocumentos"

# Configurações
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
        logger.info("✅ Database pool conectado e tabelas verificadas")
        yield
    finally:
        if hasattr(app.state, 'pool'):
            await app.state.pool.close()
            logger.info("✅ Database pool fechado")

app = FastAPI(title="Omie Webhooks + Jobs", lifespan=lifespan)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _as_json(obj: Any) -> Dict[str, Any]:
    try:
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, str):
            return json.loads(obj)
        return {}
    except Exception as e:
        logger.error(f"Erro ao converter para JSON: {e}")
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
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None

def _date_range_by_emitida(data_emis_str: Optional[str]) -> Tuple[datetime, datetime]:
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
    try:
        body = _build_omie_body(call, payload)
        res = await client.post(url, json=body, timeout=OMIE_TIMEOUT_SECONDS)
        res.raise_for_status()
        j = res.json()
        if isinstance(j, dict) and j.get("faultstring"):
            raise RuntimeError(f"Omie error: {j.get('faultstring')}")
        return j
    except Exception as e:
        logger.error(f"Erro na requisição Omie: {e}")
        raise

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
    );
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
# HEALTH
# ------------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    try:
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT now() AS now, count(*) as events FROM public.omie_webhook_events WHERE processed = false")
            return {
                "ok": True, 
                "now": str(row["now"]),
                "pending_events": row["events"]
            }
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
        logger.info(f"📥 Webhook pedido recebido")
    except Exception as e:
        logger.error(f"Erro ao ler JSON: {e}")
        body = {}
    
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, raw_headers, payload, event_ts, received_at, processed)
            VALUES ('omie', 'omie_webhook_received', '/omie/webhook', 200, $1, $2, $3, now(), now(), FALSE);
        """, "pedido_received", json.dumps(dict(request.headers)), json.dumps(body))

    return JSONResponse({"ok": True, "received": True})

@router.post("/xml/omie/webhook")
async def nfe_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=403, detail="invalid token")

    try:
        body = await request.json()
        logger.info(f"📥 Webhook NF-e recebido")
    except Exception as e:
        logger.error(f"Erro ao ler JSON NF-e: {e}")
        body = {}
    
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, raw_headers, payload, event_ts, received_at, processed)
            VALUES ('omie', 'nfe_xml_received', '/xml/omie/webhook', 200, $1, $2, $3, now(), now(), FALSE);
        """, "nfe_received", json.dumps(dict(request.headers)), json.dumps(body))

    return JSONResponse({"ok": True, "received": True})

# ------------------------------------------------------------------------------
# Processamento de NF-e (CORREÇÃO CRÍTICA - AJUSTADO)
# ------------------------------------------------------------------------------
async def processar_nfe(conn: asyncpg.Connection, payload: Dict[str, Any]) -> bool:
    """
    Processa uma NF-e e salva no banco de dados - CORRIGIDO
    """
    try:
        # Extrai campos da NF-e do payload
        event_data = payload.get("event", {})
        chave = event_data.get("nfe_chave")
        numero = event_data.get("id_nf")
        data_emis = event_data.get("data_emis")
        xml_url = event_data.get("nfe_xml")
        
        logger.info(f"🔍 Processando NF-e: Chave={chave}, Número={numero}")
        
        if not chave:
            logger.warning("❌ NF-e sem chave")
            return False
        
        if not xml_url:
            logger.warning("❌ NF-e sem URL do XML")
            return False
        
        # Baixa o XML
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(xml_url, timeout=30)
                if response.status_code == 200:
                    xml_base64 = base64.b64encode(response.content).decode('utf-8')
                    logger.info(f"✅ XML baixado com sucesso para NF-e {chave}")
                else:
                    logger.error(f"❌ Erro ao baixar XML: {response.status_code}")
                    return False
        except Exception as e:
            logger.error(f"❌ Erro ao baixar XML da NF-e {chave}: {e}")
            return False
        
        # Salva no banco - CORREÇÃO: converter numero para string
        await conn.execute("""
            INSERT INTO public.omie_nfe_xml 
            (chave_nfe, numero, emitida_em, xml_base64, recebido_em, created_at, updated_at)
            VALUES ($1, $2, $3, $4, now(), now(), now())
            ON CONFLICT (chave_nfe) DO UPDATE SET
            numero = EXCLUDED.numero,
            emitida_em = EXCLUDED.emitida_em,
            xml_base64 = EXCLUDED.xml_base64,
            updated_at = now()
        """, chave, str(numero), _parse_dt(data_emis), xml_base64)  # ← str(numero) aqui!
        
        logger.info(f"✅ NF-e {chave} salva no banco")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar NF-e: {e}")
        return False

# ------------------------------------------------------------------------------
# Extração de campos (Pedido)
# ------------------------------------------------------------------------------
def _extract_pedido_id_from_event(payload: Dict[str, Any]) -> Optional[int]:
    """
    Extrai o ID do pedido do payload - CORRIGIDO
    """
    try:
        # O ID do pedido está em event->idPedido
        event_data = payload.get("event", {})
        id_pedido = event_data.get("idPedido")
        
        if id_pedido:
            return int(id_pedido)
        
        # Tenta outras chaves possíveis
        keys_to_try = ["codigo_pedido", "id_pedido", "pedido_id", "nCodPed"]
        for key in keys_to_try:
            value = event_data.get(key)
            if value:
                return int(value)
                
        logger.warning(f"❌ Não foi possível extrair código do pedido")
        return None
        
    except Exception as e:
        logger.error(f"❌ Erro ao extrair ID do pedido: {e}")
        return None

async def _consultar_pedido(client: httpx.AsyncClient, codigo_pedido: int) -> Dict[str, Any]:
    try:
        payload = {"codigo_pedido": codigo_pedido}
        result = await _omie_post(client, OMIE_PEDIDO_URL, "ConsultarPedido", payload)
        logger.info(f"✅ Pedido {codigo_pedido} consultado com sucesso")
        return result
    except Exception as e:
        logger.error(f"❌ Erro ao consultar pedido {codigo_pedido}: {e}")
        raise

# ------------------------------------------------------------------------------
# Run Jobs (cron) - CORRIGIDO
# ------------------------------------------------------------------------------
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    processed = 0
    errors = 0
    log = []

    try:
        async with app.state.pool.acquire() as conn:
            # Buscar eventos não processados
            rows = await conn.fetch("""
                SELECT id, route, payload
                FROM public.omie_webhook_events
                WHERE processed = false
                ORDER BY id ASC
                LIMit 100;
            """)
            
            logger.info(f"🔍 Encontrados {len(rows)} eventos para processar")
            
            if not rows:
                return JSONResponse({"ok": True, "processed": 0, "message": "Nenhum evento pendente"})

            async with httpx.AsyncClient(timeout=OMIE_TIMEOUT_SECONDS) as client:
                for r in rows:
                    ev_id = r["id"]
                    route = r["route"] or ""
                    payload = _as_json(r["payload"])
                    
                    try:
                        logger.info(f"⚡ Processando evento {ev_id} da rota {route}")
                        
                        # Processar NF-e
                        if "/xml/omie/webhook" in route:
                            logger.info(f"📄 Processando NF-e do evento {ev_id}")
                            success = await processar_nfe(conn, payload)
                            if success:
                                await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                                processed += 1
                                log.append({"nfe_ok": ev_id})
                            else:
                                errors += 1
                                log.append({"nfe_error": ev_id})
                            continue
                            
                        # Processar pedidos
                        if "/omie/webhook" in route:
                            codigo_pedido = _extract_pedido_id_from_event(payload)
                            
                            if not codigo_pedido:
                                logger.warning(f"⚠️  Evento {ev_id} sem código de pedido válido")
                                await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                                processed += 1
                                log.append({"pedido_skip_sem_codigo": ev_id})
                                continue
                            
                            logger.info(f"📦 Processando pedido {codigo_pedido}")
                            
                            if ENRICH_PEDIDO_IMEDIATO:
                                try:
                                    pedido_data = await _consultar_pedido(client, codigo_pedido)
                                    pedido_venda = pedido_data.get("pedido_venda_produto", {})
                                    cabecalho = pedido_venda.get("cabecalho", {})
                                    
                                    numero = _safe_text(_pick(cabecalho, "numero_pedido", "numero"))
                                    valor_total = _pick(cabecalho, "valor_total", "valor_mercadoria")
                                    situacao = _safe_text(_pick(cabecalho, "etapa", "situacao"))
                                    cliente_codigo = _safe_text(_pick(cabecalho, "codigo_cliente"))
                                    
                                    detalhes = pedido_venda.get("det", [])
                                    quantidade_itens = len(detalhes) if isinstance(detalhes, list) else 0
                                    
                                    await conn.execute("""
                                        INSERT INTO public.omie_pedido 
                                        (id_pedido_omie, numero, valor_total, situacao, quantidade_itens, cliente_codigo, detalhe, created_at, updated_at)
                                        VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
                                        ON CONFLICT (id_pedido_omie) DO UPDATE SET
                                        numero = COALESCE(EXCLUDED.numero, omie_pedido.numero),
                                        valor_total = COALESCE(EXCLUDED.valor_total, omie_pedido.valor_total),
                                        situacao = COALESCE(EXCLUDED.situacao, omie_pedido.situacao),
                                        quantidade_itens = COALESCE(EXCLUDED.quantidade_itens, omie_pedido.quantidade_itens),
                                        cliente_codigo = COALESCE(EXCLUDED.cliente_codigo, omie_pedido.cliente_codigo),
                                        detalhe = COALESCE(EXCLUDED.detalhe, omie_pedido.detalhe),
                                        updated_at = now();
                                    """, codigo_pedido, numero, valor_total, situacao, quantidade_itens, cliente_codigo, json.dumps(pedido_venda))
                                    
                                    logger.info(f"✅ Pedido {codigo_pedido} salvo no banco")
                                    
                                except Exception as e:
                                    logger.error(f"❌ Erro ao enriquecer pedido {codigo_pedido}: {e}")
                                    # Continua mesmo com erro no enriquecimento
                            
                            await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                            processed += 1
                            log.append({"pedido_ok": codigo_pedido, "event_id": ev_id})
                            
                        else:
                            # Marcar outros tipos de evento como processados
                            await conn.execute("UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1", ev_id)
                            processed += 1
                            log.append({"evento_processado": ev_id, "rota": route})
                            
                    except Exception as e:
                        errors += 1
                        error_msg = f"{type(e).__name__}: {str(e)}"
                        await conn.execute("UPDATE public.omie_webhook_events SET error=$1 WHERE id=$2", error_msg, ev_id)
                        logger.error(f"❌ Erro processando evento {ev_id}: {error_msg}")
                        log.append({"erro": error_msg, "event_id": ev_id})
            
            logger.info(f"✅ Processamento concluído: {processed} processados, {errors} erros")
            
    except Exception as e:
        logger.error(f"❌ Erro geral no run-jobs: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    
    return JSONResponse({
        "ok": True, 
        "processed": processed, 
        "errors": errors, 
        "events": log,
        "message": f"Processados: {processed}, Erros: {errors}"
    })

# ------------------------------------------------------------------------------
# DEBUG ENDPOINTS
# ------------------------------------------------------------------------------
@router.get("/admin/debug/events")
async def debug_events(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")
    
    async with app.state.pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, route, processed, error, received_at, payload::text as payload_text
            FROM public.omie_webhook_events 
            ORDER BY id DESC 
            LIMIT 20
        """)
        
        pedidos = await conn.fetch("SELECT * FROM public.omie_pedido ORDER BY created_at DESC LIMIT 10")
        nfes = await conn.fetch("SELECT * FROM public.omie_nfe_xml ORDER BY created_at DESC LIMIT 10")
    
    return JSONResponse({
        "events": [dict(e) for e in events],
        "pedidos": [dict(p) for p in pedidos],
        "nfes": [dict(n) for n in nfes]
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
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)