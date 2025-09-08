# app_combined.py — serviço (webhooks + jobs) Omie (Pedidos + NF-e)

import os
import json
import logging
import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List
from contextlib import asynccontextmanager
from xml.etree import ElementTree as ET

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse

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

# Endpoints Omie
OMIE_PEDIDO_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_XML_URL = "https://app.omie.com.br/api/v1/contador/xml/"
OMIE_XML_LIST_CALL = "ListarDocumentos"

# Comportamento
ENRICH_PEDIDO_IMEDIATO = True
NFE_LOOKBACK_DAYS = 7
NFE_LOOKAHEAD_DAYS = 1

# ------------------------------------------------------------------------------
# App / Pool
# ------------------------------------------------------------------------------
router = APIRouter()

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, max_size=5)
    async with app.state.pool.acquire() as conn:
        await _ensure_tables(conn)
    yield
    await app.state.pool.close()

app = FastAPI(title="Omie Webhooks + Jobs", lifespan=lifespan)

# ------------------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _as_json(obj: Any) -> Dict[str, Any]:
    try:
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, str):
            return json.loads(obj)
    except Exception as e:
        logger.error(f"Erro ao converter para JSON: {e}")
    return {}

def _pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
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
    return s or None

def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    try:
        # "2025-09-06T00:00:00-03:00" ou "2025-09-06T00:00:00Z"
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

def _fmt_br_date(d: datetime) -> str:
    # dd/mm/YYYY em UTC para a API Omie
    return d.astimezone(timezone.utc).strftime("%d/%m/%Y")

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

# ------------------------------------------------------------------------------
# DDL (idempotente)
# ------------------------------------------------------------------------------
async def _ensure_tables(conn: asyncpg.Connection) -> None:
    # Fila de eventos
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_webhook_events (
        id           bigserial PRIMARY KEY,
        source       text,
        event_type   text,
        route        text,
        http_status  int4,
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

    # NF-e principal
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe (
        chave_nfe         text PRIMARY KEY,
        numero            text,
        serie             text,
        emitida_em        timestamptz,
        data_emissao      text,
        cnpj_emitente     text,
        cnpj_destinatario text,
        valor_total       numeric(15,2),
        status            text,
        xml               text,
        xml_text          text,
        xml_url           text,
        danfe_url         text,
        pdf_url           text,
        last_event_at     timestamptz,
        updated_at        timestamptz,
        recebido_em       timestamptz DEFAULT now(),
        created_at        timestamptz DEFAULT now(),
        raw               jsonb,
        id_nf             int8,
        id_pedido         int8,
        cliente_nome      text,
        chave             text
    );
    """)

    # XML auxiliar (opcional)
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

    # Pedidos (para enriquecimento)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS public.omie_pedido (
        id_pedido_omie   bigint PRIMARY KEY,
        numero           text,
        valor_total      numeric(15,2),
        situacao         text,
        quantidade_itens int4,
        cliente_codigo   text,
        detalhe          jsonb,
        created_at       timestamptz DEFAULT now(),
        updated_at       timestamptz
    );
    """)

    # Colunas extras (idempotente)
    for ddl in [
        "ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS xml_text text",
        "ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS pdf_url text",
        "ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS data_emissao text",
        "ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now()",
        "ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS id_nf int8",
        "ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS id_pedido int8",
        "ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS cliente_nome text",
        "ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS chave text"
    ]:
        await conn.execute(ddl)

    # Índice único defensivo
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
    END$$;
    """)

# ------------------------------------------------------------------------------
# HEALTH
# ------------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    try:
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT now() AS now, count(*) AS pend FROM public.omie_webhook_events WHERE processed=false"
            )
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
        logger.info("📥 Webhook pedido recebido")
    except Exception:
        body = {}
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
              (source,event_type,route,http_status,topic,raw_headers,payload,event_ts,received_at,processed)
            VALUES ('omie','omie_webhook_received','/omie/webhook',200,$1,$2,$3,now(),now(),FALSE);
        """,
        "pedido_received",
        json.dumps(dict(request.headers), ensure_ascii=False, default=str),
        json.dumps(body, ensure_ascii=False, default=str))
    return JSONResponse({"ok": True})

@router.post("/xml/omie/webhook")
async def nfe_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=403, detail="invalid token")
    try:
        body = await request.json()
        logger.info("📥 Webhook NF-e recebido")
    except Exception:
        body = {}
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
              (source,event_type,route,http_status,topic,raw_headers,payload,event_ts,received_at,processed)
            VALUES ('omie','nfe_xml_received','/xml/omie/webhook',200,$1,$2,$3,now(),now(),FALSE);
        """,
        "nfe_received",
        json.dumps(dict(request.headers), ensure_ascii=False, default=str),
        json.dumps(body, ensure_ascii=False, default=str))
    return JSONResponse({"ok": True})

# ------------------------------------------------------------------------------
# NF-e helpers
# ------------------------------------------------------------------------------
async def _fetch_xml_via_contador(
    client: httpx.AsyncClient, chave: str, data_emis: Optional[datetime]
) -> Optional[Dict[str, Any]]:
    """
    Consulta Contador/XML → ListarDocumentos, filtrando por faixa de data
    e retorna somente o item cuja chave bater com a do webhook.
    """
    start_dt, end_dt = _date_range_for_omie(data_emis)
    payload = {
        "nPagina": 1,
        "nRegPorPagina": 50,
        "cModelo": "55",
        "dEmiInicial": _fmt_br_date(start_dt),
        "dEmiFinal": _fmt_br_date(end_dt),
    }
    logger.info(f"🔎 Buscando via Contador/XML.ListarDocumentos: {payload!r}")
    try:
        j = await _omie_post(client, OMIE_XML_URL, OMIE_XML_LIST_CALL, payload)
    except Exception as e:
        logger.warning(f"⚠️  Falha ao consultar Contador/XML: {e}")
        return None

    itens: List[Dict[str, Any]] = []
    for k in ("documentos", "lista", "documento", "lista_documentos"):
        v = j.get(k)
        if isinstance(v, list):
            itens = v
            break

    for it in itens:
        # tenta por campos de chave conhecidos
        nChave = _safe_text(it.get("nChave")) or _safe_text(it.get("chave")) or _safe_text(it.get("chaveNFe"))
        if nChave == chave:
            return it
        # tenta analisar o XML embutido
        cx = _safe_text(it.get("cXml"))
        if cx and (f"<chNFe>{chave}</chNFe>" in cx or f'Id="NFe{chave}"' in cx):
            return it

    return None

def _parse_xml_fields(xml_text: str) -> Dict[str, Any]:
    """Extrai alguns campos úteis do XML da NF-e."""
    out: Dict[str, Any] = {}
    try:
        ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
        root = ET.fromstring(xml_text)
        nfe_node = root.find(".//nfe:NFe", ns) or root

        cnpj_node = nfe_node.find(".//nfe:dest/nfe:CNPJ", ns) or nfe_node.find(".//nfe:dest/nfe:CPF", ns)
        if cnpj_node is not None and cnpj_node.text:
            out["cnpj_destinatario"] = cnpj_node.text.strip()

        xnome_node = nfe_node.find(".//nfe:dest/nfe:xNome", ns)
        if xnome_node is not None and xnome_node.text:
            out["cliente_nome"] = xnome_node.text.strip()

        vnf_node = nfe_node.find(".//nfe:total/nfe:ICMSTot/nfe:vNF", ns)
        if vnf_node is not None and vnf_node.text:
            try:
                out["valor_total"] = float(str(vnf_node.text).replace(",", "."))
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Não foi possível interpretar XML para enriquecimento: {e}")
    return out

# ------------------------------------------------------------------------------
# Processamento NF-e
# ------------------------------------------------------------------------------
async def processar_nfe(conn: asyncpg.Connection, payload: Dict[str, Any]) -> bool:
    """
    Processa e persiste a NF-e em public.omie_nfe (PK = chave_nfe).
    Salva mesmo sem XML; quando o XML aparecer, atualiza.
    """
    try:
        event = payload.get("event", {}) or payload.get("evento", {}) or {}
        chave = _safe_text(event.get("nfe_chave"))
        numero_nf = _safe_text(event.get("numero_nf"))
        serie = _safe_text(event.get("série")) or _safe_text(event.get("serie"))
        data_emis_text = _safe_text(event.get("data_emis"))
        data_emis = _parse_dt(data_emis_text)
        xml_url = _safe_text(event.get("nfe_xml"))
        danfe_url = _safe_text(event.get("nfe_danfe"))
        cnpj_emit = _safe_text(event.get("empresa_cnpj"))
        status = _safe_text(event.get("acao")) or _safe_text(event.get("cStatus"))
        id_nf = event.get("id_nf")
        id_pedido = event.get("id_pedido")

        if not chave:
            logger.warning("❌ NF-e sem chave no payload; ignorando.")
            return False

        logger.info(f"🔍 Processando NF-e: Chave={chave}, Numero={numero_nf}")

        # 1) tenta baixar o XML pelo link do webhook
        xml_text: Optional[str] = None
        if xml_url:
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.get(xml_url, timeout=30)
                    resp.raise_for_status()
                    xml_text = resp.text
                    logger.info("✅ XML baixado do link do webhook.")
            except Exception as e:
                logger.warning(f"Falha ao baixar XML do link do webhook: {e}")

        # 2) se não conseguiu, tenta Contador/XML (comparando a chave)
        valor_total_hint = None
        if not xml_text:
            async with httpx.AsyncClient(timeout=OMIE_TIMEOUT_SECONDS) as client:
                item = await _fetch_xml_via_contador(client, chave, data_emis)
                if item:
                    xml_text = _safe_text(item.get("cXml"))
                    numero_nf = _safe_text(item.get("nNumero")) or numero_nf
                    serie     = _safe_text(item.get("cSerie"))  or serie
                    status    = _safe_text(item.get("cStatus")) or status
                    try:
                        v = item.get("nValor")
                        valor_total_hint = float(v) if v is not None else None
                    except Exception:
                        valor_total_hint = None
                else:
                    logger.warning("⚠️  Não foi possível localizar a NF-e por Contador/XML para esta chave.")

        # 3) enriquecer com XML (se tiver)
        cnpj_dest = None
        cliente_nome = None
        valor_total = None
        if xml_text:
            enrich = _parse_xml_fields(xml_text)
            cnpj_dest = enrich.get("cnpj_destinatario")
            cliente_nome = enrich.get("cliente_nome")
            valor_total = enrich.get("valor_total")

        if valor_total is None and valor_total_hint is not None:
            valor_total = valor_total_hint

        # 4) UPSERT na tabela principal (salva mesmo sem XML)
        await conn.execute(
            """
            INSERT INTO public.omie_nfe
              (chave_nfe, numero, serie, emitida_em, data_emissao,
               cnpj_emitente, cnpj_destinatario, valor_total, status,
               xml, xml_text, xml_url, danfe_url, pdf_url,
               last_event_at, updated_at, recebido_em, created_at, raw,
               id_nf, id_pedido, cliente_nome, chave)
            VALUES
              ($1,$2,$3,$4,$5,
               $6,$7,$8,$9,
               $10,$11,$12,$13,$14,
               now(),now(),now(),now(),$15,
               $16,$17,$18,$1)
            ON CONFLICT (chave_nfe) DO UPDATE SET
              numero            = COALESCE(EXCLUDED.numero,            public.omie_nfe.numero),
              serie             = COALESCE(EXCLUDED.serie,             public.omie_nfe.serie),
              emitida_em        = COALESCE(EXCLUDED.emitida_em,        public.omie_nfe.emitida_em),
              data_emissao      = COALESCE(EXCLUDED.data_emissao,      public.omie_nfe.data_emissao),
              cnpj_emitente     = COALESCE(EXCLUDED.cnpj_emitente,     public.omie_nfe.cnpj_emitente),
              cnpj_destinatario = COALESCE(EXCLUDED.cnpj_destinatario, public.omie_nfe.cnpj_destinatario),
              valor_total       = COALESCE(EXCLUDED.valor_total,       public.omie_nfe.valor_total),
              status            = COALESCE(EXCLUDED.status,            public.omie_nfe.status),
              xml               = COALESCE(EXCLUDED.xml,               public.omie_nfe.xml),
              xml_text          = COALESCE(EXCLUDED.xml_text,          public.omie_nfe.xml_text),
              xml_url           = COALESCE(EXCLUDED.xml_url,           public.omie_nfe.xml_url),
              danfe_url         = COALESCE(EXCLUDED.danfe_url,         public.omie_nfe.danfe_url),
              pdf_url           = COALESCE(EXCLUDED.pdf_url,           public.omie_nfe.pdf_url),
              last_event_at     = now(),
              updated_at        = now(),
              raw               = EXCLUDED.raw,
              id_nf             = COALESCE(EXCLUDED.id_nf,             public.omie_nfe.id_nf),
              id_pedido         = COALESCE(EXCLUDED.id_pedido,         public.omie_nfe.id_pedido),
              cliente_nome      = COALESCE(EXCLUDED.cliente_nome,      public.omie_nfe.cliente_nome),
              chave             = COALESCE(EXCLUDED.chave,             public.omie_nfe.chave)
            """,
            # params
            chave,
            _safe_text(numero_nf),
            _safe_text(serie),
            data_emis,
            _safe_text(data_emis_text),
            _safe_text(cnpj_emit),
            _safe_text(cnpj_dest),
            valor_total,
            _safe_text(status),
            None if not xml_text else xml_text,   # xml
            xml_text,
            xml_url,
            danfe_url,
            danfe_url or xml_url,
            json.dumps(payload),
            int(id_nf) if _safe_text(id_nf) else None,
            int(id_pedido) if _safe_text(id_pedido) else None,
            _safe_text(cliente_nome),
        )

        # 5) guarda cópia em base64 (opcional)
        if xml_text:
            xml_b64 = base64.b64encode(xml_text.encode("utf-8")).decode("utf-8")
            await conn.execute(
                """
                INSERT INTO public.omie_nfe_xml (chave_nfe, numero, serie, emitida_em, xml_base64, recebido_em, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,now(),now(),now())
                ON CONFLICT (chave_nfe) DO UPDATE SET
                  numero     = COALESCE(EXCLUDED.numero,      public.omie_nfe_xml.numero),
                  serie      = COALESCE(EXCLUDED.serie,       public.omie_nfe_xml.serie),
                  emitida_em = COALESCE(EXCLUDED.emitida_em,  public.omie_nfe_xml.emitida_em),
                  xml_base64 = EXCLUDED.xml_base64,
                  updated_at = now()
                """,
                chave, _safe_text(numero_nf), _safe_text(serie), data_emis, xml_b64
            )

        logger.info(f"✅ NF-e {chave} salva/atualizada em public.omie_nfe")
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao processar NF-e: {e}")
        return False

# ------------------------------------------------------------------------------
# Pedido
# ------------------------------------------------------------------------------
async def _consultar_pedido(client: httpx.AsyncClient, codigo_pedido: int) -> Dict[str, Any]:
    payload = {"codigo_pedido": codigo_pedido}
    return await _omie_post(client, OMIE_PEDIDO_URL, "ConsultarPedido", payload)

def _extract_pedido_id_from_event(payload: Dict[str, Any]) -> Optional[int]:
    try:
        ev = payload.get("event", {}) or payload.get("evento", {}) or {}
        if ev.get("idPedido"):
            return int(ev["idPedido"])
        for k in ("codigo_pedido", "id_pedido", "pedido_id", "nCodPed"):
            if ev.get(k):
                return int(ev[k])
    except Exception:
        pass
    logger.warning("❌ Não foi possível extrair código do pedido do evento.")
    return None

# ------------------------------------------------------------------------------
# Run Jobs
# ------------------------------------------------------------------------------
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    processed = 0
    errors = 0
    log: List[Dict[str, Any]] = []

    try:
        async with app.state.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, route, payload
                  FROM public.omie_webhook_events
                 WHERE processed = false
              ORDER BY id ASC
                 LIMIT 100
            """)
            logger.info(f"🔍 Encontrados {len(rows)} eventos para processar")

            async with httpx.AsyncClient(timeout=OMIE_TIMEOUT_SECONDS) as client:
                for r in rows:
                    ev_id = r["id"]
                    route = r["route"] or ""
                    payload = _as_json(r["payload"])

                    try:
                        logger.info(f"⚡ Processando evento {ev_id} da rota {route}")

                        if "/xml/omie/webhook" in route:
                            ok = await processar_nfe(conn, payload)
                            if ok:
                                await conn.execute(
                                    "UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1",
                                    ev_id,
                                )
                                processed += 1
                                log.append({"nfe_ok": ev_id})
                            else:
                                errors += 1
                                await conn.execute(
                                    "UPDATE public.omie_webhook_events SET error=$1 WHERE id=$2",
                                    "processar_nfe_failed",
                                    ev_id,
                                )
                            continue

                        if "/omie/webhook" in route:
                            codigo_pedido = _extract_pedido_id_from_event(payload)
                            if not codigo_pedido:
                                logger.warning(f"⚠️  Evento {ev_id} sem código de pedido")
                                await conn.execute(
                                    "UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1",
                                    ev_id,
                                )
                                processed += 1
                                log.append({"pedido_skip_sem_codigo": ev_id})
                                continue

                            if ENRICH_PEDIDO_IMEDIATO:
                                try:
                                    data = await _consultar_pedido(client, codigo_pedido)
                                    pedido_venda = data.get("pedido_venda_produto", {})
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
                                          numero           = COALESCE(EXCLUDED.numero,          omie_pedido.numero),
                                          valor_total      = COALESCE(EXCLUDED.valor_total,     omie_pedido.valor_total),
                                          situacao         = COALESCE(EXCLUDED.situacao,        omie_pedido.situacao),
                                          quantidade_itens = COALESCE(EXCLUDED.quantidade_itens,omie_pedido.quantidade_itens),
                                          cliente_codigo   = COALESCE(EXCLUDED.cliente_codigo,  omie_pedido.cliente_codigo),
                                          detalhe          = COALESCE(EXCLUDED.detalhe,         omie_pedido.detalhe),
                                          updated_at       = now();
                                    """, int(codigo_pedido), numero, valor_total, situacao,
                                         quantidade_itens, cliente_codigo, json.dumps(pedido_venda))
                                    logger.info(f"✅ Pedido {codigo_pedido} salvo/atualizado")
                                except Exception as e:
                                    logger.error(f"❌ Erro ao enriquecer pedido {codigo_pedido}: {e}")

                            await conn.execute(
                                "UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1",
                                ev_id,
                            )
                            processed += 1
                            log.append({"pedido_ok": codigo_pedido, "event_id": ev_id})
                            continue

                        # rota desconhecida → marca como processado
                        await conn.execute(
                            "UPDATE public.omie_webhook_events SET processed=true, processed_at=now() WHERE id=$1",
                            ev_id,
                        )
                        processed += 1
                        log.append({"evento_processado": ev_id, "rota": route})

                    except Exception as e:
                        errors += 1
                        await conn.execute(
                            "UPDATE public.omie_webhook_events SET error=$1 WHERE id=$2",
                            f"{type(e).__name__}: {e}", ev_id
                        )
                        logger.error(f"❌ Erro processando evento {ev_id}: {e}")

    except Exception as e:
        logger.error(f"❌ Erro geral no run-jobs: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    logger.info(f"✅ Processamento concluído: {processed} processados, {errors} erros")
    return JSONResponse({"ok": True, "processed": processed, "errors": errors, "log": log})

# ------------------------------------------------------------------------------
# Debug
# ------------------------------------------------------------------------------
@router.get("/admin/debug/events")
async def debug_events(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")
    async with app.state.pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, route, processed, error, received_at, payload::text AS payload_text
              FROM public.omie_webhook_events
          ORDER BY id DESC LIMIT 50
        """)
        nfes = await conn.fetch("""
            SELECT chave_nfe, numero, serie, emitida_em, status, xml_url, danfe_url, pdf_url, recebido_em, updated_at
              FROM public.omie_nfe
          ORDER BY recebido_em DESC NULLS LAST, updated_at DESC NULLS LAST
             LIMIT 20
        """)
    return JSONResponse({"events": [dict(e) for e in events], "nfes": [dict(n) for n in nfes]})

# ------------------------------------------------------------------------------
# Mount
# ------------------------------------------------------------------------------
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
