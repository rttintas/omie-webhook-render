# app_combined.py — serviço único (webhooks + jobs) com ajustes para Omie (Pedidos + NF-e via contador/xml)
# Start command (Render): uvicorn app_combined:app --host 0.0.0.0 --port $PORT

import os
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse

# ========================= ENV =========================
DATABASE_URL              = os.getenv("DATABASE_URL", "")
ADMIN_RUNJOBS_SECRET      = os.getenv("ADMIN_RUNJOBS_SECRET", "julia-matheus")

WEBHOOK_TOKEN_PED         = os.getenv("OMIE_WEBHOOK_TOKEN", "")       # ex: t1
WEBHOOK_TOKEN_XML         = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "")   # ex: t2

OMIE_APP_KEY              = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET           = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT_SECONDS      = float(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

# Pedidos
OMIE_PEDIDO_URL           = os.getenv("OMIE_PEDIDO_URL", "https://app.omie.com.br/api/v1/produtos/pedido/")
ENRICH_PEDIDO_IMEDIATO    = os.getenv("ENRICH_PEDIDO_IMEDIATO", "true").lower() in ("1", "true", "yes", "y")

# NF-e (dois provedores: 'contador' recomendado, 'nfetool' legado)
OMIE_XML_PROVIDER         = os.getenv("OMIE_XML_PROVIDER", "contador")  # 'contador' ou 'nfetool'
OMIE_CONTADOR_XML_URL     = os.getenv("OMIE_CONTADOR_XML_URL", "https://app.omie.com.br/api/v1/contador/xml/")
OMIE_XML_URL              = os.getenv("OMIE_XML_URL", "https://app.omie.com.br/api/v1/nfe/nfetool/")
OMIE_XML_LIST_CALL        = os.getenv("OMIE_XML_LIST_CALL", "ListarDocumentos")

NFE_LOOKBACK_DAYS         = int(os.getenv("NFE_LOOKBACK_DAYS",  "15"))
NFE_LOOKAHEAD_DAYS        = int(os.getenv("NFE_LOOKAHEAD_DAYS", "1"))

if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Postgres).")

# ==================== APP / POOL =======================
router = APIRouter()

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(
        dsn=DATABASE_URL, min_size=1, max_size=5,
        command_timeout=60, max_inactive_connection_lifetime=300
    )
    async with app.state.pool.acquire() as conn:
        await conn.execute("SELECT 1;")
        await _ensure_tables(conn)
    try:
        yield
    finally:
        await app.state.pool.close()

app = FastAPI(title="Omie Webhooks + Jobs (combined)", lifespan=lifespan)

# ==================== HELPERS ==========================
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

def _tz_sp() -> timezone:
    return timezone(timedelta(hours=-3))  # América/São_Paulo (fixo)

def _parse_dt_any(v: Any) -> Optional[datetime]:
    """Aceita ISO (Z/offset) e BR (dd/MM/yyyy [HH:mm[:ss]])"""
    if not v:
        return None
    s = str(v).strip().replace("às", "").replace("  ", " ")
    # ISO (suporta Z)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass
    # BR
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=_tz_sp())
        except Exception:
            continue
    return None

def _event_block(body: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(body, dict):
        return {}
    ev = body.get("event") or body.get("evento")
    return ev if isinstance(ev, dict) else body

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
        processed    boolean,
        processed_at timestamptz,
        error        text
    );
    CREATE INDEX IF NOT EXISTS idx_owe_received_at ON public.omie_webhook_events (received_at);
    CREATE INDEX IF NOT EXISTS idx_owe_processed   ON public.omie_webhook_events (processed);
    CREATE INDEX IF NOT EXISTS idx_owe_route       ON public.omie_webhook_events (route);
    CREATE INDEX IF NOT EXISTS idx_owe_topic       ON public.omie_webhook_events (topic);
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
    # Auto-cicatrização de colunas
    await conn.execute("""
      ALTER TABLE public.omie_webhook_events
        ADD COLUMN IF NOT EXISTS processed     boolean,
        ADD COLUMN IF NOT EXISTS processed_at  timestamptz,
        ADD COLUMN IF NOT EXISTS error         text,
        ADD COLUMN IF NOT EXISTS received_at   timestamptz DEFAULT now();
    """)
    # Tenta ajustar tipos para jsonb (sem quebrar se houver view)
    for col in ("raw_headers","payload"):
        try:
            cur = await conn.fetchval("""
                SELECT data_type FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='omie_webhook_events' AND column_name=$1
            """, col)
            if cur and cur.lower() != "jsonb":
                await conn.execute(f"ALTER TABLE public.omie_webhook_events ALTER COLUMN {col} TYPE jsonb USING ({col}::jsonb)")
        except Exception:
            pass

# ==================== HEALTH ===========================
@router.get("/")
async def root():
    return {"ok": True, "see": ["/health", "/db-ping"]}

@router.get("/health")
async def health():
    return {"ok": True, "ts": _now_utc().isoformat()}

@router.get("/db-ping")
async def db_ping():
    async with app.state.pool.acquire() as conn:
        v = await conn.fetchval("SELECT 1;")
    return {"db": int(v)}

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
    topic     = _safe_text(_pick(body_dict, "topic","TipoEvento","evento")) or "omie_webhook_received"
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at)
            VALUES ('omie', $1, '/omie/webhook', 200, $2, NULL, $3::jsonb, $4::jsonb, now(), now());
        """,
        "omie_webhook_received", topic,
        json.dumps(dict(request.headers)),
        json.dumps(body_dict)
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
    body_dict = _as_json(body)
    topic     = _safe_text(_pick(body_dict, "topic","TipoEvento","evento")) or "nfe_xml_received"
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events
                (source, event_type, route, http_status, topic, status, raw_headers, payload, event_ts, received_at)
            VALUES ('omie', $1, '/xml/omie/webhook', 200, $2, NULL, $3::jsonb, $4::jsonb, now(), now());
        """,
        "nfe_xml_received", topic,
        json.dumps(dict(request.headers)),
        json.dumps(body_dict)
        )
    return JSONResponse({"ok": True})

# ==================== EXTRAÇÃO DOS CAMPOS ==================
def _extract_pedido_fields(ev: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    retorna: (codigo_pedido, numero_pedido, codigo_pedido_integracao)
    suporta: raiz e pedido_venda_produto.cabecalho
    """
    id_ped = _safe_text(_pick(ev, "idPedido","id_pedido","codigo_pedido","codigoPedido","id"))
    numero = _safe_text(_pick(ev, "numeroPedido","numero_pedido","numero","nPedido"))
    integ  = _safe_text(_pick(ev, "codigo_pedido_integracao","codigoPedidoIntegracao"))

    pvp = ev.get("pedido_venda_produto")
    if isinstance(pvp, dict):
        cab = pvp.get("cabecalho") if isinstance(pvp.get("cabecalho"), dict) else {}
        id_ped = id_ped or _safe_text(_pick(cab, "codigo_pedido"))
        numero = numero or _safe_text(_pick(cab, "numero_pedido"))
        integ  = integ  or _safe_text(_pick(cab, "codigo_pedido_integracao"))

    return id_ped, numero, integ

def _extract_pedido_metrics(ev: Dict[str, Any], detalhe: Optional[Dict[str,Any]]) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[str]]:
    """
    valor_total, qtd_itens, situacao, cliente_codigo
    tenta pegar do evento (cabecalho) e, se falhar, do 'detalhe' consultado
    """
    valor = _safe_text(_pick(ev, "valorTotal","valor_total"))
    qtd   = None
    sit   = _safe_text(_pick(ev, "situacao","status"))
    cli   = _safe_text(_pick(ev, "idCliente","codigo_cliente","cliente_codigo"))

    pvp = ev.get("pedido_venda_produto")
    if isinstance(pvp, dict):
        cab = pvp.get("cabecalho") if isinstance(pvp.get("cabecalho"), dict) else {}
        valor = valor or _safe_text(_pick(cab, "valor_total","valor_merc","valor_pedido"))
        cli   = cli   or _safe_text(_pick(cab, "codigo_cliente"))
        etapa = _safe_text(_pick(cab, "etapa"))
        if etapa and not sit:
            sit_map = {"50":"FATURAR","60":"FATURADO"}
            sit = sit_map.get(etapa, etapa)
        qtd = qtd or _pick(cab, "quantidade_itens","qtd_itens")

        if qtd is None:
            det = pvp.get("det") or pvp.get("detalhe") or []
            if isinstance(det, list):
                qtd = len(det)

    if qtd is None and isinstance(detalhe, dict):
        det = detalhe.get("det") or detalhe.get("detalhe") or []
        if isinstance(det, list):
            qtd = len(det)

    try:
        qtd = int(qtd) if qtd is not None else None
    except Exception:
        qtd = None

    return valor, qtd, sit, cli

def _extract_nfe_fields(ev: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    chave     = _safe_text(_pick(ev, "nfe_chave","chave_nfe","chave","nChave"))
    numero    = _safe_text(_pick(ev, "numero_nf","nNumero","numero"))
    serie     = _safe_text(_pick(ev, "serie","cSerie"))
    data_emis = _safe_text(_pick(ev, "data_emis","dEmissao","emitida_em","dhEmi","data_emissao"))
    xml_b64   = _safe_text(_pick(ev, "nfe_xml","xml","xml_base64","cXml","xml_url"))

    nfe_blk = ev.get("nfe")
    if isinstance(nfe_blk, dict):
        xml_b64   = xml_b64   or _safe_text(_pick(nfe_blk, "xml","xml_base64","cXml"))
        chave     = chave     or _safe_text(_pick(nfe_blk, "nfe_chave","chave","nChave"))
        numero    = numero    or _safe_text(_pick(nfe_blk, "numero_nf","nNumero"))
        serie     = serie     or _safe_text(_pick(nfe_blk, "serie","cSerie"))
        data_emis = data_emis or _safe_text(_pick(nfe_blk, "data_emis","dEmissao","dhEmi"))
    return chave, numero, serie, data_emis, xml_b64

# ===================== OMIE CLIENTS ======================
async def _fetch_pedido_omie(client: httpx.AsyncClient, codigo_pedido: Optional[str], codigo_integracao: Optional[str]) -> Optional[Dict[str, Any]]:
    """ConsultarPedido por codigo_pedido (preferido) ou codigo_pedido_integracao."""
    if not (OMIE_APP_KEY and OMIE_APP_SECRET):
        return None
    param: Dict[str, Any] = {}
    if codigo_pedido:
        try:
            param["codigo_pedido"] = int(str(codigo_pedido))
        except Exception:
            return None
    elif codigo_integracao:
        param["codigo_pedido_integracao"] = codigo_integracao
    else:
        return None

    payload = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [param],
    }
    r = await client.post(OMIE_PEDIDO_URL, json=payload, timeout=OMIE_TIMEOUT_SECONDS)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else None

async def _fetch_xml_por_chave(client: httpx.AsyncClient, chave: str, data_emis: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Suporta 2 provedores:
      - 'contador'  -> /api/v1/contador/xml  (campos nChave, cXml, cSerie, nNumero, dEmissao, hEmissao)
      - 'nfetool'   -> /api/v1/nfe/nfetool   (legado)
    Retorna (obj_documento, xml_string_ou_base64)
    """
    if not (OMIE_APP_KEY and OMIE_APP_SECRET and chave):
        return None, None

    # Datas de busca (± janela)
    dt_ref  = _parse_dt_any(data_emis) or _now_utc()
    dt_ini  = (dt_ref - timedelta(days=NFE_LOOKBACK_DAYS))
    dt_fim  = (dt_ref + timedelta(days=NFE_LOOKAHEAD_DAYS))

    if OMIE_XML_PROVIDER.lower() == "contador":
        payload = {
            "call": "ListarDocumentos",
            "app_key": OMIE_APP_KEY,
            "app_secret": OMIE_APP_SECRET,
            "param": [{
                "nPagina": 1,
                "nRegPorPagina": 50,
                "cModelo": "55",
                "dEmiInicial": dt_ini.strftime("%d/%m/%Y"),
                "dEmiFinal":   dt_fim.strftime("%d/%m/%Y"),
            }]
        }
        r = await client.post(OMIE_CONTADOR_XML_URL, json=payload, timeout=OMIE_TIMEOUT_SECONDS)
        r.raise_for_status()
        data = r.json() if r.content else {}
        docs = (data.get("documentos") or data.get("lista") or data.get("documentosNFe") or [])
        target = None
        for doc in docs:
            try:
                if str(doc.get("nChave") or "").strip() == chave:
                    target = doc
                    break
            except Exception:
                continue
        if not target and docs:
            target = docs[0]

        if isinstance(target, dict):
            numero    = target.get("nNumero")
            serie     = target.get("cSerie")
            dEmissao  = target.get("dEmissao")
            hEmissao  = target.get("hEmissao")
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
        return data if isinstance(data, dict) else None, None

    # --- nfetool (legado) ---
    payload = {
        "call": OMIE_XML_LIST_CALL,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "pagina": 1,
            "registros_por_pagina": 50,
            "apenas_importadas": "N",
            "filtrar_por_data": "S",
            "data_emissao_inicial": dt_ini.date().isoformat(),
            "data_emissao_final":   dt_fim.date().isoformat(),
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
            xml_b64 = _safe_text(_pick(item, "xml","xml_base64","xmlNFe","arquivo_xml"))
            return item, xml_b64
    except Exception:
        pass
    return data if isinstance(data, dict) else None, None

# ===================== JOBS ==============================
@router.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...)):
    if secret != ADMIN_RUNJOBS_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")

    processed = 0
    errors    = 0
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
            ev_id = r["id"]
            route = r["route"] or ""
            payload = _as_json(r["payload"])
            ev = _event_block(_as_json(payload.get("payload") or payload))

            try:
                # -------- NF-e ----------
                if "/xml/omie/webhook" in route:
                    chave, numero, serie, data_emis, xml_b64 = _extract_nfe_fields(ev)

                    if (not xml_b64) and chave:
                        try:
                            fetched, xml_b64 = await _fetch_xml_por_chave(client, chave, data_emis)
                            # se o provedor trouxe número/série/data, aproveita
                            if fetched and isinstance(fetched, dict):
                                numero = str(fetched.get("numero") or numero or "") or None
                                serie  = str(fetched.get("serie") or serie or "") or None
                                data_emis = fetched.get("emitida_em") or data_emis
                        except Exception as ex:
                            log.append({"nfe_fetch_err": str(ex)})

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
                    _parse_dt_any(data_emis),
                    xml_b64
                    )

                    await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now(), error=NULL WHERE id=$1;", ev_id)
                    processed += 1
                    log.append({"nfe_ok": chave or "[sem_chave]", "provider": OMIE_XML_PROVIDER})
                    continue

                # -------- Pedido --------
                if "/omie/webhook" in route:
                    codigo_pedido, numero_pedido, codigo_integracao = _extract_pedido_fields(ev)

                    detalhe = None
                    if ENRICH_PEDIDO_IMEDIATO:
                        try:
                            detalhe = await _fetch_pedido_omie(client, codigo_pedido, codigo_integracao)
                        except Exception as ex:
                            log.append({"pedido_enrich_err": str(ex)})

                    valor_total, qtd_itens, situacao, cliente_codigo = _extract_pedido_metrics(ev, detalhe)

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
                    int(str(codigo_pedido)) if codigo_pedido else None,
                    numero_pedido,
                    valor_total,
                    situacao,
                    qtd_itens,
                    cliente_codigo,
                    json.dumps(detalhe if isinstance(detalhe, dict) else _as_json(ev))
                    )

                    await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now(), error=NULL WHERE id=$1;", ev_id)
                    processed += 1
                    log.append({"pedido_ok": codigo_pedido or numero_pedido or codigo_integracao or "[sem_id]"})
                    continue

                # rota desconhecida
                await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now(), error=$1 WHERE id=$2;",
                                   "rota_desconhecida", ev_id)
                processed += 1
                log.append({"ignorado": route})

            except Exception as ex:
                errors += 1
                await conn.execute("UPDATE public.omie_webhook_events SET processed=TRUE, processed_at=now(), error=$1 WHERE id=$2;",
                                   f"erro:{ex}", ev_id)
                log.append({"err": str(ex), "route": route})

    return JSONResponse({"ok": True, "processed": processed, "errors": errors, "events": log})

# ================= MOUNT & MAIN =========================
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT","8000")), reload=True)
