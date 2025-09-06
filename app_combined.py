# app_combined.py
# FastAPI + asyncpg + Omie webhooks + reprocesso (ConsultarPedido / ListarDocumentos)

import os
import json
import base64
import asyncio
from datetime import datetime, timezone

import aiohttp
import asyncpg
from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("DB_URL") or ""
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "julia-matheus")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET") or os.getenv("OMIE_APP_HASH") or ""
OMIE_BASE = "https://app.omie.com.br/api/v1"
TZ = os.getenv("TIMEZONE", "America/Sao_Paulo")

if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL (Postgres).")

# ------------------------------------------------------------------------------
# App
# ------------------------------------------------------------------------------
app = FastAPI(title="Omie Webhooks + Jobs")
router = APIRouter()

# pool asyncpg
@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with app.state.pool.acquire() as conn:
        await _run_migrations(conn)

@app.on_event("shutdown")
async def shutdown():
    pool = getattr(app.state, "pool", None)
    if pool:
        await pool.close()

# ------------------------------------------------------------------------------
# Migrations (tabelas e colunas usadas pelo código)
# ------------------------------------------------------------------------------
async def _run_migrations(conn: asyncpg.Connection):
    # Eventos brutos (tudo cai aqui)
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
    # índice e unicidade do event_id (aceita NULL)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_owe_event_ts ON public.omie_webhook_events(event_ts);")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_owe_route ON public.omie_webhook_events(route);")
    await conn.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'omie_webhook_events_event_id_uniq'
        ) THEN
            ALTER TABLE public.omie_webhook_events
            ADD CONSTRAINT omie_webhook_events_event_id_uniq UNIQUE (event_id);
        END IF;
    END$$;
    """)

    # Pedidos (campos mínimos + JSON completo)
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

    # NF-e (armazenar XML base64)
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
# Helpers Omie API
# ------------------------------------------------------------------------------
async def _omie_call(session: aiohttp.ClientSession, endpoint: str, call: str, payload: dict):
    """
    Faz uma chamada à API da Omie (POST JSON).
    """
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
        if isinstance(data, dict) and data.get("faultstring"):
            raise RuntimeError(f"Omie {endpoint}/{call}: {data.get('faultstring')}")
        return data

async def _fetch_pedido_por_codigo(session: aiohttp.ClientSession, codigo_pedido: int) -> dict:
    """
    /api/v1/produtos/pedido/ -> ConsultarPedido
    """
    data = await _omie_call(session, "produtos/pedido/", "ConsultarPedido",
                            {"codigo_pedido": int(codigo_pedido)})
    return data.get("pedido_venda_produto") or data

def _yyyymmdd(dt_iso: str) -> str:
    """
    Converte '2025-09-06T00:00:00-03:00' -> '06/09/2025'
    """
    try:
        dt = datetime.fromisoformat(dt_iso.replace("Z","+00:00"))
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return ""

async def _fetch_xml_por_chave(session: aiohttp.ClientSession, nfe_chave: str, data_emis_iso: str|None) -> str|None:
    """
    /api/v1/contador/xml/ -> ListarDocumentos (encontra pelo nChave e retorna cXml)
    Sempre retorna base64 (converte se vier texto).
    """
    d_ini = d_fim = _yyyymmdd(data_emis_iso or "")
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
            # Se começar com XML, converte para base64; senão assume que já é base64
            if cxml.lstrip().startswith("<?xml"):
                return base64.b64encode(cxml.encode("utf-8")).decode("ascii")
            return cxml
    return None

# ------------------------------------------------------------------------------
# Webhooks
# ------------------------------------------------------------------------------
@router.post("/omie/webhook")
async def pedidos_webhook(request: Request, token: str):
    """
    Recebe eventos (VendaProduto.*). Só armazena o evento bruto.
    """
    # (Opcional) validar token se quiser: if token != os.getenv("WEBHOOK_TOKEN_PED", "um-segredo-forte"): ...
    body = await request.json()
    event_id = (body.get("messageId") or body.get("id") or "")[:64]
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events (source, event_type, event_id, route, payload)
            VALUES ('omie', 'omie_webhook_received', $1, '/omie/webhook', $2::jsonb)
            ON CONFLICT (event_id) DO NOTHING;
        """, event_id, json.dumps(body))
    return {"ok": True}

@router.post("/xml/omie/webhook")
async def xml_webhook(request: Request, token: str):
    """
    Recebe eventos de NFe.NotaAutorizada / NFe.NotaCancelada (etc). Armazena bruto.
    """
    body = await request.json()
    event_id = (body.get("messageId") or body.get("id") or "")[:64]
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_webhook_events (source, event_type, event_id, route, payload)
            VALUES ('omie', 'nfe_xml_received', $1, '/xml/omie/webhook', $2::jsonb)
            ON CONFLICT (event_id) DO NOTHING;
        """, event_id, json.dumps(body))
    return {"ok": True}

# ------------------------------------------------------------------------------
# Health
# ------------------------------------------------------------------------------
@router.get("/healthz")
async def healthz():
    return {"status": "healthy", "components": ["pedidos", "nfe_xml"], "compat": True}

# ------------------------------------------------------------------------------
# Admin: run-jobs (processar eventos -> API Omie -> gravar no banco)
# ------------------------------------------------------------------------------
@router.post("/admin/run-jobs")
async def run_jobs(secret: str):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    processed = 0
    errors = 0
    events_log: list[dict] = []

    async with app.state.pool.acquire() as conn, aiohttp.ClientSession() as session:
        # 1) Eventos de pedidos (rota /omie/webhook)
        rows = await conn.fetch("""
            SELECT id, event_id, payload
              FROM public.omie_webhook_events
             WHERE processed IS NOT TRUE
               AND route LIKE '/omie/webhook%%'
             ORDER BY id
             LIMIT 200;
        """)
        for r in rows:
            try:
                ev = r["payload"] or {}
                e = ev.get("event") or {}
                codigo_pedido = e.get("idPedido") or e.get("id_pedido") or e.get("codigo_pedido")
                if not codigo_pedido:
                    raise RuntimeError("evento sem idPedido")

                pedido = await _fetch_pedido_por_codigo(session, int(codigo_pedido))

                cab = pedido.get("cabecalho", {}) if isinstance(pedido, dict) else {}
                tot = pedido.get("total_pedido", {}) if isinstance(pedido, dict) else {}

                id_pedido_omie = int(cab.get("codigo_pedido", codigo_pedido))
                numero = str(cab.get("numero_pedido") or "")
                valor_total = float(tot.get("valor_total_pedido") or 0)
                situacao = str(cab.get("etapa") or "")
                quantidade_itens = int(cab.get("quantidade_itens") or 0)
                cliente_codigo = str(cab.get("codigo_cliente") or "")

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
                events_log.append({"pedido_ok": id_pedido_omie})

            except Exception as ex:
                errors += 1
                events_log.append({"pedido_err": str(ex)})
                # marca como processado com erro para não travar a fila
                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, status = $2
                     WHERE id = $1;
                """, r["id"], f"erro:{ex}")

        # 2) Eventos de NFe (rota /xml/omie/webhook)
        rows = await conn.fetch("""
            SELECT id, event_id, payload
              FROM public.omie_webhook_events
             WHERE processed IS NOT TRUE
               AND route LIKE '/xml/omie/webhook%%'
             ORDER BY id
             LIMIT 200;
        """)
        for r in rows:
            try:
                ev = r["payload"] or {}
                e = ev.get("event") or {}
                nfe_chave = e.get("nfe_chave") or e.get("nChave")
                data_emis = e.get("data_emis") or e.get("dEmissao")
                numero_nf = e.get("numero_nf") or e.get("nNumero")
                serie = e.get("serie") or e.get("cSerie")

                if not nfe_chave:
                    raise RuntimeError("evento NFe sem chave")

                xml_b64 = await _fetch_xml_por_chave(session, str(nfe_chave), data_emis)
                if not xml_b64:
                    raise RuntimeError("XML não encontrado na API (ListarDocumentos)")

                await conn.execute("""
                    INSERT INTO public.omie_nfe_xml
                      (chave_nfe, numero, serie, emitida_em, recebido_em, xml_base64, created_at)
                    VALUES
                      ($1, $2, $3, $4, now(), $5, now())
                    ON CONFLICT (chave_nfe) DO UPDATE
                      SET numero = EXCLUDED.numero,
                          serie  = EXCLUDED.serie,
                          xml_base64 = EXCLUDED.xml_base64,
                          updated_at = now();
                """, str(nfe_chave), str(numero_nf or ""), str(serie or ""),
                     data_emis, xml_b64)

                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, status = 'consumido'
                     WHERE id = $1;
                """, r["id"])
                processed += 1
                events_log.append({"nfe_ok": str(nfe_chave)})

            except Exception as ex:
                errors += 1
                events_log.append({"nfe_err": str(ex)})
                await conn.execute("""
                    UPDATE public.omie_webhook_events
                       SET processed = TRUE, status = $2
                     WHERE id = $1;
                """, r["id"], f"erro:{ex}")

    return {"ok": True, "processed": processed, "errors": errors, "events": events_log}

# ------------------------------------------------------------------------------
# Util: selects rápidos para conferência (opcionais)
# ------------------------------------------------------------------------------
@router.get("/admin/check-today")
async def check_today(secret: str):
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
# Mount
# ------------------------------------------------------------------------------
app.include_router(router)

# ------------------------------------------------------------------------------
# Entry (quando executar local com: python app_combined.py)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_combined:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
