import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException, Query

# ----------------------------- #
# Config / ENVs
# ----------------------------- #
DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "false").lower() in ("1", "true", "yes")
OMIE_TIMEOUT = float(os.getenv("OMIE_TIMEOUT", "25"))  # segundos
OMIE_BASE = os.getenv("OMIE_BASE", "https://api.omie.com.br")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurada")

# ----------------------------- #
# App / Logger
# ----------------------------- #
app = FastAPI(title="Omie Webhook API")
log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)

# ----------------------------- #
# DB helpers
# ----------------------------- #
async def get_pool() -> asyncpg.Pool:
    if not hasattr(app.state, "pool"):
        app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return app.state.pool

async def ensure_schema():
    pool = await get_pool()
    async with pool.acquire() as con:
        # fila de jobs
        await con.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_jobs (
          id           BIGSERIAL PRIMARY KEY,
          job_type     TEXT NOT NULL,           -- 'enrich_pedido', etc
          payload      JSONB NOT NULL,
          status       TEXT NOT NULL DEFAULT 'queued',   -- queued|processing|done|failed|retrying
          attempts     INT  NOT NULL DEFAULT 0,
          next_run     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_error   TEXT,
          created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_omie_jobs_status_next
          ON public.omie_jobs (status, next_run);
        """)

        # garantias mínimas nas tabelas de destino (sem atrapalhar seu schema)
        # omie_pedido: numero UNIQUE já existe na sua base — usamos isso
        await con.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='omie_pedido' AND column_name='raw'
          ) THEN
            BEGIN
              ALTER TABLE public.omie_pedido ADD COLUMN raw JSONB;
            EXCEPTION WHEN duplicate_column THEN
              -- ok
            END;
          END IF;
        END$$;
        """)

        # omie_nfe: usa xml como URL do XML (se existir), pdf_url para DANFE
        await con.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='omie_nfe' AND column_name='pdf_url'
          ) THEN
            BEGIN
              ALTER TABLE public.omie_nfe ADD COLUMN pdf_url TEXT;
            EXCEPTION WHEN duplicate_column THEN
            END;
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='omie_nfe' AND column_name='xml'
          ) THEN
            BEGIN
              ALTER TABLE public.omie_nfe ADD COLUMN xml TEXT;
            EXCEPTION WHEN duplicate_column THEN
            END;
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='omie_nfe' AND column_name='recebido_em'
          ) THEN
            BEGIN
              ALTER TABLE public.omie_nfe ADD COLUMN recebido_em TIMESTAMPTZ DEFAULT NOW();
            EXCEPTION WHEN duplicate_column THEN
            END;
          END IF;
        END$$;
        """)

# ----------------------------- #
# Utils
# ----------------------------- #
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def backoff_seconds(attempts: int) -> int:
    # 1m, 2m, 4m, 8m, ... máx ~30m
    return min(60 * (2 ** max(0, attempts - 1)), 1800)

def j(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

# ----------------------------- #
# Normalizadores de payload
# ----------------------------- #
def normalize_pedido(evt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tenta extrair numero (numeroPedido) e idPedido.
    """
    id_pedido = evt.get("idPedido") or evt.get("id_pedido")
    numero = evt.get("numeroPedido") or evt.get("numero_pedido") or evt.get("numero")
    valor = evt.get("valorPedido") or evt.get("valor_pedido")

    return {
      "id_pedido": id_pedido,
      "numero": str(numero) if numero is not None else None,
      "valor_total": float(valor) if isinstance(valor, (int, float, str)) and str(valor).replace('.', '', 1).isdigit() else None,
      "raw": evt
    }

def normalize_nfe(evt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrai numero da NF, chave, urls e id_pedido se vier.
    """
    numero = evt.get("numero_nf") or evt.get("numero")
    chave = evt.get("nfe_chave") or evt.get("chave_nfe") or evt.get("chave")
    xml_url = evt.get("nfe_xml") or evt.get("xml")
    danfe_url = evt.get("nfe_danfe") or evt.get("pdf_url")
    id_pedido = evt.get("id_pedido") or evt.get("idPedido")

    return {
      "numero": str(numero) if numero is not None else None,
      "chave_nfe": str(chave) if chave is not None else None,
      "xml_url": xml_url,
      "danfe_url": danfe_url,
      "id_pedido": id_pedido,
      "raw": evt
    }

# ----------------------------- #
# Upserts
# ----------------------------- #
async def upsert_pedido(con: asyncpg.Connection, data: Dict[str, Any]) -> None:
    numero = data.get("numero")
    valor = data.get("valor_total")
    raw = data.get("raw")

    if not numero:
        # sem número, armazena o que der (fallback em id_pedido)
        await con.execute("""
        INSERT INTO public.omie_pedido (id_pedido, numero, recebido_em, raw)
        VALUES ($1, $2, NOW(), $3)
        ON CONFLICT DO NOTHING;
        """, data.get("id_pedido"), None, json.dumps(raw))
        return

    await con.execute("""
    INSERT INTO public.omie_pedido (numero, valor_total, recebido_em, raw)
    VALUES ($1, $2, NOW(), $3)
    ON CONFLICT (numero)
      DO UPDATE SET
        valor_total = EXCLUDED.valor_total,
        recebido_em = NOW(),
        raw = EXCLUDED.raw;
    """, numero, valor, json.dumps(raw))

async def upsert_nfe(con: asyncpg.Connection, data: Dict[str, Any]) -> None:
    numero = data.get("numero")
    chave = data.get("chave_nfe")
    xml_url = data.get("xml_url")
    danfe_url = data.get("danfe_url")

    if not numero and not chave:
        return

    # tenta por numero; se nulo, usa chave como "âncora" única
    if numero:
        await con.execute("""
        INSERT INTO public.omie_nfe (numero, chave_nfe, xml, pdf_url, recebido_em)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (numero)
          DO UPDATE SET
            chave_nfe = COALESCE(EXCLUDED.chave_nfe, public.omie_nfe.chave_nfe),
            xml      = COALESCE(EXCLUDED.xml, public.omie_nfe.xml),
            pdf_url  = COALESCE(EXCLUDED.pdf_url, public.omie_nfe.pdf_url),
            recebido_em = NOW();
        """, numero, chave, xml_url, danfe_url)
    else:
        # ancora por chave
        await con.execute("""
        INSERT INTO public.omie_nfe (chave_nfe, xml, pdf_url, recebido_em)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (chave_nfe)
          DO UPDATE SET
            xml      = COALESCE(EXCLUDED.xml, public.omie_nfe.xml),
            pdf_url  = COALESCE(EXCLUDED.pdf_url, public.omie_nfe.pdf_url),
            recebido_em = NOW();
        """, chave, xml_url, danfe_url)

# ----------------------------- #
# Fila de Jobs
# ----------------------------- #
async def enqueue_job(con: asyncpg.Connection, job_type: str, payload: Dict[str, Any]) -> None:
    await con.execute("""
    INSERT INTO public.omie_jobs (job_type, payload, status, attempts, next_run, created_at, updated_at)
    VALUES ($1, $2, 'queued', 0, NOW(), NOW(), NOW());
    """, job_type, json.dumps(payload))

async def fetch_next_job(con: asyncpg.Connection) -> Optional[asyncpg.Record]:
    return await con.fetchrow("""
    UPDATE public.omie_jobs
       SET status='processing', updated_at=NOW()
     WHERE id = (
       SELECT id
         FROM public.omie_jobs
        WHERE status IN ('queued', 'retrying')
          AND next_run <= NOW()
        ORDER BY next_run ASC, id ASC
        LIMIT 1
     )
    RETURNING *;
    """)

async def mark_job_done(con: asyncpg.Connection, job_id: int) -> None:
    await con.execute("""
    UPDATE public.omie_jobs
       SET status='done', updated_at=NOW()
     WHERE id=$1;
    """, job_id)

async def mark_job_retry(con: asyncpg.Connection, job_id: int, attempts: int, err: str) -> None:
    delay = backoff_seconds(attempts + 1)
    await con.execute("""
    UPDATE public.omie_jobs
       SET status='retrying',
           attempts=$2,
           last_error=$3,
           next_run=NOW() + INTERVAL '%s seconds',
           updated_at=NOW()
     WHERE id=$1;
    """ % delay, job_id, attempts + 1, err[:8000])

# ----------------------------- #
# Chamada Omie (pedido)
# ----------------------------- #
async def omie_consultar_pedido(client: httpx.AsyncClient, id_pedido: Optional[int], numero: Optional[str]) -> Dict[str, Any]:
    """
    Endpoint de exemplo baseado nos seus logs anteriores.
    Ajuste o "call" conforme seu contrato de API Omie (ListarPedidos/ConsultarPedido).
    """
    url = f"{OMIE_BASE}/api/v1/produtos/pedido/"
    # Exemplo genérico: tentar por id, senão por número
    if id_pedido:
        call = "ConsultarPedido"
        params = {"idPedido": id_pedido}
    else:
        call = "ListarPedidos"
        params = {"numero_pedido": numero}

    body = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [params],
    }

    r = await client.post(url, json=body, timeout=OMIE_TIMEOUT)
    r.raise_for_status()
    return r.json()

# ----------------------------- #
# Worker do Job enrich_pedido
# ----------------------------- #
async def process_enrich_pedido(con: asyncpg.Connection, payload: Dict[str, Any]) -> None:
    id_pedido = payload.get("id_pedido")
    numero = payload.get("numero")

    async with httpx.AsyncClient() as client:
        data = await omie_consultar_pedido(client, id_pedido=id_pedido, numero=numero)

    # Ajuste o parsing da resposta conforme a Omie retorna para você.
    # Aqui gravamos só como raw extra, e atualizamos valor_total se existir.
    valor_total = None
    try:
        # tente achar algum campo de valor total na resposta
        # (ajuste conforme seu contrato)
        if isinstance(data, dict):
            valor_total = (
                data.get("pedido") or {}
            ).get("valor_total")
    except Exception:
        pass

    await con.execute("""
    UPDATE public.omie_pedido
       SET raw = COALESCE(raw,'{}'::jsonb) || $2::jsonb,
           valor_total = COALESCE($3, valor_total),
           updated_at = NOW()
     WHERE numero = $1 OR ($1 IS NULL AND id_pedido = $4)
    """, numero, json.dumps({"omie_response": data}), valor_total, id_pedido)

# ----------------------------- #
# Endpoints
# ----------------------------- #
@app.on_event("startup")
async def _startup():
    await ensure_schema()
    log.info("Inicialização do aplicativo concluída.")

@app.get("/health/app")
async def health_app():
    return {"ok": True, "name": "Omie Webhook API"}

@app.get("/health/db")
async def health_db():
    pool = await get_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT 1 as select1;")
        return {"db": "ok", "select1": row["select1"]}

@app.post("/omie/webhook")
async def omie_webhook(req: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="token inválido")

    payload = await req.json()
    log.info("Payload recebido: %s", j(payload))

    topic = (payload.get("topic") or "").strip()
    event = payload.get("event") or {}

    pool = await get_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            if topic == "VendaProduto.Incluida":
                p = normalize_pedido(event)
                await upsert_pedido(con, p)
                # fila para enriquecimento
                await enqueue_job(con, "enrich_pedido", {"id_pedido": p.get("id_pedido"), "numero": p.get("numero")})

                # opcionalmente tentar enriquecer já (não recomendado se a Omie estiver lenta)
                if ENRICH_PEDIDO_IMEDIATO:
                    try:
                        await process_enrich_pedido(con, {"id_pedido": p.get("id_pedido"), "numero": p.get("numero")})
                    except Exception as e:
                        log.exception("Erro enriquecendo imediatamente: %s", e)

                return {"status": "success", "message": f"Pedido {p.get('numero') or p.get
