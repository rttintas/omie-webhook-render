import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException, Query

# =======================
# Configuração / ENVs
# =======================
APP_NAME = "Omie Webhook API"

DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")

OMIE_BASE = os.getenv("OMIE_BASE", "https://api.omie.com.br")
OMIE_TIMEOUT = float(os.getenv("OMIE_TIMEOUT", "25"))  # segundos
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "false").lower() in ("1", "true", "yes")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurada")

# =======================
# App / Logger
# =======================
app = FastAPI(title=APP_NAME)
log = logging.getLogger("uvicorn")
log.setLevel(logging.INFO)

# =======================
# Helpers
# =======================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def backoff_seconds(attempts: int) -> int:
    # 1min, 2min, 4min, 8min, 16min, máx 30min
    return min(60 * (2 ** max(0, attempts - 1)), 1800)

def j(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

# =======================
# DB pool + schema
# =======================
async def get_pool() -> asyncpg.Pool:
    if not hasattr(app.state, "pool"):
        app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return app.state.pool

async def ensure_schema() -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        # Fila de jobs
        await con.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_jobs (
          id         BIGSERIAL PRIMARY KEY,
          job_type   TEXT   NOT NULL,
          payload    JSONB  NOT NULL,
          status     TEXT   NOT NULL DEFAULT 'queued',  -- queued|processing|retrying|done
          attempts   INT    NOT NULL DEFAULT 0,
          next_run   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_omie_jobs_status_next
          ON public.omie_jobs (status, next_run);
        """)

        # Garante colunas mínimas em omie_pedido (sem quebrar seu schema existente)
        await con.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='omie_pedido' AND column_name='raw'
          ) THEN
            ALTER TABLE public.omie_pedido ADD COLUMN raw JSONB;
          END IF;

          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='omie_pedido' AND column_name='recebido_em'
          ) THEN
            ALTER TABLE public.omie_pedido ADD COLUMN recebido_em TIMESTAMPTZ DEFAULT NOW();
          END IF;

          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='omie_pedido' AND column_name='valor_total'
          ) THEN
            ALTER TABLE public.omie_pedido ADD COLUMN valor_total NUMERIC(18,2);
          END IF;
        END$$;
        """)

        # Garante colunas mínimas em omie_nfe
        await con.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='omie_nfe' AND column_name='pdf_url'
          ) THEN
            ALTER TABLE public.omie_nfe ADD COLUMN pdf_url TEXT;
          END IF;

          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='omie_nfe' AND column_name='xml'
          ) THEN
            ALTER TABLE public.omie_nfe ADD COLUMN xml TEXT;
          END IF;

          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='omie_nfe' AND column_name='recebido_em'
          ) THEN
            ALTER TABLE public.omie_nfe ADD COLUMN recebido_em TIMESTAMPTZ DEFAULT NOW();
          END IF;
        END$$;
        """)

# =======================
# UPSERTs
# =======================
async def upsert_pedido(con: asyncpg.Connection, data: Dict[str, Any]) -> None:
    numero = data.get("numero")
    id_pedido = data.get("id_pedido")
    valor = data.get("valor_total")
    raw = data.get("raw") or {}

    # Se não veio número, guarda pelo id_pedido
    if not numero:
        await con.execute("""
        INSERT INTO public.omie_pedido (id_pedido, numero, valor_total, recebido_em, raw)
        VALUES ($1, NULL, $2, NOW(), $3)
        ON CONFLICT DO NOTHING;
        """, id_pedido, valor, json.dumps(raw))
        return

    await con.execute("""
    INSERT INTO public.omie_pedido (numero, valor_total, recebido_em, raw)
    VALUES ($1, $2, NOW(), $3)
    ON CONFLICT (numero)
      DO UPDATE SET
        valor_total = COALESCE(EXCLUDED.valor_total, public.omie_pedido.valor_total),
        recebido_em = NOW(),
        raw = EXCLUDED.raw;
    """, str(numero), valor, json.dumps(raw))

async def upsert_nfe(con: asyncpg.Connection, data: Dict[str, Any]) -> None:
    numero = data.get("numero")
    chave = data.get("chave_nfe")
    xml_url = data.get("xml_url")
    danfe_url = data.get("danfe_url")
    raw = data.get("raw") or {}

    if numero:
        await con.execute("""
        INSERT INTO public.omie_nfe (numero, chave_nfe, xml, pdf_url, recebido_em, raw)
        VALUES ($1, $2, $3, $4, NOW(), $5)
        ON CONFLICT (numero)
          DO UPDATE SET
            chave_nfe = COALESCE(EXCLUDED.chave_nfe, public.omie_nfe.chave_nfe),
            xml       = COALESCE(EXCLUDED.xml,      public.omie_nfe.xml),
            pdf_url   = COALESCE(EXCLUDED.pdf_url,  public.omie_nfe.pdf_url),
            recebido_em = NOW(),
            raw = EXCLUDED.raw;
        """, str(numero), chave, xml_url, danfe_url, json.dumps(raw))
    elif chave:
        await con.execute("""
        INSERT INTO public.omie_nfe (chave_nfe, xml, pdf_url, recebido_em, raw)
        VALUES ($1, $2, $3, NOW(), $4)
        ON CONFLICT (chave_nfe)
          DO UPDATE SET
            xml       = COALESCE(EXCLUDED.xml,      public.omie_nfe.xml),
            pdf_url   = COALESCE(EXCLUDED.pdf_url,  public.omie_nfe.pdf_url),
            recebido_em = NOW(),
            raw = EXCLUDED.raw;
        """, chave, xml_url, danfe_url, json.dumps(raw))

# =======================
# Fila de Jobs
# =======================
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
        WHERE status IN ('queued','retrying')
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
           next_run=NOW() + make_interval(secs => $4),
           updated_at=NOW()
     WHERE id=$1;
    """, job_id, attempts + 1, err[:8000], delay)

# =======================
# Omie – Consultar Pedido
# =======================
async def omie_consultar_pedido(id_pedido: Optional[int], numero: Optional[str]) -> Dict[str, Any]:
    """
    Ajuste o 'call' conforme seu contrato. Tentamos por id; fallback por número.
    """
    call = "ConsultarPedido" if id_pedido else "ListarPedidos"
    params = {"idPedido": id_pedido} if id_pedido else {"numero_pedido": numero}

    body = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [params],
    }

    attempts = 0
    async with httpx.AsyncClient(timeout=OMIE_TIMEOUT) as client:
        while attempts < 5:
            attempts += 1
            try:
                r = await client.post(f"{OMIE_BASE}/api/v1/produtos/pedido/", json=body)
                log.info("HTTP Omie %s -> %s", call, r.status_code)
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        await asyncio.sleep(int(ra))
                        continue
                if 500 <= r.status_code < 600:
                    await asyncio.sleep(backoff_seconds(attempts))
                    continue
                # 4xx (exceto 429): não insistir
                raise HTTPException(502, f"Omie respondeu {r.status_code}: {r.text[:300]}")
            except (httpx.TimeoutException, httpx.ConnectError):
                await asyncio.sleep(backoff_seconds(attempts))
            except Exception as e:
                raise e
    raise HTTPException(504, "Timeout consultando pedido na Omie")

# =======================
# Worker de Job
# =======================
async def process_enrich_pedido(con: asyncpg.Connection, payload: Dict[str, Any]) -> None:
    data = await omie_consultar_pedido(payload.get("id_pedido"), payload.get("numero"))
    # Atualiza o raw anexando a resposta da Omie
    await con.execute("""
    UPDATE public.omie_pedido
       SET raw = COALESCE(raw,'{}'::jsonb) || $2::jsonb,
           recebido_em = NOW()
     WHERE numero = $1 OR ($1 IS NULL AND id_pedido = $3)
    """, payload.get("numero"), json.dumps({"omie_response": data}), payload.get("id_pedido"))

# =======================
# Startup / Health
# =======================
@app.on_event("startup")
async def _startup():
    await ensure_schema()
    log.info("Inicialização OK.")

@app.get("/health/app")
async def health_app():
    return {"ok": True, "name": APP_NAME}

@app.get("/health/db")
async def health_db():
    pool = await get_pool()
    async with pool.acquire() as con:
        v = await con.fetchval("SELECT 1;")
    return {"db": "ok", "select1": v}

# =======================
# Webhook
# =======================
def normalize_pedido(evt: Dict[str, Any]) -> Dict[str, Any]:
    id_pedido = evt.get("idPedido") or evt.get("id_pedido")
    numero = evt.get("numeroPedido") or evt.get("numero_pedido") or evt.get("numero")
    valor = evt.get("valorPedido") or evt.get("valor_pedido")
    try:
        valor = float(str(valor).replace(",", ".")) if valor is not None else None
    except Exception:
        valor = None
    return {"id_pedido": id_pedido, "numero": str(numero) if numero else None, "valor_total": valor, "raw": evt}

def normalize_nfe(evt: Dict[str, Any]) -> Dict[str, Any]:
    numero = evt.get("numero_nf") or evt.get("numero")
    chave = evt.get("nfe_chave") or evt.get("chave_nfe") or evt.get("chave")
    xml_url = evt.get("nfe_xml") or evt.get("xml")
    danfe_url = evt.get("nfe_danfe") or evt.get("pdf_url")
    return {"numero": str(numero) if numero else None, "chave_nfe": str(chave) if chave else None,
            "xml_url": xml_url, "danfe_url": danfe_url, "raw": evt}

@app.post("/omie/webhook")
async def omie_webhook(req: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN:
        raise HTTPException(401, "token inválido")

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
                # Enfileira enriquecimento (consulta Omie)
                await enqueue_job(con, "enrich_pedido", {"id_pedido": p.get("id_pedido"), "numero": p.get("numero")})
                # opcional: enriquecer já (cuidado com 504)
                if ENRICH_PEDIDO_IMEDIATO:
                    try:
                        await process_enrich_pedido(con, {"id_pedido": p.get("id_pedido"), "numero": p.get("numero")})
                    except Exception as e:
                        log.exception("Enriquecimento imediato falhou: %s", e)
                return {"status": "success", "message": f"Pedido {p.get('numero') or p.get('id_pedido')} salvo/enfileirado."}

            elif topic == "NFe.NotaAutorizada":
                n = normalize_nfe(event)
                await upsert_nfe(con, n)
                return {"status": "success", "message": f"NF {n.get('numero') or n.get('chave_nfe')} salva/atualizada."}

            else:
                return {"status": "ignored", "message": f"Evento '{topic}' não suportado."}

# =======================
# Admin
# =======================
@app.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...), limit: int = Query(20, ge=1, le=200)):
    if secret != ADMIN_SECRET:
        raise HTTPException(401, "forbidden")

    processed = 0
    pool = await get_pool()
    async with pool.acquire() as con:
        while processed < limit:
            job = await fetch_next_job(con)
            if not job:
                break
            try:
                if job["job_type"] == "enrich_pedido":
                    await process_enrich_pedido(con, job["payload"])
                # finalize
                await mark_job_done(con, job["id"])
            except Exception as e:
                await mark_job_retry(con, job["id"], job["attempts"], str(e))
            processed += 1
    return {"processed": processed, "limit": limit, "time": now_utc().isoformat()}

@app.get("/admin/jobs-stats")
async def jobs_stats(secret: str = Query(...)):
    if secret != ADMIN_SECRET:
        raise HTTPException(401, "forbidden")
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT status, count(*) AS total
              FROM public.omie_jobs
             GROUP BY status
             ORDER BY status;
        """)
        return {"stats": [dict(r) for r in rows], "time": now_utc().isoformat()}
