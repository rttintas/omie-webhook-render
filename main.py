# main.py — Omie Webhook API (FastAPI + asyncpg + httpx)
import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
APP_NAME = "Omie Webhook API"
TZ = timezone.utc

DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")

# se true, tenta enriquecer o pedido imediatamente; senão, só enfileira
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "false").lower() == "true"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado.")

OMIE_PEDIDO_ENDPOINT = "https://app.omie.com.br/api/v1/produtos/pedido/"

# ------------------------------------------------------------------------------
# App & Logger
# ------------------------------------------------------------------------------
app = FastAPI(title=APP_NAME)
logger = logging.getLogger("uvicorn")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def jlog(level: str, **data):
    msg = json.dumps(data, ensure_ascii=False, default=str)
    getattr(logger, level, logger.info)(msg)

# ------------------------------------------------------------------------------
# DB
# ------------------------------------------------------------------------------
pool: Optional[asyncpg.pool.Pool] = None

async def db_init_schema():
    async with pool.acquire() as conn:
        # pedidos
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_pedido (
            id_pedido      BIGSERIAL PRIMARY KEY,
            numero         TEXT UNIQUE,
            id_pedido_omie BIGINT,
            valor_total    NUMERIC,
            status         TEXT DEFAULT 'pendente_consulta',
            raw_basico     JSONB,
            raw_detalhe    JSONB,
            recebido_em    TIMESTAMPTZ DEFAULT now()
        );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_omie_pedido_numero
            ON public.omie_pedido (numero);
        """)

        # nfe (mantém compatível com o que você já tem)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_nfe (
            chave_nfe      TEXT,
            numero         TEXT,
            serie          TEXT,
            emitida_em     TIMESTAMPTZ,
            cnpj_emitente TEXT,
            cnpj_destinatario TEXT,
            valor_total    NUMERIC,
            status         TEXT,
            xml            TEXT,
            pdf_url        TEXT,
            last_event_at TIMESTAMPTZ,
            updated_at     TIMESTAMPTZ,
            data_emissao   TEXT,
            cliente_nome   TEXT,
            raw            JSONB,
            created_at     TIMESTAMPTZ DEFAULT now(),
            recebido_em    TIMESTAMPTZ DEFAULT now(),
            id_nfe         BIGINT,
            danfe_url      TEXT,
            xml_url        TEXT
        );
        """)

        # jobs
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_jobs (
            id         BIGSERIAL PRIMARY KEY,
            job_type   TEXT NOT NULL,
            status     TEXT NOT NULL DEFAULT 'pending',
            attempts   INT  NOT NULL DEFAULT 0,
            next_run   TIMESTAMPTZ DEFAULT now(),
            last_error TEXT,
            payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_omie_jobs_status_next_run
            ON public.omie_jobs(status, next_run);
        """)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _ensure_dict(x: Any) -> Dict[str, Any]:
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return {}
    try:
        return dict(x)  # JSONB do asyncpg já costuma vir como dict
    except Exception:
        return {}

async def upsert_pedido_basico(
    numero: Optional[str], id_pedido: Optional[int],
    valor: Optional[float], raw: Dict[str, Any]
):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_pedido
                (numero, id_pedido_omie, valor_total, status, raw_basico, recebido_em)
            VALUES ($1, $2, $3, 'pendente_consulta', $4::jsonb, now())
            ON CONFLICT (numero) DO UPDATE SET
                id_pedido_omie = EXCLUDED.id_pedido_omie,
                valor_total    = COALESCE(EXCLUDED.valor_total, public.omie_pedido.valor_total),
                raw_basico     = EXCLUDED.raw_basico,
                recebido_em    = now()
        """, numero, id_pedido, valor, json.dumps(raw, ensure_ascii=False))

async def enqueue_job(job_type: str, payload: Dict[str, Any], delay_seconds: int = 0):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.omie_jobs (job_type, status, attempts, next_run, payload, created_at, updated_at)
            VALUES ($1, 'pending', 0, now() + ($2 || ' seconds')::interval, $3::jsonb, now(), now())
        """, job_type, delay_seconds, json.dumps(payload, ensure_ascii=False))

async def pick_pending_jobs(limit: int = 20) -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT id, job_type, status, attempts, next_run, last_error, payload
              FROM public.omie_jobs
             WHERE status='pending' AND next_run <= now()
             ORDER BY next_run ASC
             LIMIT $1
        """, limit)

async def update_job_done(job_id: int):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE public.omie_jobs
               SET status='done', updated_at=now(), last_error=NULL
             WHERE id=$1
        """, job_id)

async def update_job_fail(job_id: int, err: str):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE public.omie_jobs
               SET attempts = attempts + 1,
                   last_error = $1,
                   updated_at = now(),
                   next_run = now() + interval '2 minutes'
             WHERE id=$2
        """, err[:800], job_id)

async def set_pedido_detalhe(numero: str, detalhe: Dict[str, Any]):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE public.omie_pedido
               SET raw_detalhe = $1::jsonb,
                   status='consultado'
             WHERE numero=$2
        """, json.dumps(detalhe, ensure_ascii=False), numero)

# ------------------------------------------------------------------------------
# Omie API — usa codigo_pedido (idPedido do evento). Fallback para numero_pedido.
# ------------------------------------------------------------------------------
async def omie_consultar_pedido(id_pedido: Optional[int], numero_pedido: Optional[str]) -> Dict[str, Any]:
    if not OMIE_APP_KEY or not OMIE_APP_SECRET:
        raise RuntimeError("OMIE_APP_KEY/OMIE_APP_SECRET não configurados.")

    param: Dict[str, Any] = {}
    if id_pedido:
        param["codigo_pedido"] = int(id_pedido)
    elif numero_pedido:
        param["numero_pedido"] = str(numero_pedido)
    else:
        raise RuntimeError("Sem id_pedido (codigo_pedido) e sem numero_pedido.")

    body = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [param],
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(OMIE_PEDIDO_ENDPOINT, json=body)
            
            # Lança um erro se o status for 4xx/5xx, o que é tratado abaixo
            r.raise_for_status()

            data = r.json()
            # Tratamento de fault do Omie (SOAP-style)
            if isinstance(data, dict) and data.get("faultstring"):
                raise RuntimeError(f"Omie fault: {data.get('faultstring')}")
            return data
            
    except httpx.TimeoutException as e:
        # Erro de timeout (a requisição demorou demais)
        raise RuntimeError(f"Timeout ao conectar com a API Omie: {e}")
    except httpx.HTTPStatusError as e:
        # Erro de status HTTP (4xx ou 5xx) vindo da Omie
        jlog("error", tag="omie_api_error_body", status_code=e.response.status_code, response_body=e.response.text)
        # Re-lança a exceção original para que o job falhe e seja re-tentado
        raise e
    except Exception as e:
        # Qualquer outro tipo de erro
        raise e

# ------------------------------------------------------------------------------
# Lifecycle
# ------------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await db_init_schema()
    jlog("info", tag="startup", msg="Pool OK & schema verificado.")

# ------------------------------------------------------------------------------
# Health
# ------------------------------------------------------------------------------
@app.get("/health/app")
async def health_app():
    return {"ok": True, "name": APP_NAME}

@app.get("/health/db")
async def health_db():
    try:
        async with pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1;")
        return {"db": "ok", "select1": val}
    except Exception as e:
        return JSONResponse(status_code=500, content={"db": "error", "detail": str(e)})

# ------------------------------------------------------------------------------
# Webhook (Pedidos + NFe simples)
# ------------------------------------------------------------------------------
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: Optional[str] = Query(default=None)):
    if token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

    # Aceita JSON puro ou form com campo "json"
    try:
        if "application/json" in (request.headers.get("content-type") or ""):
            payload = await request.json()
        else:
            form = await request.form()
            if "json" in form:
                payload = json.loads(form["json"])
            else:
                payload = json.loads((await request.body()).decode() or "{}")
    except Exception:
        raise HTTPException(status_code=400, detail="payload parse error")

    topic = (payload.get("topic") or payload.get("tópico") or "").strip()
    event = _ensure_dict(payload.get("event") or payload.get("evento") or {})

    # Campos do evento de VendaProduto
    numero_pedido = str(event.get("numeroPedido") or event.get("numero_pedido") or "" or None)
    _id_pedido = event.get("idPedido") or event.get("id_pedido")
    try:
        id_pedido_omie = int(_id_pedido) if _id_pedido not in (None, "", "None") else None
    except Exception:
        id_pedido_omie = None

    try:
        valor_pedido = float(event.get("valorPedido") or event.get("valor_pedido") or 0) or None
    except Exception:
        valor_pedido = None

    # Upsert básico do pedido
    await upsert_pedido_basico(numero_pedido or None, id_pedido_omie, valor_pedido, payload)

    # Se for evento de venda → enriquece (imediato ou via fila)
    if topic.lower().startswith("vendaproduto."):
        job_payload = {"id_pedido": id_pedido_omie, "numero_pedido": numero_pedido}
        if ENRICH_PEDIDO_IMEDIATO and (id_pedido_omie or numero_pedido):
            try:
                data = await omie_consultar_pedido(id_pedido_omie, numero_pedido)
                await set_pedido_detalhe(numero_pedido, data)
                jlog("info", tag="webhook_enriched_immediate", numero=numero_pedido, id_pedido=id_pedido_omie)
            except Exception as e:
                jlog("warning", tag="webhook_enrich_fail", numero=numero_pedido, id_pedido=id_pedido_omie, error=str(e))
                await enqueue_job("pedido.consultar", job_payload)
        else:
            await enqueue_job("pedido.consultar", job_payload)

        jlog("info", tag="omie_webhook_received",
              numero_pedido=numero_pedido, id_pedido=id_pedido_omie)
        return {"ok": True, "numero_pedido": numero_pedido, "id_pedido": id_pedido_omie}

    # Evento de NFe simples (salva links)
    if topic.lower() in {"nfe.notaautorizada", "nfe.nota_autorizada"}:
        nfe_chave = str(event.get("nfe_chave") or event.get("chNFe") or "" or None)
        nfe_xml    = str(event.get("nfe_xml") or event.get("xml") or "" or None)
        nfe_danfe = str(event.get("nfe_danfe") or event.get("danfe") or event.get("pdf_url") or "" or None)
        numero_nf = str(event.get("numero_nf") or event.get("numero") or "" or None)

        async with pool.acquire() as conn:
            if nfe_chave:
                await conn.execute("""
                    INSERT INTO public.omie_nfe (chave_nfe, numero, xml_url, danfe_url, raw, recebido_em)
                    VALUES ($1, $2, $3, $4, $5::jsonb, now())
                    ON CONFLICT (chave_nfe) DO UPDATE SET
                        numero      = COALESCE(EXCLUDED.numero, public.omie_nfe.numero),
                        xml_url     = COALESCE(EXCLUDED.xml_url, public.omie_nfe.xml_url),
                        danfe_url   = COALESCE(EXCLUDED.danfe_url, public.omie_nfe.danfe_url),
                        raw         = EXCLUDED.raw,
                        recebido_em = now();
                """, nfe_chave, numero_nf, nfe_xml, nfe_danfe, json.dumps(payload, ensure_ascii=False))
            else:
                await conn.execute("""
                    INSERT INTO public.omie_nfe (numero, xml_url, danfe_url, raw, recebido_em)
                    VALUES ($1, $2, $3, $4::jsonb, now())
                    ON CONFLICT (numero) DO UPDATE SET
                        xml_url     = COALESCE(EXCLUDED.xml_url, public.omie_nfe.xml_url),
                        danfe_url   = COALESCE(EXCLUDED.danfe_url, public.omie_nfe.danfe_url),
                        raw         = EXCLUDED.raw,
                        recebido_em = now();
                """, numero_nf, nfe_xml, nfe_danfe, json.dumps(payload, ensure_ascii=False))
        return {"ok": True, "nf": numero_nf or nfe_chave}

    # Demais tópicos: ignora
    return {"status": "ignored", "topic": topic}

# ------------------------------------------------------------------------------
# Admin: rodar fila
# ------------------------------------------------------------------------------
@app.post("/admin/run-jobs")
async def admin_run_jobs(secret: Optional[str] = Query(default=None), limit: int = 20):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    jobs = await pick_pending_jobs(limit)
    ran = 0

    for job in jobs:
        job_id = job["id"]
        jtype = job["job_type"]
        payload = _ensure_dict(job["payload"])

        try:
            if jtype == "pedido.consultar":
                numero = payload.get("numero_pedido")
                id_ped = payload.get("id_pedido")
                data = await omie_consultar_pedido(id_ped, numero)
                await set_pedido_detalhe(numero, data)
            else:
                jlog("warning", tag="job_unknown", id=job_id, job_type=jtype)

            await update_job_done(job_id)
            jlog("info", tag="job_done", id=job_id)
            ran += 1

        except Exception as e:
            await update_job_fail(job_id, str(e))
            jlog("error", tag="job_fail", id=job_id, error=str(e))

    return {"ok": True, "picked": len(jobs), "ran": ran}

# ------------------------------------------------------------------------------
# Admin: stats
# ------------------------------------------------------------------------------
@app.get("/admin/jobs-stats")
async def jobs_stats(secret: Optional[str] = Query(default=None)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT status, COUNT(*) AS qnt
              FROM public.omie_jobs
             GROUP BY status
             ORDER BY status;
        """)
    return {"ok": True, "by_status": [{"status": r["status"], "qnt": int(r["qnt"])} for r in rows]}