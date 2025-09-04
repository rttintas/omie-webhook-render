# -*- coding: utf-8 -*-
# Omie Webhook API - FastAPI + asyncpg
from __future__ import annotations

import os
import json
import logging
import random
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse

# .env (opcional no local)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

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
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "false").lower() == "true"

OMIE_PEDIDO_ENDPOINT = "https://app.omie.com.br/api/v1/produtos/pedido/"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado.")

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("omie")

# ------------------------------------------------------------------------------
# App / singletons
# ------------------------------------------------------------------------------
app = FastAPI(title=APP_NAME)
_pool: Optional[asyncpg.pool.Pool] = None

# ------------------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------------------
def jlog(level: str, **data: Any) -> None:
    try:
        msg = json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        msg = str(data)
    getattr(logger, level, logger.info)(msg)

def _utcnow() -> datetime:
    return datetime.now(TZ)

def _norm_key(k: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", k) if unicodedata.category(c) != "Mn").lower()

def get_field(d: Dict[str, Any], *names: str, default: Any = None) -> Any:
    if not isinstance(d, dict):
        return default
    keys_map = {_norm_key(k): k for k in d.keys()}
    for name in names:
        nk = _norm_key(name)
        if nk in keys_map:
            return d[keys_map[nk]]
    return default

def _clean_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    return str(u).replace(" ", "").strip() or None

def _ensure_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return {}
    try:
        return dict(v)  # tipo jsonb que veio como mapeável
    except Exception:
        return {}

def _schedule_next_run(attempts: int) -> datetime:
    # 1, 5, 15, 60, 120 min
    minutes = [1, 5, 15, 60, 120]
    idx = min(max(0, attempts), len(minutes) - 1)
    return _utcnow() + timedelta(minutes=minutes[idx])

# ------------------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------------------
async def ensure_schema(conn: asyncpg.Connection) -> None:
    # Pedidos
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_pedido (
            id_pedido        BIGSERIAL PRIMARY KEY,
            numero           TEXT UNIQUE,
            id_pedido_omie   BIGINT,
            valor_total      NUMERIC(18,2),
            status           TEXT DEFAULT 'pendente_consulta',
            raw_basico       JSONB,
            raw_detalhe      JSONB,
            recebido_em      TIMESTAMPTZ DEFAULT now()
        );
    """)
    # Garante colunas (para DB já existente)
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS id_pedido_omie BIGINT;")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS valor_total NUMERIC(18,2);")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pendente_consulta';")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS raw_basico JSONB;")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS raw_detalhe JSONB;")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now();")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS numero TEXT;")
    await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_pedido_numero ON public.omie_pedido (numero);")

    # NF-e
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_nfe (
            id           BIGSERIAL PRIMARY KEY,
            numero       TEXT,
            chave_nfe    TEXT,
            danfe_url    TEXT,
            xml_url      TEXT,
            raw          JSONB,
            recebido_em  TIMESTAMPTZ DEFAULT now()
        );
    """)
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS numero TEXT;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS chave_nfe TEXT;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS danfe_url TEXT;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS xml_url TEXT;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS raw JSONB;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now();")
    await conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_nfe_chave
        ON public.omie_nfe (chave_nfe) WHERE chave_nfe IS NOT NULL;
    """)

    # Jobs
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_jobs (
            id          BIGSERIAL PRIMARY KEY,
            job_type    TEXT NOT NULL,
            payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
            status      TEXT NOT NULL DEFAULT 'pending',
            attempts    INT NOT NULL DEFAULT 0,
            last_error  TEXT,
            next_run    TIMESTAMPTZ NOT NULL DEFAULT now(),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS ix_omie_jobs_status_nextrun
        ON public.omie_jobs (status, next_run);
    """)

# ------------------------------------------------------------------------------
# Lifecycle
# ------------------------------------------------------------------------------
@app.on_event("startup")
async def startup() -> None:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with _pool.acquire() as conn:
        await conn.execute("SELECT 1;")
        await ensure_schema(conn)
    jlog("info", tag="startup", msg="Pool OK & schema verificado.")

@app.on_event("shutdown")
async def shutdown() -> None:
    if _pool:
        await _pool.close()

# ------------------------------------------------------------------------------
# Health
# ------------------------------------------------------------------------------
@app.get("/", response_class=PlainTextResponse)
async def root() -> str:
    return f"{APP_NAME} ok"

@app.get("/health/app")
async def health_app():
    return {"ok": True, "name": APP_NAME}

@app.get("/health/db")
async def health_db():
    if not _pool:
        raise HTTPException(500, "Pool indisponível")
    async with _pool.acquire() as conn:
        one = await conn.fetchval("SELECT 1;")
    return {"db": "ok", "select1": one}

# ------------------------------------------------------------------------------
# Omie API (Consulta Pedido) - usando codigo_pedido
# ------------------------------------------------------------------------------
async def omie_consultar_pedido(id_pedido: Optional[int],
                                numero_pedido: Optional[str]) -> Dict[str, Any]:
    """
    Consulta pedido na Omie.
    - Preferir 'codigo_pedido' (idPedido).
    - Fallback para 'numero_pedido' se não houver id.
    - Log detalhado em caso de erro (HTTP 500, faultstring etc).
    """
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
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "omie-webhook-render/1.0",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(OMIE_PEDIDO_ENDPOINT, headers=headers, json=body)

        jlog("info", tag="omie_http_call",
             status=r.status_code, url=OMIE_PEDIDO_ENDPOINT, req=body)

        txt = (r.text or "")[:1200]
        if r.status_code != 200:
            jlog("error", tag="omie_http_error", status=r.status_code, text=txt)
            raise RuntimeError(f"Omie HTTP {r.status_code}: {txt}")

        try:
            data = r.json()
        except Exception:
            jlog("error", tag="omie_json_parse_fail", text=txt)
            raise RuntimeError("Resposta 200 sem JSON parseável.")

        if isinstance(data, dict) and data.get("faultstring"):
            raise RuntimeError(f"Omie fault: {data.get('faultstring')}")
        return data

# ------------------------------------------------------------------------------
# Webhook helpers
# ------------------------------------------------------------------------------
async def _parse_payload(request: Request) -> Optional[dict]:
    ct = request.headers.get("Content-Type", "")
    try:
        if "application/json" in ct:
            j = await request.json()
            if isinstance(j, dict):
                return j
        elif "application/x-www-form-urlencoded" in ct:
            body = (await request.body()).decode("utf-8", "ignore")
            from urllib.parse import parse_qs
            form = parse_qs(body)
            for k in ("payload", "json", "body", "data"):
                if k in form:
                    try:
                        return json.loads(form[k][0])
                    except Exception:
                        pass
            # fallback melhor-esforço
            cand: Dict[str, Any] = {}
            for k in ("messageId", "topic", "evento", "event"):
                if k in form:
                    v = form[k][0]
                    if k in ("evento", "event"):
                        try:
                            cand[k] = json.loads(v)
                        except Exception:
                            cand[k] = v
                    else:
                        cand[k] = v
            if cand:
                return cand
        raw = (await request.body() or b"").decode("utf-8", "ignore").strip()
        if raw:
            return json.loads(raw)
    except Exception:
        rawp = (await request.body() or b"")[:1024]
        jlog("warning", tag="webhook_parse", msg="payload não parseável",
             headers=dict(request.headers), raw_preview=str(rawp))
    return None

def _evento(payload: Dict[str, Any]) -> Dict[str, Any]:
    ev = payload.get("evento") or payload.get("event")
    if isinstance(ev, str):
        try:
            ev = json.loads(ev)
        except Exception:
            ev = {"raw": ev}
    return ev if isinstance(ev, dict) else {}

def _extrai_pedido_e_nf(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[float], Dict[str, Any]]:
    ev = _evento(payload)
    numero_pedido = get_field(ev, "numeroPedido", "numero_pedido", "pedido")
    numero_nf     = get_field(ev, "numero_nf", "numeroNFe", "numero")
    id_pedido     = get_field(ev, "idPedido", "id_pedido", "codigo_pedido")
    valor_pedido  = get_field(ev, "valorPedido", "valor_pedido", "valor")

    try:
        id_pedido = int(id_pedido) if id_pedido not in (None, "", "None") else None
    except Exception:
        id_pedido = None

    try:
        if valor_pedido not in (None, ""):
            valor_pedido = float(str(valor_pedido).replace(",", "."))
        else:
            valor_pedido = None
    except Exception:
        valor_pedido = None

    numero_pedido = str(numero_pedido) if numero_pedido not in (None, "") else None
    numero_nf     = str(numero_nf) if numero_nf not in (None, "") else None
    return numero_pedido, numero_nf, id_pedido, valor_pedido, ev

# ------------------------------------------------------------------------------
# Webhook
# ------------------------------------------------------------------------------
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: Optional[str] = Query(default=None)):
    if not WEBHOOK_TOKEN or token != WEBHOOK_TOKEN:
        jlog("warning", tag="auth", msg="Token inválido em /omie/webhook", got=token)
        raise HTTPException(status_code=401, detail="unauthorized")

    payload = await _parse_payload(request)
    if not payload:
        raise HTTPException(400, "payload inválido")

    topic = (payload.get("topic") or payload.get("tópico") or "").strip().lower()
    numero_pedido, numero_nf, id_pedido, valor_pedido, ev = _extrai_pedido_e_nf(payload)
    raw_json = json.dumps(payload, ensure_ascii=False)

    if not _pool:
        raise HTTPException(500, "Pool indisponível")

    # --- VendaProduto.* ---
    if topic.startswith("vendaproduto."):
        async with _pool.acquire() as conn:
            status: Optional[str] = None
            # upsert básico
            await conn.execute("""
                INSERT INTO public.omie_pedido (numero, id_pedido_omie, valor_total, status, raw_basico, recebido_em)
                VALUES ($1, $2, $3, 'pendente_consulta', $4, now())
                ON CONFLICT (numero) DO UPDATE
                SET id_pedido_omie = COALESCE(EXCLUDED.id_pedido_omie, public.omie_pedido.id_pedido_omie),
                    valor_total    = COALESCE(EXCLUDED.valor_total,    public.omie_pedido.valor_total),
                    status         = COALESCE(EXCLUDED.status,         public.omie_pedido.status),
                    raw_basico     = EXCLUDED.raw_basico,
                    recebido_em    = now();
            """, numero_pedido, id_pedido, valor_pedido, raw_json)

            detailed = None
            if ENRICH_PEDIDO_IMEDIATO and (id_pedido or numero_pedido):
                try:
                    detailed = await omie_consultar_pedido(id_pedido, numero_pedido)
                    await conn.execute("""
                        UPDATE public.omie_pedido
                           SET raw_detalhe=$1, status='consultado'
                         WHERE numero=$2 OR id_pedido_omie=$3;
                    """, json.dumps(detailed, ensure_ascii=False), numero_pedido, id_pedido)
                except Exception as e:
                    status = "pendente_consulta"
                    await conn.execute("""
                        INSERT INTO public.omie_jobs (job_type, payload, status, attempts, last_error, next_run)
                        VALUES ('pedido.consultar', $1, 'pending', 0, $2, now());
                    """, json.dumps({"id_pedido": id_pedido, "numero_pedido": numero_pedido}, ensure_ascii=False),
                         str(e)[:800])
            else:
                status = "pendente_consulta"
                await conn.execute("""
                    INSERT INTO public.omie_jobs (job_type, payload, status, attempts, next_run)
                    VALUES ('pedido.consultar', $1, 'pending', 0, now());
                """, json.dumps({"id_pedido": id_pedido, "numero_pedido": numero_pedido}, ensure_ascii=False))

            return {
                "status": "success",
                "message": f"Pedido {numero_pedido or id_pedido} salvo/atualizado.",
                "consulta_imediata_ok": bool(detailed)
            }

    # --- NFe.NotaAutorizada ---
    elif topic in {"nfe.notaautorizada", "nfe.nota_autorizada", "nfe.autorizada"}:
        nfe_chave = get_field(ev, "nfe_chave", "chave_nfe", "chave", "chNFe")
        nfe_xml   = _clean_url(get_field(ev, "nfe_xml", "xml_url", "xml"))
        nfe_danfe = _clean_url(get_field(ev, "nfe_danfe", "danfe", "danfe_url", "pdf_url"))
        numero_nf = numero_nf or get_field(ev, "numero_nf", "numeroNFe", "numero")

        async with _pool.acquire() as conn:
            if nfe_chave:
                await conn.execute("""
                    INSERT INTO public.omie_nfe (chave_nfe, numero, xml_url, danfe_url, raw, recebido_em)
                    VALUES ($1, $2, $3, $4, $5, now())
                    ON CONFLICT (chave_nfe) DO UPDATE
                    SET numero      = COALESCE(EXCLUDED.numero, public.omie_nfe.numero),
                        xml_url     = COALESCE(EXCLUDED.xml_url, public.omie_nfe.xml_url),
                        danfe_url   = COALESCE(EXCLUDED.danfe_url, public.omie_nfe.danfe_url),
                        raw         = EXCLUDED.raw,
                        recebido_em = now();
                """, str(nfe_chave), str(numero_nf) if numero_nf else None, nfe_xml, nfe_danfe, raw_json)
                alvo = nfe_chave
            else:
                await conn.execute("""
                    INSERT INTO public.omie_nfe (numero, xml_url, danfe_url, raw, recebido_em)
                    VALUES ($1, $2, $3, $4, now())
                    ON CONFLICT (numero) DO UPDATE
                    SET xml_url     = COALESCE(EXCLUDED.xml_url, public.omie_nfe.xml_url),
                        danfe_url   = COALESCE(EXCLUDED.danfe_url, public.omie_nfe.danfe_url),
                        raw         = EXCLUDED.raw,
                        recebido_em = now();
                """, str(numero_nf) if numero_nf else None, nfe_xml, nfe_danfe, raw_json)
                alvo = numero_nf
        return {"status": "success", "message": f"NF {alvo} salva/atualizada."}

    # --- não suportado ---
    else:
        return {"status": "ignored", "message": f"Tipo de evento '{topic}' não suportado."}

# ------------------------------------------------------------------------------
# Admin – ping direto na Omie (diagnóstico)
# ------------------------------------------------------------------------------
@app.get("/admin/ping-omie")
async def ping_omie(secret: str = Query(...),
                    codigo_pedido: Optional[int] = Query(None),
                    numero_pedido: Optional[str] = Query(None)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    if not codigo_pedido and not numero_pedido:
        raise HTTPException(400, "informe codigo_pedido OU numero_pedido")
    try:
        data = await omie_consultar_pedido(codigo_pedido, numero_pedido)
        return {"ok": True, "preview": str(data)[:1200]}
    except Exception as e:
        raise HTTPException(502, f"erro ao consultar omie: {e}")

# ------------------------------------------------------------------------------
# Admin – rodar fila
# ------------------------------------------------------------------------------
@app.post("/admin/run-jobs")
async def admin_run_jobs(secret: str = Query(...), limit: int = Query(20, ge=1, le=200)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    if not _pool:
        raise HTTPException(500, "Pool indisponível")

    processed = 0
    async with _pool.acquire() as conn:
        rows: List[asyncpg.Record] = await conn.fetch("""
            SELECT * FROM public.omie_jobs
             WHERE status='pending' AND next_run <= now()
             ORDER BY next_run ASC
             LIMIT $1;
        """, limit)

        for job in rows:
            job_id   = job["id"]
            jtype    = job["job_type"]
            attempts = int(job["attempts"]) + 1
            payload  = _ensure_dict(job.get("payload"))

            try:
                if jtype == "pedido.consultar":
                    id_ped  = payload.get("id_pedido")
                    num_ped = payload.get("numero_pedido")
                    data = await omie_consultar_pedido(id_ped, num_ped)
                    await conn.execute("""
                        UPDATE public.omie_pedido
                           SET raw_detalhe=$1, status='consultado', recebido_em=now()
                         WHERE numero = $2 OR id_pedido_omie = $3;
                    """, json.dumps(data, ensure_ascii=False), num_ped, id_ped)
                    await conn.execute("""
                        UPDATE public.omie_jobs
                           SET status='done', attempts=$2, last_error=NULL, updated_at=now()
                         WHERE id=$1;
                    """, job_id, attempts)
                else:
                    await conn.execute("""
                        UPDATE public.omie_jobs
                           SET status='done', attempts=$2, last_error='tipo_desconhecido', updated_at=now()
                         WHERE id=$1;
                    """, job_id, attempts)

                jlog("info", tag="job_done", id=job_id, job_type=jtype)
                processed += 1

            except Exception as e:
                next_run = _schedule_next_run(attempts)
                await conn.execute("""
                    UPDATE public.omie_jobs
                       SET attempts=$2, next_run=$3, last_error=$4, updated_at=now()
                     WHERE id=$1;
                """, job_id, attempts, next_run, str(e)[:800])
                jlog("error", tag="job_fail", id=job_id, error=str(e))

    return {"ok": True, "processed": processed, "time": _utcnow().isoformat()}

# ------------------------------------------------------------------------------
# Admin – stats
# ------------------------------------------------------------------------------
@app.get("/admin/jobs-stats")
async def jobs_stats(secret: str = Query(...)):
    if secret != ADMIN_SECRET:
        raise HTTPException(401, "forbidden")
    if not _pool:
        raise HTTPException(500, "Pool indisponível")
    async with _pool.acquire() as conn:
        stats = await conn.fetch("""
            SELECT status, count(*) AS total
              FROM public.omie_jobs
             GROUP BY status
             ORDER BY status;
        """)
        return {"stats": [dict(r) for r in stats], "time": _utcnow().isoformat()}
