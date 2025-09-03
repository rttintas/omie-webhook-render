# main.py
# FastAPI + asyncpg — Webhook Omie (Render)
# - Opera em modo degradado com fila para enriquecimento.
# - Fila simples em omie_jobs para reprocessamento.
# - Endpoint /jobs/run para processar a fila.
# - Retry/backoff e fallback em caso de falha.
# - Dedupe por id_pedido.

from __future__ import annotations

import os
import json
import logging
import asyncio
import random
import unicodedata
from datetime import timedelta, datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse

try:
    # opcional (local): carrega .env
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

APP_NAME = "Omie Webhook API"
OMIE_BASE = "https://api.omie.com.br"
OMIE_PEDIDO_PATH = "/api/v1/produtos/pedido/"
ENRICH_IMMEDIATE_FLAG = os.environ.get("ENRICH_PEDIDO_IMEDIATO", "true").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("omie")
app = FastAPI(title=APP_NAME)

_pool: Optional[asyncpg.pool.Pool] = None
_http: Optional[httpx.AsyncClient] = None


# ------------------------- env / helpers ------------------------- #
def get_env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise RuntimeError(f"Variável de ambiente ausente: {name}")
    return v

def _norm_key(k: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", k) if unicodedata.category(c) != "Mn").lower()

def get_field(d: Dict[str, Any], *names: str, default: Any = None) -> Any:
    if not isinstance(d, dict):
        return default
    keys_map = { _norm_key(k): k for k in d.keys() }
    for name in names:
        nk = _norm_key(name)
        if nk in keys_map:
            return d[keys_map[nk]]
    return default

def _clean_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    return str(u).replace(" ", "").strip() or None

def _topic(value: str) -> str:
    return (value or "").strip().lower()

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

# backoff progressivo com jitter
def _next_delay_seconds(attempts: int) -> float:
    # 1s, 2s, 4s, 8s, 15s (cap) + jitter 0–0.3
    base = min(1 * (2 ** max(0, attempts - 1)), 15)
    return base + random.random() * 0.3


# ------------------------- schema ------------------------- #
async def ensure_schema(conn: asyncpg.Connection) -> None:
    # Pedidos
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_pedido (
            id_pedido         BIGSERIAL PRIMARY KEY,
            numero            TEXT UNIQUE,
            id_pedido_omie    BIGINT,
            valor_total       NUMERIC(18,2),
            status            TEXT,
            raw               JSONB,
            recebido_em       TIMESTAMPTZ DEFAULT now()
        );
    """)
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS id_pedido_omie BIGINT;")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS valor_total NUMERIC(18,2);")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS status TEXT;")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS raw JSONB;")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now();")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS numero TEXT;")
    await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_pedido_numero ON public.omie_pedido (numero);")

    # NF-e
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_nfe (
            id                BIGSERIAL PRIMARY KEY,
            numero            TEXT UNIQUE,
            chave_nfe         TEXT,
            danfe_url         TEXT,
            xml_url           TEXT,
            raw               JSONB,
            recebido_em       TIMESTAMPTZ DEFAULT now()
        );
    """)
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS chave_nfe TEXT;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS danfe_url TEXT;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS xml_url TEXT;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS raw JSONB;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now();")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS numero TEXT;")
    # Único parcial para permitir vários NULLs
    await conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_nfe_chave
        ON public.omie_nfe (chave_nfe) WHERE chave_nfe IS NOT NULL;
    """)

    # Jobs (reprocessamento)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_jobs (
            id          BIGSERIAL PRIMARY KEY,
            job_type    TEXT NOT NULL,
            payload     JSONB NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            attempts    INT NOT NULL DEFAULT 0,
            last_error  TEXT,
            next_run    TIMESTAMPTZ NOT NULL DEFAULT now(),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS ix_omie_jobs_status_nextrun ON public.omie_jobs (status, next_run);")


# ------------------------- lifecycle ------------------------- #
@app.on_event("startup")
async def startup() -> None:
    global _pool, _http
    db_url = get_env("DATABASE_URL")
    _pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
    _http = httpx.AsyncClient(base_url=OMIE_BASE, timeout=60.0)
    async with _pool.acquire() as conn:
        await conn.execute("SELECT 1;")
        await ensure_schema(conn)
    logger.info("Pool OK & schema verificado.")


@app.on_event("shutdown")
async def shutdown() -> None:
    if _pool:
        await _pool.close()
    if _http:
        await _http.aclose()


# ------------------------- health ------------------------- #
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


# ------------------------- parse payload ------------------------- #
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
        logger.warning("Webhook sem JSON parseável. headers=%s raw-preview=%r",
                       dict(request.headers), rawp)
    return None


def _evento(payload: Dict[str, Any]) -> Dict[str, Any]:
    ev = payload.get("evento")
    if ev is None:
        ev = payload.get("event")
    if isinstance(ev, str):
        try:
            ev = json.loads(ev)
        except Exception:
            ev = {"raw": ev}
    return ev if isinstance(ev, dict) else {}


def _extrai_pedido_e_nf(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[float], Dict[str, Any]]:
    ev = _evento(payload)
    numero_pedido = get_field(ev, "numeroPedido", "numero_pedido", "númeroPedido", "pedido")
    numero_nf       = get_field(ev, "numero_nf", "numeroNFe", "numero", "número")
    id_pedido       = get_field(ev, "idPedido", "id_pedido")
    valor_pedido = get_field(ev, "valorPedido", "valor_pedido", "valor")
    try:
        id_pedido = int(id_pedido) if id_pedido is not None else None
    except Exception:
        id_pedido = None
    try:
        valor_pedido = float(str(valor_pedido).replace(",", ".")) if valor_pedido is not None else None
    except Exception:
        valor_pedido = None
    numero_pedido = str(numero_pedido) if numero_pedido is not None else None
    numero_nf       = str(numero_nf) if numero_nf is not None else None
    return numero_pedido, numero_nf, id_pedido, valor_pedido, ev


# ------------------------- Omie API com retry ------------------------- #
async def consultar_pedido_omie(id_pedido: int) -> Optional[Dict[str, Any]]:
    """Chama ConsultarPedido com retry + backoff + jitter.
    - Respeita Retry-After em 429
    - Tenta novamente em 5xx/timeout
    """
    app_key = get_env("OMIE_APP_KEY")
    app_secret = get_env("OMIE_APP_SECRET")

    payload = {
        "call": "ConsultarPedido",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [{
            # enviamos os dois para cobrir variações
            "idPedido": id_pedido,
            "codigo_pedido": id_pedido
        }]
    }

    if not _http:
        raise RuntimeError("Cliente HTTP indisponível")

    attempts = 0
    while attempts < 5:
        if attempts:
            # backoff com jitter
            await asyncio.sleep(_next_delay_seconds(attempts))
        attempts += 1
        try:
            r = await _http.post(OMIE_PEDIDO_PATH, json=payload)
            logger.info("Requisição HTTP: POST %s %r", OMIE_PEDIDO_PATH, r.status_code)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    logger.warning("Resposta 200 sem JSON parseável: %r", r.text[:500])
                    return None

            if r.status_code == 429:
                # respeita Retry-After se presente
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        wait = int(ra)
                        await asyncio.sleep(wait)
                        continue
                    except Exception:
                        pass
                # sem Retry-After, cai no backoff padrão
                continue

            if 500 <= r.status_code < 600:
                # Omie instável: tenta de novo
                continue

            # 4xx diferente de 429: não insiste
            logger.info("Parando retry (status %s). Body: %s", r.status_code, r.text[:500])
            return None

        except (httpx.TimeoutException, httpx.ReadTimeout, httpx.ConnectError) as e:
            logger.info("Timeout/Conexão (%s). Tentando de novo...", e.__class__.__name__)
            continue
        except Exception as e:
            logger.error("Erro inesperado na chamada Omie: %s", e)
            return None

    return None


# ------------------------- jobs helpers ------------------------- #
async def _enqueue_job(conn: asyncpg.Connection, job_type: str, payload: Dict[str, Any], err: Optional[str] = None) -> None:
    await conn.execute("""
        INSERT INTO public.omie_jobs (job_type, payload, status, attempts, last_error, next_run, created_at, updated_at)
        VALUES ($1, $2, 'pending', 0, $3, now(), now(), now());
    """, job_type, json.dumps(payload, ensure_ascii=False), err)

def _schedule_next_run(attempts: int) -> datetime:
    # 1min, 5min, 15min, 60min, 2h (cap)
    minutes = [1, 5, 15, 60, 120]
    idx = min(attempts, len(minutes)-1)
    return _utcnow() + timedelta(minutes=minutes[idx])

async def _run_single_job(conn: asyncpg.Connection, job: asyncpg.Record) -> None:
    job_id = job["id"]
    payload = job["payload"]
    jtype = job["job_type"]
    attempts = job["attempts"] + 1

    try:
        if jtype == "pedido.consultar":
            id_pedido = payload.get("id_pedido")
            numero_pedido = payload.get("numero_pedido")
            detailed = await consultar_pedido_omie(int(id_pedido))
            if detailed:
                raw_data = json.dumps(detailed, ensure_ascii=False)
                await conn.execute("""
                    UPDATE public.omie_pedido
                       SET raw = $1, status = NULL, recebido_em = now()
                     WHERE numero = $2 OR id_pedido_omie = $3;
                """, raw_data, numero_pedido, id_pedido)

                await conn.execute("""
                    UPDATE public.omie_jobs
                       SET status='done', attempts=$2, updated_at=now()
                     WHERE id=$1;
                """, job_id, attempts)
            else:
                # reprograma
                next_run = _schedule_next_run(attempts)
                await conn.execute("""
                    UPDATE public.omie_jobs
                       SET attempts=$2, next_run=$3, last_error='consulta sem sucesso', updated_at=now()
                     WHERE id=$1;
                """, job_id, attempts, next_run)
        else:
            await conn.execute("""
                UPDATE public.omie_jobs
                   SET status='done', attempts=$2, last_error='tipo desconhecido', updated_at=now()
                 WHERE id=$1;
            """, job_id, attempts)

    except Exception as e:
        next_run = _schedule_next_run(attempts)
        await conn.execute("""
            UPDATE public.omie_jobs
               SET attempts=$2, next_run=$3, last_error=$4, updated_at=now()
             WHERE id=$1;
        """, job_id, attempts, next_run, str(e)[:800])


# ------------------------- webhook ------------------------- #
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str):
    expected = get_env("WEBHOOK_TOKEN")
    if token != expected:
        raise HTTPException(status_code=401, detail="Token inválido.")

    payload = await _parse_payload(request)
    if not payload:
        raise HTTPException(status_code=400, detail="Payload inválido ou vazio.")

    logger.info("Payload recebido: %s", json.dumps(payload, ensure_ascii=False))

    topic = _topic(payload.get("topic", ""))

    numero_pedido, numero_nf, id_pedido, valor_pedido, evento = _extrai_pedido_e_nf(payload)
    raw_data = json.dumps(payload, ensure_ascii=False)

    if not _pool:
        raise HTTPException(500, "Pool indisponível")

    # ----- Pedido de Venda -----
    if topic.startswith("vendaproduto."):
        async with _pool.acquire() as conn:
            detailed = None
            status: Optional[str] = None
            
            # Tenta consultar imediatamente se o flag estiver ligado, ou enfileira
            if id_pedido and ENRICH_IMMEDIATE_FLAG:
                logger.info("Chamando Omie API para consultar o pedido: %s", id_pedido)
                detailed = await consultar_pedido_omie(id_pedido)
                if detailed:
                    raw_data = json.dumps(detailed, ensure_ascii=False)
                else:
                    status = "pendente_consulta"
                    await _enqueue_job(conn, "pedido.consultar",
                                       {"id_pedido": id_pedido, "numero_pedido": numero_pedido},
                                       err="consulta inicial falhou")
            elif id_pedido:
                # Se ENRICH_IMMEDIATE_FLAG for false, enfileira o job
                status = "pendente_consulta"
                await _enqueue_job(conn, "pedido.consultar",
                                    {"id_pedido": id_pedido, "numero_pedido": numero_pedido})
            else:
                # Se id_pedido ausente, enfileira o job
                status = "pendente_consulta"
                await _enqueue_job(conn, "pedido.consultar",
                                    {"id_pedido": id_pedido, "numero_pedido": numero_pedido})
                
            # fallback: não perder evento
            await conn.execute("""
                INSERT INTO public.omie_pedido (numero, id_pedido_omie, valor_total, status, raw, recebido_em)
                VALUES ($1, $2, $3, $4, $5, now())
                ON CONFLICT (numero) DO UPDATE
                SET id_pedido_omie = COALESCE(EXCLUDED.id_pedido_omie, public.omie_pedido.id_pedido_omie),
                    valor_total  = COALESCE(EXCLUDED.valor_total,    public.omie_pedido.valor_total),
                    status       = COALESCE(EXCLUDED.status,         public.omie_pedido.status),
                    raw          = EXCLUDED.raw,
                    recebido_em  = now();
            """, numero_pedido, id_pedido, valor_pedido, status, raw_data)

        return {"status": "success",
                "message": f"Pedido {numero_pedido or id_pedido} salvo/atualizado.",
                "consulta_imediata_ok": bool(detailed)}

    # ----- NF-e -----
    elif topic in {"nfe.notaautorizada", "nfe.nota_autorizada", "nfe.autorizada"}:
        nfe_chave = get_field(evento, "nfe_chave", "chave_nfe", "chave", "chNFe")
        nfe_xml   = _clean_url(get_field(evento, "nfe_xml", "xml_url", "xml"))
        nfe_danfe = _clean_url(get_field(evento, "nfe_danfe", "danfe_url", "pdf_url"))
        numero_nf = numero_nf or get_field(evento, "numero_nf", "numeroNFe", "numero")

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
                """, str(nfe_chave), str(numero_nf) if numero_nf else None, nfe_xml, nfe_danfe, raw_data)
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
                """, str(numero_nf) if numero_nf else None, nfe_xml, nfe_danfe, raw_data)
                alvo = numero_nf

        return {"status": "success", "message": f"NF {alvo} salva/atualizada."}

    # ----- Não suportado -----
    else:
        return {"status": "ignored", "message": f"Tipo de evento '{topic}' não suportado."}


# ------------------------- admin: rodar jobs ------------------------- #
@app.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...), limit: int = Query(20, ge=1, le=200)):
    if secret != get_env("ADMIN_SECRET"):
        raise HTTPException(401, "forbidden")
    
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
            await _run_single_job(conn, job)
            processed += 1

    return {"processed": processed, "limit": limit, "time": _utcnow().isoformat()}

@app.get("/admin/jobs-stats")
async def jobs_stats(secret: str = Query(...)):
    if secret != get_env("ADMIN_SECRET"):
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