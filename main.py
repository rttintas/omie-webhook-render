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
import unicodedata
from typing import Any, Dict, Optional, Tuple

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException
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
    await conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_nfe_chave
        ON public.omie_nfe (chave_nfe) WHERE chave_nfe IS NOT NULL;
    """)
    
    # Nova tabela para a fila de enriquecimento
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.omie_jobs (
            id_job        BIGSERIAL PRIMARY KEY,
            id_pedido     BIGINT NOT NULL,
            status        TEXT NOT NULL,
            tentativas    INTEGER DEFAULT 0,
            criado_em     TIMESTAMPTZ DEFAULT now(),
            processado_em TIMESTAMPTZ
        );
    """)


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
    """Chama ConsultarPedido com retry + backoff em 504/timeout/5xx."""
    app_key = get_env("OMIE_APP_KEY")
    app_secret = get_env("OMIE_APP_SECRET")

    payload = {
        "call": "ConsultarPedido",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [{
            "idPedido": id_pedido,
            "codigo_pedido": id_pedido
        }]
    }

    if not _http:
        raise RuntimeError("Cliente HTTP indisponível")

    waits = [0, 1, 3, 7, 15]
    for i, wait in enumerate(waits):
        if wait:
            await asyncio.sleep(wait)
        try:
            r = await _http.post(OMIE_PEDIDO_PATH, json=payload)
            logger.info('Requisição HTTP: POST %s %r', OMIE_PEDIDO_PATH, r.status_code)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    return None
            if 500 <= r.status_code < 600:
                continue
            return None
        except (httpx.TimeoutException, httpx.ReadTimeout, httpx.ConnectError):
            continue
        except Exception:
            return None
    return None


# ------------------------- webhook ------------------------- #
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str):
    expected = get_env("WEBHOOK_TOKEN")
    if token != expected:
        raise HTTPException(status_code=401, detail="Token inválido.")

    payload = await _parse_payload(request)
    if not payload:
        raise HTTPException(status_code=400, detail="Payload inválido ou vazio.")

    logger.info("Payload recebidos: %s", json.dumps(payload, ensure_ascii=False))

    topic = _topic(payload.get("topic", ""))

    numero_pedido, numero_nf, id_pedido, valor_pedido, evento = _extrai_pedido_e_nf(payload)
    raw_data = json.dumps(payload, ensure_ascii=False)

    if not _pool:
        raise HTTPException(500, "Pool indisponível")

    # ----- Pedido de Venda -----
    if topic.startswith("vendaproduto."):
        async with _pool.acquire() as conn:
            detailed = None
            if id_pedido and ENRICH_IMMEDIATE_FLAG:
                logger.info("Chamando Omie API para consultar o pedido: %s", id_pedido)
                detailed = await consultar_pedido_omie(id_pedido)
                if detailed:
                    raw_data = json.dumps(detailed, ensure_ascii=False)
                    status = None
                else:
                    status = "pendente_consulta"
            elif id_pedido:
                # Se ENRICH_IMMEDIATE_FLAG for false, enfileira o job
                status = "pendente_consulta"
                await conn.execute("""
                    INSERT INTO public.omie_jobs (id_pedido, status, criado_em)
                    VALUES ($1, 'pendente', now());
                """, id_pedido)

            else:
                status = "pendente_consulta"

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

        return {"status": "success", "message": f"Pedido {numero_pedido or id_pedido} salvo/atualizado.", "retry_status": detailed is None}

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


# ------------------------- job runner ------------------------- #
@app.get("/jobs/run")
async def run_jobs():
    """Endpoint para processar os jobs pendentes na fila."""
    if not _pool:
        raise HTTPException(500, "Pool indisponível")
    
    async with _pool.acquire() as conn:
        jobs = await conn.fetch("""
            SELECT id_job, id_pedido
            FROM public.omie_jobs
            WHERE status = 'pendente'
            ORDER BY criado_em
            LIMIT 10;
        """)

        if not jobs:
            return {"status": "no_jobs", "message": "Nenhum job pendente para processar."}

        processed_count = 0
        for job in jobs:
            id_pedido = job["id_pedido"]
            
            await conn.execute("""
                UPDATE public.omie_jobs SET status = 'processando', tentativas = tentativas + 1
                WHERE id_job = $1;
            """, job["id_job"])

            detailed = await consultar_pedido_omie(id_pedido)

            if detailed:
                status = "processado"
                raw_data = json.dumps(detailed, ensure_ascii=False)
                await conn.execute("""
                    UPDATE public.omie_pedido SET raw = $1, status = 'completo'
                    WHERE id_pedido_omie = $2;
                """, raw_data, id_pedido)
            else:
                status = "falhou"
                
            await conn.execute("""
                UPDATE public.omie_jobs SET status = $1, processado_em = now()
                WHERE id_job = $2;
            """, status, job["id_job"])
            
            processed_count += 1

    return {"status": "success", "message": f"{processed_count} jobs processados."}