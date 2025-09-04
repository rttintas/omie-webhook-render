import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List

import psycopg2
import psycopg2.extras
import requests
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
APP_NAME = "Omie Webhook API"
TZ = timezone.utc  # ajustamos timestamps no banco como timestamptz (UTC)

DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
ENRICH_PEDIDO_IMEDIATO = os.getenv("ENRICH_PEDIDO_IMEDIATO", "false").lower() == "true"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado.")

# ------------------------------------------------------------------------------
# App & Logger
# ------------------------------------------------------------------------------
app = FastAPI(title=APP_NAME)

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)

def jlog(level: str, **data):
    try:
        msg = json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        msg = str(data)
    getattr(logger, level, logger.info)(msg)

# ------------------------------------------------------------------------------
# DB Helpers
# ------------------------------------------------------------------------------
def get_conn():
    # A conexão simples por chamada. Em Render free/low tier, isso evita pool esquisito.
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def db_exec(sql: str, params: Tuple = ()):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            try:
                rows = cur.fetchall()
            except psycopg2.ProgrammingError:
                rows = None
    return rows

def db_init_schema():
    # Tabela de pedidos recebidos/enriquecidos
    db_exec("""
    CREATE TABLE IF NOT EXISTS public.omie_pedido (
        id_pedido        BIGSERIAL PRIMARY KEY,
        numero           TEXT UNIQUE,              -- número do pedido no Omie (string)
        id_pedido_omie   BIGINT,                   -- idPedido (Omie)
        valor_total      NUMERIC,
        status           TEXT DEFAULT 'pendente_consulta',
        raw_basico       JSONB,                    -- payload "simples" do webhook
        raw_detalhe      JSONB,                    -- resposta detalhada da API Omie
        recebido_em      TIMESTAMPTZ DEFAULT now()
    );
    """)

    # Tabela de NF-e (quando você usar NFe.NotaAutorizada)
    db_exec("""
    CREATE TABLE IF NOT EXISTS public.omie_nfe (
        chave_nfe     TEXT,
        numero        TEXT,
        serie         TEXT,
        emitida_em    TIMESTAMPTZ,
        cnpj_emitente TEXT,
        cnpj_destinatario TEXT,
        valor_total   NUMERIC,
        status        TEXT,
        xml           TEXT,
        pdf_url       TEXT,
        last_event_at TIMESTAMPTZ,
        updated_at    TIMESTAMPTZ,
        data_emissao  TEXT,
        cliente_nome  TEXT,
        raw           JSONB,
        created_at    TIMESTAMPTZ DEFAULT now(),
        recebido_em   TIMESTAMPTZ DEFAULT now(),
        id_nfe        BIGINT,
        danfe_url     TEXT,
        xml_url       TEXT
    );
    """)

    # Fila de jobs (cron)
    db_exec("""
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

    # Índices auxiliares
    db_exec("CREATE INDEX IF NOT EXISTS idx_omie_jobs_status_next_run ON public.omie_jobs(status, next_run);")
    db_exec("CREATE INDEX IF NOT EXISTS idx_omie_pedido_numero ON public.omie_pedido(numero);")

    jlog("info", tag="startup", msg="Pool OK & schema verificado.")

# ------------------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------------------
def _ensure_dict(payload: Any) -> Dict[str, Any]:
    """
    Converte payload em dict, seja ele str/JSONB/dict.
    Corrige erro: 'str' object has no attribute 'get'
    """
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return {}
    # psycopg2 JSONB geralmente já vira dict; em último caso:
    try:
        return dict(payload)
    except Exception:
        return {}

def upsert_pedido_basico(numero: Optional[str], id_pedido: Optional[int], valor: Optional[float], raw: Dict[str, Any]):
    db_exec("""
        INSERT INTO public.omie_pedido (numero, id_pedido_omie, valor_total, status, raw_basico, recebido_em)
        VALUES ($$%s$$, %s, %s, 'pendente_consulta', %s, now())
        ON CONFLICT (numero) DO UPDATE SET
           id_pedido_omie = EXCLUDED.id_pedido_omie,
           valor_total    = COALESCE(EXCLUDED.valor_total, public.omie_pedido.valor_total),
           raw_basico     = EXCLUDED.raw_basico,
           recebido_em    = now(),
           status         = CASE 
                               WHEN public.omie_pedido.status = 'pendente_consulta' THEN 'pendente_consulta'
                               ELSE public.omie_pedido.status
                            END
    """, (numero, id_pedido, valor, json.dumps(raw, ensure_ascii=False)))

def enqueue_job(job_type: str, payload: Dict[str, Any], run_in_seconds: int = 0):
    next_run = datetime.now(tz=TZ) + timedelta(seconds=run_in_seconds)
    db_exec("""
        INSERT INTO public.omie_jobs (job_type, status, attempts, next_run, payload, created_at, updated_at)
        VALUES (%s, 'pending', 0, %s, %s, now(), now())
    """, (job_type, next_run, json.dumps(payload, ensure_ascii=False)))

def update_job_done(job_id: int):
    db_exec("""
        UPDATE public.omie_jobs
           SET status='done', updated_at=now(), last_error=NULL
         WHERE id=%s
    """, (job_id,))

def update_job_fail(job_id: int, err: str):
    db_exec("""
        UPDATE public.omie_jobs
           SET attempts = attempts + 1,
               status   = CASE WHEN attempts >= 2 THEN 'pending' ELSE 'pending' END,
               last_error = %s,
               updated_at = now(),
               next_run = now() + interval '2 minutes'
         WHERE id=%s
    """, (err[:900], job_id))

def pick_pending_jobs(limit: int = 20) -> List[Dict[str, Any]]:
    rows = db_exec("""
        SELECT id, job_type, status, attempts, next_run, last_error, payload
          FROM public.omie_jobs
         WHERE status='pending' AND next_run <= now()
         ORDER BY next_run ASC
         LIMIT %s
    """, (limit,))
    return rows or []

def set_pedido_detalhe(numero: Optional[str], detalhe: Dict[str, Any]):
    # Atualiza status e salva raw_detalhe
    db_exec("""
        UPDATE public.omie_pedido
           SET raw_detalhe = %s,
               status      = 'consultado'
         WHERE numero = %s
    """, (json.dumps(detalhe, ensure_ascii=False), numero,))

# ------------------------------------------------------------------------------
# Omie API (Consulta Pedido)
# ------------------------------------------------------------------------------
OMIE_ENDPOINT = "https://app.omie.com.br/api/v1/produtos/pedido/"

def omie_consultar_pedido(id_pedido: Optional[int]=None, numero_pedido: Optional[str]=None) -> Dict[str, Any]:
    """
    Faz POST para Omie: ConsultarPedido
    Documentação Omie (conceitual): método 'ConsultarPedido' no recurso de Pedidos.
    """
    if not OMIE_APP_KEY or not OMIE_APP_SECRET:
        raise RuntimeError("OMIE_APP_KEY/OMIE_APP_SECRET não configurados.")

    # Corpo mínimo — Omie aceita consultar por numero_pedido ou id_pedido
    # (alguns ambientes exigem um dos dois; ajustamos ao que tivermos)
    call_body: Dict[str, Any] = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            # Omie costuma usar 'numero_pedido' (string) e/ou 'id_pedido'
            "numero_pedido": str(numero_pedido) if numero_pedido else None,
            "id_pedido": int(id_pedido) if id_pedido else None
        }]
    }

    # Limpa chaves None do param
    param0 = {k: v for k, v in call_body["param"][0].items() if v not in (None, "", "None")}
    call_body["param"][0] = param0

    jlog("info", tag="omie_call", method="ConsultarPedido", payload=param0)

    resp = requests.post(OMIE_ENDPOINT, json=call_body, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Omie ConsultarPedido HTTP {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    # Algumas respostas de erro vêm com chaves 'faultstring' etc.
    if isinstance(data, dict) and data.get("faultstring"):
        raise RuntimeError(f"Omie fault: {data.get('faultstring')}")
    return data

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.get("/health/app")
def health_app():
    return {"ok": True, "name": APP_NAME}

@app.get("/health/db")
def health_db():
    try:
        row = db_exec("SELECT 1 AS select1;")
        return {"db": "ok", "select1": row[0]["select1"] if row else None}
    except Exception as e:
        return JSONResponse(status_code=500, content={"db": "error", "detail": str(e)})

@app.on_event("startup")
def on_startup():
    db_init_schema()

# -------------------- Webhook Omie --------------------
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: Optional[str] = Query(default=None)):
    if not WEBHOOK_TOKEN or token != WEBHOOK_TOKEN:
        jlog("warning", tag="auth", msg="Token inválido em /omie/webhook", got=token)
        raise HTTPException(status_code=401, detail="unauthorized")

    # Omie pode enviar JSON (Content-Type: application/json)
    # ou formulário (application/x-www-form-urlencoded) com um campo "json"
    try:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            payload = await request.json()
        else:
            form = await request.form()
            # alguns parceiros mandam { "json": "<string json>" }
            if "json" in form:
                payload = json.loads(form["json"])
            else:
                # melhor esforço: tenta ler como json mesmo assim
                body = await request.body()
                try:
                    payload = json.loads(body.decode() or "{}")
                except Exception:
                    payload = {}

    except Exception as e:
        jlog("error", tag="webhook_parse_error", error=str(e))
        raise HTTPException(status_code=400, detail="payload parse error")

    message_id = payload.get("messageId")
    topic = payload.get("topic") or payload.get("Topico") or ""
    event = _ensure_dict(payload.get("event") or payload.get("evento") or {})
    autor = _ensure_dict(payload.get("autor") or {})

    # Extrai dados principais
    numero_pedido = str(event.get("numeroPedido") or event.get("numero_pedido") or "")

    # idPedido pode aparecer como int "grande"
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
    upsert_pedido_basico(numero_pedido or None, id_pedido_omie, valor_pedido, raw=payload)

    # Enfileira job de consulta detalhada (ou tenta enriquecer na hora)
    job_payload = {"id_pedido": id_pedido_omie, "numero_pedido": numero_pedido}
    if ENRICH_PEDIDO_IMEDIATO and (id_pedido_omie or numero_pedido):
        try:
            data = omie_consultar_pedido(id_pedido=id_pedido_omie, numero_pedido=numero_pedido)
            set_pedido_detalhe(numero_pedido or None, data)
            jlog("info", tag="webhook_enriched_immediate", numero=numero_pedido, id_pedido=id_pedido_omie)
        except Exception as e:
            jlog("warning", tag="webhook_enrich_fail", numero=numero_pedido, id_pedido=id_pedido_omie, error=str(e))
            enqueue_job("pedido.consultar", job_payload, run_in_seconds=0)
    else:
        enqueue_job("pedido.consultar", job_payload, run_in_seconds=0)

    jlog("info",
         tag="omie_webhook_received",
         messageId=message_id,
         topic=topic,
         numero_pedido=numero_pedido,
         id_pedido=id_pedido_omie)

    return {"ok": True}

# -------------------- Admin: Run Jobs --------------------
@app.post("/admin/run-jobs")
def admin_run_jobs(secret: Optional[str] = Query(default=None), limit: int = Query(default=20)):
    if not ADMIN_SECRET or secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    jobs = pick_pending_jobs(limit=limit)
    ran = 0
    for job in jobs:
        job_id = job["id"]
        job_type = job["job_type"]
        payload = _ensure_dict(job.get("payload"))
        jlog("info", tag="job_start", id=job_id, job_type=job_type, payload=payload)

        try:
            if job_type == "pedido.consultar":
                numero = str(payload.get("numero_pedido") or "")
                id_ped = payload.get("id_pedido")
                id_ped_int = None
                try:
                    if id_ped not in (None, "", "None"):
                        id_ped_int = int(id_ped)
                except Exception:
                    id_ped_int = None

                if not numero and not id_ped_int:
                    raise RuntimeError("payload sem numero_pedido ou id_pedido")

                data = omie_consultar_pedido(id_pedido=id_ped_int, numero_pedido=numero)
                # salva detalhe
                set_pedido_detalhe(numero or None, data)

            else:
                # Job desconhecido → marca done para não travar fila
                jlog("warning", tag="job_unknown", id=job_id, job_type=job_type)
            update_job_done(job_id)
            jlog("info", tag="job_done", id=job_id)
            ran += 1

        except Exception as e:
            update_job_fail(job_id, str(e))
            jlog("error", tag="job_fail", id=job_id, error=str(e))

    return {"ok": True, "picked": len(jobs), "ran": ran}

# -------------------- Admin: Jobs Stats --------------------
@app.get("/admin/jobs-stats")
def jobs_stats(secret: Optional[str] = Query(default=None)):
    if not ADMIN_SECRET or secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    rows = db_exec("""
        SELECT status, COUNT(*) AS qnt FROM public.omie_jobs GROUP BY status
    """) or []
    by_status = {r["status"]: int(r["qnt"]) for r in rows}

    return {"ok": True, "by_status": by_status, "time": datetime.now(tz=TZ).isoformat()}
