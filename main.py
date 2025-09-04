# main.py
import os
import json
import logging
import traceback
from typing import Any, Optional

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Query

# ------------------------------------------------------------
# Configuração
# ------------------------------------------------------------
app = FastAPI()
logger = logging.getLogger("uvicorn.error")

DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "troque-isto")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_WEBHOOK_TOKEN = os.getenv("OMIE_WEBHOOK_TOKEN", "")  # valida ?token=...
OMIE_TIMEOUT = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))
OMIE_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"

_pool: Optional[asyncpg.Pool] = None


# ------------------------------------------------------------
# Conexão com o banco
# ------------------------------------------------------------
async def get_pool() -> asyncpg.Pool:
    global _pool
    if not _pool:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL não configurada")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        logger.info({"tag": "startup", "msg": "Pool OK e esquema selecionado."})
    return _pool


async def _safe_bootstrap(conn: asyncpg.Connection):
    """Migrações seguras (idempotentes). NÃO cria UNIQUE aqui."""
    await conn.execute(
        """
        ALTER TABLE omie_pedido
          ADD COLUMN IF NOT EXISTS raw_basico  JSONB,
          ADD COLUMN IF NOT EXISTS raw_detalhe JSONB,
          ADD COLUMN IF NOT EXISTS status      TEXT,
          ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now(),
          ADD COLUMN IF NOT EXISTS codigo_pedido_integracao TEXT;

        CREATE INDEX IF NOT EXISTS idx_omie_pedido_status
          ON omie_pedido (status);

        CREATE INDEX IF NOT EXISTS idx_omie_pedido_integr
          ON omie_pedido (codigo_pedido_integracao);
        """
    )


@app.on_event("startup")
async def _startup():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _safe_bootstrap(conn)


@app.on_event("shutdown")
async def _shutdown():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ------------------------------------------------------------
# Utilitários
# ------------------------------------------------------------
WANTED_ID_KEYS = {
    "idPedido",
    "id_pedido",
    "idPedidoOmie",
    "idPedidoVenda",
    "id_pedido_omie",
    "codigo_pedido",  # às vezes vem assim
}
WANTED_NUM_KEYS = {"numeroPedido", "numero_pedido", "numero", "codigo_pedido_integracao"}
WANTED_INTEGR_KEYS = {"codigo_pedido_integracao", "codigoPedidoIntegracao"}


def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.isdigit():
        try:
            return int(v)
        except Exception:
            return None
    return None


def deep_find_first(obj: Any, keys: set[str]) -> Optional[Any]:
    """Procura recursivamente a 1ª ocorrência de qualquer chave do conjunto."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys:
                return v
        for v in obj.values():
            got = deep_find_first(v, keys)
            if got is not None:
                return got
    elif isinstance(obj, list):
        for it in obj:
            got = deep_find_first(it, keys)
            if got is not None:
                return got
    return None


def looks_like_integr(code: Optional[str]) -> bool:
    """Heurística simples para códigos de integração (ex.: OH15186144)."""
    return bool(code) and not str(code).isdigit()


# ------------------------------------------------------------
# Chamada Omie (blindada — nunca manda idPedido)
# ------------------------------------------------------------
def _param_consultar(
    codigo_pedido: int | None = None,
    codigo_pedido_integracao: str | None = None,
    idPedido_legacy: int | None = None,  # compat: converte para codigo_pedido
) -> dict:
    if codigo_pedido is None and codigo_pedido_integracao is None and idPedido_legacy is None:
        raise ValueError("Informe codigo_pedido OU codigo_pedido_integracao.")

    param: dict[str, object] = {}
    if codigo_pedido is not None:
        param["codigo_pedido"] = int(codigo_pedido)
    if codigo_pedido_integracao:
        param["codigo_pedido_integracao"] = str(codigo_pedido_integracao)
    if idPedido_legacy is not None and "codigo_pedido" not in param:
        param["codigo_pedido"] = int(idPedido_legacy)

    # Garantia extra: nunca retorna idPedido*
    return param


async def omie_consultar_pedido(
    codigo_pedido: int | None = None,
    codigo_pedido_integracao: str | None = None,
    _idPedido_legacy: int | None = None,
) -> dict:
    param = _param_consultar(codigo_pedido, codigo_pedido_integracao, _idPedido_legacy)

    payload = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [param],
    }

    # auditoria: garante que não há 'idPedido' no payload
    assert "idPedido" not in json.dumps(payload), "Não envie idPedido para ConsultarPedido"

    async with httpx.AsyncClient(timeout=httpx.Timeout(OMIE_TIMEOUT)) as client:
        logger.info({"tag": "omie_http_out", "payload": payload})
        r = await client.post(OMIE_URL, json=payload)
        try:
            data = r.json()
        except Exception:
            data = {"_non_json_body": r.text}
        logger.info(
            {"tag": "omie_http_in", "status": r.status_code, "param": param, "fault": (data.get("faultstring") if isinstance(data, dict) else None)}
        )
        r.raise_for_status()
        return data


# ------------------------------------------------------------
# Banco: helpers
# ------------------------------------------------------------
async def has_unique_on_id(conn: asyncpg.Connection) -> bool:
    row = await conn.fetchrow(
        """
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'omie_pedido'::regclass
           AND contype IN ('u','p')
           AND conname = 'omie_pedido_uid'
         LIMIT 1
        """
    )
    return row is not None


async def insert_inbox_safe(
    conn: asyncpg.Connection,
    numero: Optional[str],
    id_pedido: Optional[int],
    body: dict,
    codigo_pedido_integracao: Optional[str] = None,
):
    """
    Insere o evento recebido:
      - com id -> UPSERT se UNIQUE existir; caso contrário, INSERT
      - com apenas integração -> INSERT
      - sem chave -> status 'sem_id'
    """
    if id_pedido is None and not codigo_pedido_integracao:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, NULL, NULL, $2, 'sem_id')
            """,
            numero,
            json.dumps(body),
        )
        logger.info({"tag": "inbox_inserted_sem_id", "numero": str(numero)})
        return

    if id_pedido is not None and await has_unique_on_id(conn):
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, $2, $3, $4, 'pendente_consulta')
            ON CONFLICT ON CONSTRAINT omie_pedido_uid DO UPDATE
               SET numero     = EXCLUDED.numero,
                   raw_basico = EXCLUDED.raw_basico,
                   codigo_pedido_integracao =
                       COALESCE(EXCLUDED.codigo_pedido_integracao, omie_pedido.codigo_pedido_integracao)
            """,
            numero,
            id_pedido,
            codigo_pedido_integracao,
            json.dumps(body),
        )
    else:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, $2, $3, $4, 'pendente_consulta')
            """,
            numero,
            id_pedido,
            codigo_pedido_integracao,
            json.dumps(body),
        )
        logger.warning(
            {
                "tag": "no_unique_or_only_integr",
                "msg": "inseriu sem upsert (id ausente ou UNIQUE não encontrada)",
                "id_pedido": id_pedido,
                "codigo_pedido_integracao": codigo_pedido_integracao,
            }
        )


async def update_pedido_detalhe(
    conn: asyncpg.Connection,
    detalhe: dict,
    id_pedido_omie: Optional[int] = None,
    codigo_pedido_integracao: Optional[str] = None,
):
    if id_pedido_omie is not None:
        await conn.execute(
            """
            UPDATE omie_pedido
               SET raw_detalhe = $1,
                   status = 'consultado'
             WHERE id_pedido_omie = $2
            """,
            json.dumps(detalhe),
            id_pedido_omie,
        )
    elif codigo_pedido_integracao:
        await conn.execute(
            """
            UPDATE omie_pedido
               SET raw_detalhe = $1,
                   status = 'consultado'
             WHERE codigo_pedido_integracao = $2
            """,
            json.dumps(detalhe),
            codigo_pedido_integracao,
        )
    else:
        raise ValueError("Sem chave para atualizar raw_detalhe")


async def fetch_pendentes(conn: asyncpg.Connection, limit: int = 50):
    return await conn.fetch(
        """
        SELECT id_pedido_omie, numero, codigo_pedido_integracao
          FROM omie_pedido
         WHERE status = 'pendente_consulta'
         ORDER BY recebido_em ASC
         LIMIT $1
        """,
        limit,
    )


async def fetch_keys_by_numero(conn: asyncpg.Connection, numero: str):
    return await conn.fetchrow(
        """
        SELECT id_pedido_omie, codigo_pedido_integracao
          FROM omie_pedido
         WHERE numero = $1
         ORDER BY recebido_em DESC
         LIMIT 1
        """,
        numero,
    )


# ------------------------------------------------------------
# Endpoints básicos
# ------------------------------------------------------------
@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/healthz")
async def healthz():
    return {"status": "healthy"}


# ------------------------------------------------------------
# Webhook da Omie
# ------------------------------------------------------------
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str = Query(default="")):
    if OMIE_WEBHOOK_TOKEN and token != OMIE_WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")

    body = await request.json()

    numero_any = deep_find_first(body, WANTED_NUM_KEYS)
    id_any = deep_find_first(body, WANTED_ID_KEYS)
    integ_any = deep_find_first(body, WANTED_INTEGR_KEYS)

    numero = str(numero_any) if numero_any is not None else None
    id_pedido = _as_int(id_any)
    codigo_pedido_integracao = None

    if isinstance(integ_any, str) and integ_any:
        codigo_pedido_integracao = integ_any
    elif looks_like_integr(numero):
        codigo_pedido_integracao = numero

    logger.info(
        {
            "tag": "omie_webhook_received",
            "numero_pedido": str(numero),
            "id_pedido": id_pedido,
            "codigo_pedido_integracao": codigo_pedido_integracao,
        }
    )

    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await _safe_bootstrap(conn)  # idempotente
            await insert_inbox_safe(conn, numero, id_pedido, body, codigo_pedido_integracao)
        except Exception:
            logger.error(
                {
                    "tag": "inbox_error",
                    "err": traceback.format_exc(),
                    "numero": str(numero),
                    "id_pedido": id_pedido,
                    "codigo_pedido_integracao": codigo_pedido_integracao,
                }
            )
            # Não derruba a Omie: aceite para processamento posterior
            raise HTTPException(status_code=202, detail="aceito para processamento")

    return {"ok": True}


# ------------------------------------------------------------
# Reprocesso
# ------------------------------------------------------------
async def _processar_pendentes(conn: asyncpg.Connection, limit: int):
    ok = err = 0
    ids_ok = []
    rows = await fetch_pendentes(conn, limit=limit)

    for row in rows:
        idp = row["id_pedido_omie"]
        integ = row["codigo_pedido_integracao"]
        numero = row["numero"]

        try:
            logger.info({"tag": "reprocess_start", "id_pedido": idp, "integ": integ, "numero": numero})

            if idp is not None:
                detalhe = await omie_consultar_pedido(codigo_pedido=int(idp))
                await update_pedido_detalhe(conn, detalhe, id_pedido_omie=int(idp))
            elif integ:
                detalhe = await omie_consultar_pedido(codigo_pedido_integracao=str(integ))
                await update_pedido_detalhe(conn, detalhe, codigo_pedido_integracao=str(integ))
            else:
                logger.warning({"tag": "reprocess_skip_sem_chave", "numero": numero})
                continue

            logger.info({"tag": "reprocess_done", "id": idp, "integ": integ, "numero": numero})
            ok += 1
            ids_ok.append(idp if idp is not None else integ)
        except Exception:
            err += 1
            logger.error(
                {"tag": "reprocess_error", "id_pedido": idp, "integ": integ, "numero": numero, "err": traceback.format_exc()}
            )

    return ok, err, ids_ok, len(rows)


@app.post("/admin/reprocessar-pendentes")
async def reprocessar_pendentes(secret: str, limit: int = 50):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        ok, err, ids_ok, found = await _processar_pendentes(conn, limit)
    return {"ok": True, "pendentes_encontrados": found, "processados_ok": ok, "processados_erro": err, "ids_ok": ids_ok}


@app.post("/admin/reprocessar-pedido")
async def reprocessar_pedido(
    secret: str,
    numero: Optional[str] = None,
    codigo_pedido_integracao: Optional[str] = None,
):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    pool = await get_pool()
    async with pool.acquire() as conn:
        idp: Optional[int] = None
        integ: Optional[str] = None

        if codigo_pedido_integracao:
            integ = codigo_pedido_integracao
        elif numero:
            row = await fetch_keys_by_numero(conn, numero)
            if row:
                idp = row["id_pedido_omie"]
                integ = row["codigo_pedido_integracao"]
        else:
            raise HTTPException(status_code=400, detail="Informe 'numero' ou 'codigo_pedido_integracao'")

        if idp is not None:
            detalhe = await omie_consultar_pedido(codigo_pedido=int(idp))
            await update_pedido_detalhe(conn, detalhe, id_pedido_omie=int(idp))
            ref = {"codigo_pedido": idp}
        elif integ:
            detalhe = await omie_consultar_pedido(codigo_pedido_integracao=str(integ))
            await update_pedido_detalhe(conn, detalhe, codigo_pedido_integracao=str(integ))
            ref = {"codigo_pedido_integracao": integ}
        else:
            raise HTTPException(status_code=404, detail="Pedido não encontrado ou sem chave de consulta")

    return {"ok": True, "ref": ref}


# Compat com cron antigo
@app.post("/admin/run-jobs")
async def run_jobs(secret: str, limit: int = 50):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        ok, err, ids_ok, found = await _processar_pendentes(conn, limit)
    return {"ok": True, "pendentes_encontrados": found, "processados_ok": ok, "processados_erro": err, "ids_ok": ids_ok}


# Rodar local:
# uvicorn main:app --host 0.0.0.0 --port 8000
