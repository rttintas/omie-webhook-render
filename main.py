# main.py
import os
import json
import logging
import traceback
from typing import Any, Optional

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Query

# -----------------------------
# Config
# -----------------------------
app = FastAPI()
logger = logging.getLogger("uvicorn.error")

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "troque-isto")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_WEBHOOK_TOKEN = os.getenv("OMIE_WEBHOOK_TOKEN", "")  # opcional, valida ?token=
OMIE_TIMEOUT = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))
OMIE_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"

_pool: Optional[asyncpg.Pool] = None


# -----------------------------
# Pool de conexões
# -----------------------------
async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL não configurada")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        logger.info({"tag": "startup", "msg": "Pool OK e esquema selecionado."})
    return _pool


@app.on_event("startup")
async def _startup():
    await get_pool()


@app.on_event("shutdown")
async def _shutdown():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# -----------------------------
# Utils para extrair campos do JSON
# -----------------------------
WANTED_ID_KEYS = {
    "idPedido",
    "id_pedido",
    "idPedidoOmie",
    "idPedidoVenda",
    "id_pedido_omie",
    "codigo_pedido",  # alguns webhooks já mandam com esse nome
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
    """Heurística simples: integração costuma ser alfanumérico (ex.: OH15186144)."""
    return bool(code) and not str(code).isdigit()


# -----------------------------
# Chamadas à Omie
# -----------------------------
async def omie_consultar_pedido(
    codigo_pedido: int | None = None,
    codigo_pedido_integracao: str | None = None,
) -> dict:
    """
    Consulta detalhes do pedido de venda (PedidoVendaProduto::ConsultarPedido).
    Aceita id numérico (codigo_pedido) ou código de integração (codigo_pedido_integracao).
    """
    if not codigo_pedido and not codigo_pedido_integracao:
        raise ValueError("Informe codigo_pedido ou codigo_pedido_integracao")

    param: dict[str, object] = {}
    if codigo_pedido is not None:
        param["codigo_pedido"] = int(codigo_pedido)
    if codigo_pedido_integracao:
        param["codigo_pedido_integracao"] = str(codigo_pedido_integracao)

    payload = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [param],
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(OMIE_TIMEOUT)) as client:
        r = await client.post(OMIE_URL, json=payload)
        # Log minimamente verboso (status e parâmetro)
        logger.info({"tag": "omie_http", "status": r.status_code, "param": param})
        try:
            data = r.json()
        except Exception:
            data = {"_non_json_body": r.text}
        # Loga a resposta inteira (útil para auditoria)
        logger.info({"tag": "omie_response", "param": param, "response": data})
        r.raise_for_status()
        # Mesmo com 200 a Omie pode retornar "Erros encontrados" dentro do JSON;
        # deixe para o fluxo superior decidir como tratar.
        return data


# -----------------------------
# Helpers de Banco
# -----------------------------
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
    Insere o evento recebido sem derrubar a app.
    - Se houver UNIQUE pelo id e tivermos 'id_pedido': UPSERT.
    - Se só houver 'codigo_pedido_integracao': INSERT simples (sem upsert).
    - Se não houver chave nenhuma: marca 'sem_id'.
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
        # UPSERT por constraint do id
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
        # Sem UNIQUE ou só integração → insert simples
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


# -----------------------------
# Endpoints básicos
# -----------------------------
@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/healthz")
async def healthz():
    return {"status": "healthy"}


# -----------------------------
# Webhook da Omie
# -----------------------------
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

    # Preferir campo explícito de integração; se não houver, inferir a partir de "numero" alfanumérico
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
            # não derruba a Omie; 202 = aceito p/ processamento
            raise HTTPException(status_code=202, detail="aceito para processamento")

    return {"ok": True}


# -----------------------------
# Reprocesso (pendentes e por pedido)
# -----------------------------
async def _processar_pendentes(conn: asyncpg.Connection, limit: int):
    total_ok, total_err, ids_ok = 0, 0, []
    rows = await fetch_pendentes(conn, limit=limit)

    for row in rows:
        idp = row["id_pedido_omie"]
        integ = row["codigo_pedido_integracao"]
        numero = row["numero"]

        try:
            logger.info({"tag": "reprocess_start", "id": idp, "integ": integ, "numero": numero})
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
            total_ok += 1
            ids_ok.append(idp if idp is not None else integ)
        except Exception:
            total_err += 1
            logger.error(
                {"tag": "reprocess_error", "id": idp, "integ": integ, "numero": numero, "err": traceback.format_exc()}
            )

    return total_ok, total_err, ids_ok, len(rows)


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
    """
    Reprocessa um pedido específico.
    - Pode informar 'numero' que a gente resolve as chaves pelo banco;
    - ou informar diretamente 'codigo_pedido_integracao'.
    """
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


# Compatibilidade com cron antigo
@app.post("/admin/run-jobs")
async def run_jobs(secret: str, limit: int = 50):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        ok, err, ids_ok, found = await _processar_pendentes(conn, limit)
    return {"ok": True, "pendentes_encontrados": found, "processados_ok": ok, "processados_erro": err, "ids_ok": ids_ok}


# Para rodar localmente:
# uvicorn main:app --host 0.0.0.0 --port 8000
