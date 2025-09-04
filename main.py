import os, json, logging, traceback
from typing import Optional, List, Dict, Any

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Query

# -----------------------------
# Config & Logger
# -----------------------------
app = FastAPI()
logger = logging.getLogger("uvicorn.error")

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "troque-isto")

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

# Ajuste a URL exata do endpoint Omie de pedido
OMIE_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"

# -----------------------------
# Pool de DB
# -----------------------------
_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
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
# Omie Client
# -----------------------------
async def omie_consultar_pedido(id_pedido_omie: int) -> Dict[str, Any]:
    """
    Ajuste o payload conforme o método/rota da Omie que você usa.
    'ConsultarPedido' é um exemplo comum para /produtos/pedido/.
    """
    payload = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{"idPedido": id_pedido_omie}],
    }
    timeout = httpx.Timeout(OMIE_TIMEOUT)
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(OMIE_URL, json=payload)
        logger.info(f"HTTP POST {OMIE_URL} -> {r.status_code}")
        try:
            data = r.json()
        except Exception:
            data = {"_non_json_body": r.text}
        # Loga a resposta da Omie
        logger.info({"tag": "omie_response", "id_pedido": id_pedido_omie, "response": data})
        r.raise_for_status()
        return data

# -----------------------------
# Persistência
# -----------------------------
async def update_pedido_detalhe(conn: asyncpg.Connection, id_pedido_omie: int, detalhe: Dict[str, Any]):
    """Atualiza raw_detalhe + status=consultado"""
    await conn.execute(
        """
        UPDATE omie_pedido
           SET raw_detalhe = $1,
               status = 'consultado'
         WHERE id_pedido_omie = $2
        """,
        json.dumps(detalhe), id_pedido_omie
    )

async def fetch_pendentes(conn: asyncpg.Connection, limit: int = 50) -> List[asyncpg.Record]:
    return await conn.fetch(
        """
        SELECT id_pedido_omie, numero
          FROM omie_pedido
         WHERE status = 'pendente_consulta'
         ORDER BY recebido_em ASC
         LIMIT $1
        """,
        limit
    )

async def fetch_id_by_numero(conn: asyncpg.Connection, numero: str) -> Optional[int]:
    rec = await conn.fetchrow(
        """
        SELECT id_pedido_omie
          FROM omie_pedido
         WHERE numero = $1
         LIMIT 1
        """,
        numero
    )
    return rec["id_pedido_omie"] if rec else None

# -----------------------------
# Rotas básicas
# -----------------------------
@app.get("/")
async def root():
    return {"status": "ok", "msg": "raiz do serviço"}

@app.get("/healthz")
async def healthz():
    return {"status": "healthy"}

# -----------------------------
# Webhook Omie (recebe eventos)
# -----------------------------
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str = Query(default="")):
    # Se quiser, valide token:
    # if token != os.getenv("WEBHOOK_SECRET", "um-segredo-forte"):
    #     raise HTTPException(status_code=401, detail="unauthorized")

    body = await request.json()
    # Tente extrair campos úteis (ajuste conforme seu payload)
    numero = body.get("numeroPedido") or body.get("numero")
    evento = body.get("evento") or {}
    id_pedido = evento.get("idPedido") or body.get("idPedido")

    logger.info({"tag": "omie_webhook_received", "numero_pedido": str(numero), "id_pedido": id_pedido})

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Upsert “inbox” com status pendente_consulta
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, raw_basico, status)
            VALUES ($1, $2, $3, 'pendente_consulta')
            ON CONFLICT (id_pedido_omie) DO UPDATE
               SET numero     = EXCLUDED.numero,
                   raw_basico = EXCLUDED.raw_basico
            """,
            numero, id_pedido, json.dumps(body)
        )

    return {"ok": True}

# -----------------------------
# Admin: reprocessar TODOS pendentes
# -----------------------------
@app.post("/admin/reprocessar-pendentes")
async def reprocessar_pendentes(secret: str, limit: int = 50):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    pool = await get_pool()
    total_ok = 0
    total_err = 0
    pendentes_total = 0
    ids_ok: List[int] = []

    async with pool.acquire() as conn:
        rows = await fetch_pendentes(conn, limit=limit)
        pendentes_total = len(rows)

        for row in rows:
            id_pedido = int(row["id_pedido_omie"])
            numero = row["numero"]
            try:
                logger.info({"tag": "reprocess_start", "id_pedido": id_pedido, "numero": numero})
                detalhe = await omie_consultar_pedido(id_pedido)
                await update_pedido_detalhe(conn, id_pedido, detalhe)
                total_ok += 1
                ids_ok.append(id_pedido)
                logger.info({"tag": "reprocess_done", "id_pedido": id_pedido, "numero": numero})
            except Exception:
                total_err += 1
                logger.error({
                    "tag": "reprocess_error",
                    "id_pedido": id_pedido,
                    "numero": numero,
                    "err": traceback.format_exc()
                })

    return {
        "ok": True,
        "pendentes_encontrados": pendentes_total,
        "processados_ok": total_ok,
        "processados_erro": total_err,
        "ids_ok": ids_ok
    }

# -----------------------------
# Admin: reprocessar por número
# -----------------------------
@app.post("/admin/reprocessar-pedido")
async def reprocessar_pedido(numero: str, secret: str):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    pool = await get_pool()
    async with pool.acquire() as conn:
        id_pedido = await fetch_id_by_numero(conn, numero)
        if not id_pedido:
            raise HTTPException(status_code=404, detail="Pedido não encontrado")

        try:
            logger.info({"tag": "reprocess_start", "numero": numero, "id_pedido": id_pedido})
            detalhe = await omie_consultar_pedido(int(id_pedido))
            await update_pedido_detalhe(conn, int(id_pedido), detalhe)
            logger.info({"tag": "reprocess_done", "numero": numero, "id_pedido": id_pedido})
            return {"ok": True, "numero": numero, "id_pedido": id_pedido}
        except Exception:
            logger.error({
                "tag": "reprocess_error",
                "numero": numero,
                "id_pedido": id_pedido,
                "err": traceback.format_exc()
            })
            raise HTTPException(status_code=500, detail="Falha ao reprocessar")
