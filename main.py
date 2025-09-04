import os, json, logging, traceback
from typing import Optional, List, Dict, Any, Union

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Query

app = FastAPI()
logger = logging.getLogger("uvicorn.error")

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "troque-isto")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_TIMEOUT = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))
OMIE_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"

_pool: Optional[asyncpg.Pool] = None
async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        logger.info({"tag":"startup","msg":"Pool OK"})
    return _pool

@app.on_event("startup")
async def _startup(): await get_pool()

@app.on_event("shutdown")
async def _shutdown():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

# ----------------------------- #
# Utils: extração profunda do JSON
# ----------------------------- #
WANTED_ID_KEYS = {"idPedido","id_pedido","idPedidoOmie","idPedidoVenda","id_pedido_omie"}
WANTED_NUM_KEYS = {"numeroPedido","numero_pedido","numero","codigo_pedido","codigoPedido"}

def _as_int(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, int): return v
    if isinstance(v, str) and v.isdigit():
        try: return int(v)
        except: return None
    return None

def deep_find_first(obj: Any, keys: set[str]) -> Optional[Union[str,int]]:
    if isinstance(obj, dict):
        for k,v in obj.items():
            if k in keys:
                return v
        for v in obj.values():
            got = deep_find_first(v, keys)
            if got is not None: return got
    elif isinstance(obj, list):
        for it in obj:
            got = deep_find_first(it, keys)
            if got is not None: return got
    return None

# ----------------------------- #
# Omie API
# ----------------------------- #
async def omie_consultar_pedido(id_pedido_omie: int) -> Dict[str, Any]:
    payload = {
        "call": "ConsultarPedido",   # ajuste se seu call for outro
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{"idPedido": id_pedido_omie}],
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(OMIE_TIMEOUT)) as client:
        r = await client.post(OMIE_URL, json=payload)
        logger.info(f"HTTP POST {OMIE_URL} -> {r.status_code}")
        try:
            data = r.json()
        except Exception:
            data = {"_non_json_body": r.text}
        logger.info({"tag":"omie_response","id_pedido":id_pedido_omie,"response":data})
        r.raise_for_status()
        return data

# ----------------------------- #
# DB helpers
# ----------------------------- #
async def has_unique_on_id(conn: asyncpg.Connection) -> bool:
    # checa se já existe a UNIQUE/índice com esse nome (do nosso script)
    row = await conn.fetchrow("""
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'omie_pedido'::regclass
          AND contype IN ('u','p')
          AND conname = 'omie_pedido_uid'
        LIMIT 1
    """)
    return row is not None

async def insert_inbox_safe(conn: asyncpg.Connection, numero: Optional[str], id_pedido: Optional[int], body: dict):
    """
    Insere sem derrubar o app:
    - Se tiver UNIQUE, faz UPSERT pela constraint.
    - Se não tiver, faz INSERT simples.
    - Se id for None, grava como 'sem_id' e não tenta upsert.
    """
    if id_pedido is None:
        await conn.execute("""
            INSERT INTO omie_pedido (numero, id_pedido_omie, raw_basico, status)
            VALUES ($1, NULL, $2, 'sem_id')
        """, numero, json.dumps(body))
        logger.info({"tag":"inbox_inserted_sem_id","numero":str(numero)})
        return

    if await has_unique_on_id(conn):
        # usa a constraint nominal -> robusto
        await conn.execute("""
            INSERT INTO omie_pedido (numero, id_pedido_omie, raw_basico, status)
            VALUES ($1, $2, $3, 'pendente_consulta')
            ON CONFLICT ON CONSTRAINT omie_pedido_uid DO UPDATE
               SET numero     = EXCLUDED.numero,
                   raw_basico = EXCLUDED.raw_basico
        """, numero, id_pedido, json.dumps(body))
    else:
        # fallback temporário sem UNIQUE (evita 500)
        await conn.execute("""
            INSERT INTO omie_pedido (numero, id_pedido_omie, raw_basico, status)
            VALUES ($1, $2, $3, 'pendente_consulta')
        """, numero, id_pedido, json.dumps(body))
        logger.warning({"tag":"no_unique_yet","msg":"inseriu sem upsert","id_pedido":id_pedido})

async def update_pedido_detalhe(conn: asyncpg.Connection, id_pedido_omie: int, detalhe: Dict[str, Any]):
    await conn.execute("""
        UPDATE omie_pedido
           SET raw_detalhe = $1,
               status = 'consultado'
         WHERE id_pedido_omie = $2
    """, json.dumps(detalhe), id_pedido_omie)

async def fetch_pendentes(conn: asyncpg.Connection, limit: int = 50):
    return await conn.fetch("""
        SELECT id_pedido_omie, numero
          FROM omie_pedido
         WHERE status = 'pendente_consulta'
         ORDER BY recebido_em ASC
         LIMIT $1
    """, limit)

async def fetch_id_by_numero(conn: asyncpg.Connection, numero: str) -> Optional[int]:
    r = await conn.fetchrow("""
        SELECT id_pedido_omie
          FROM omie_pedido
         WHERE numero = $1
         LIMIT 1
    """, numero)
    return int(r["id_pedido_omie"]) if r and r["id_pedido_omie"] is not None else None

# ----------------------------- #
# Rotas básicas
# ----------------------------- #
@app.get("/")
async def root(): return {"status":"ok"}

@app.get("/healthz")
async def healthz(): return {"status":"healthy"}

# ----------------------------- #
# Webhook (robusto, sem 500)
# ----------------------------- #
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str = Query(default="")):
    body = await request.json()
    # extração robusta
    numero_any = deep_find_first(body, WANTED_NUM_KEYS)
    id_any = deep_find_first(body, WANTED_ID_KEYS)

    numero = str(numero_any) if numero_any is not None else None
    id_pedido = _as_int(id_any)

    logger.info({"tag":"omie_webhook_received","numero_pedido":str(numero),"id_pedido":id_pedido})

    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await insert_inbox_safe(conn, numero, id_pedido, body)
        except Exception:
            logger.error({"tag":"inbox_error","err":traceback.format_exc(),"numero":str(numero),"id_pedido":id_pedido})
            # não derruba a Omie; responde 202 para reentrega se necessário
            raise HTTPException(status_code=202, detail="aceito para processamento")

    return {"ok": True}

# ----------------------------- #
# Reprocessos
# ----------------------------- #
async def _processar_pendentes(conn: asyncpg.Connection, limit: int):
    total_ok, total_err, ids_ok = 0, 0, []
    rows = await fetch_pendentes(conn, limit=limit)
    for row in rows:
        id_pedido = int(row["id_pedido_omie"])
        numero = row["numero"]
        try:
            logger.info({"tag":"reprocess_start","id_pedido":id_pedido,"numero":numero})
            detalhe = await omie_consultar_pedido(id_pedido)
            await update_pedido_detalhe(conn, id_pedido, detalhe)
            logger.info({"tag":"reprocess_done","id_pedido":id_pedido,"numero":numero})
            total_ok += 1; ids_ok.append(id_pedido)
        except Exception:
            total_err += 1
            logger.error({"tag":"reprocess_error","id_pedido":id_pedido,"numero":numero,"err":traceback.format_exc()})
    return total_ok, total_err, ids_ok, len(rows)

@app.post("/admin/reprocessar-pendentes")
async def reprocessar_pendentes(secret: str, limit: int = 50):
    if secret != ADMIN_SECRET: raise HTTPException(status_code=401, detail="unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        ok, err, ids_ok, found = await _processar_pendentes(conn, limit)
    return {"ok": True, "pendentes_encontrados": found, "processados_ok": ok, "processados_erro": err, "ids_ok": ids_ok}

@app.post("/admin/reprocessar-pedido")
async def reprocessar_pedido(numero: str, secret: str):
    if secret != ADMIN_SECRET: raise HTTPException(status_code=401, detail="unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        id_pedido = await fetch_id_by_numero(conn, numero)
        if not id_pedido:
            raise HTTPException(status_code=404, detail="Pedido não encontrado ou sem id")
        detalhe = await omie_consultar_pedido(id_pedido)
        await update_pedido_detalhe(conn, id_pedido, detalhe)
    return {"ok": True, "numero": numero, "id_pedido": id_pedido}

# Compatibilidade com seu cron antigo:
@app.post("/admin/run-jobs")
async def run_jobs(secret: str, limit: int = 50):
    if secret != ADMIN_SECRET: raise HTTPException(status_code=401, detail="unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        ok, err, ids_ok, found = await _processar_pendentes(conn, limit)
    return {"ok": True, "pendentes_encontrados": found, "processados_ok": ok, "processados_erro": err, "ids_ok": ids_ok}
