# main_xml.py — Serviço de NF-e / XML (Omie) + diagnósticos + migração idempotente

import os
import json
import asyncio
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Query, Form, Depends
from fastapi.responses import JSONResponse

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
DATABASE_URL          = os.getenv("DATABASE_URL", "")
ADMIN_SECRET          = os.getenv("ADMIN_SECRET_XML") or os.getenv("ADMIN_SECRET") or ""
OMIE_APP_KEY          = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET       = os.getenv("OMIE_APP_SECRET", "")
OMIE_WEBHOOK_TOKEN_XML= os.getenv("OMIE_WEBHOOK_TOKEN_XML", "")
OMIE_TIMEOUT          = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))   # chamadas API Omie
URL_TIMEOUT           = int(os.getenv("URL_TIMEOUT_SECONDS", "20"))    # baixar XML por URL
OMIE_XML_URL          = os.getenv("OMIE_XML_URL", "https://app.omie.com.br/api/v1/contador/xml/")
OMIE_XML_LIST_CALL    = os.getenv("OMIE_XML_LIST_CALL", "ListarDocumentos")

# Heurística de campos aceitos no evento
WANTED_CHAVE_KEYS   = {"nfe_chave", "nChave", "chave", "chNFe", "chaveNFe", "nfeChave"}
WANTED_IDNF_KEYS    = {"id_nf", "nIdNF", "idNFe", "idNotaFiscal", "idNota"}
WANTED_IDPED_KEYS   = {"id_pedido", "nIdPedido", "idPedido"}

# ------------------------------------------------------------
# App
# ------------------------------------------------------------
app = FastAPI(title="RTT Omie - NFe/XML")

@app.get("/healthz")
def healthz():
    return {"status": "healthy"}

# ------------------------------------------------------------
# Banco
# ------------------------------------------------------------
_pool: Optional[asyncpg.pool.Pool] = None

async def get_pool() -> asyncpg.pool.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL não configurada.")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool

# ------------------------------------------------------------
# BOOTSTRAP/MIGRAÇÃO DA TABELA (idempotente)
# ------------------------------------------------------------
async def _safe_bootstrap(conn: asyncpg.Connection):
    # Cria a tabela se ainda não existir (esquema mais recente)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS omie_nfe (
      chave        TEXT,
      id_nf        BIGINT,
      data_emis    TIMESTAMPTZ,
      xml_text     TEXT,
      status       TEXT NOT NULL DEFAULT 'pendente_consulta',
      last_error   TEXT,
      recebido_em  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)

    # Garante colunas e defaults (NÃO apaga nada)
    await conn.execute("ALTER TABLE omie_nfe ADD COLUMN IF NOT EXISTS id_nf BIGINT;")
    await conn.execute("ALTER TABLE omie_nfe ADD COLUMN IF NOT EXISTS data_emis TIMESTAMPTZ;")
    await conn.execute("ALTER TABLE omie_nfe ADD COLUMN IF NOT EXISTS xml_text TEXT;")
    await conn.execute("ALTER TABLE omie_nfe ADD COLUMN IF NOT EXISTS status TEXT;")
    await conn.execute("ALTER TABLE omie_nfe ALTER COLUMN status SET DEFAULT 'pendente_consulta';")
    await conn.execute("ALTER TABLE omie_nfe ADD COLUMN IF NOT EXISTS last_error TEXT;")
    await conn.execute("ALTER TABLE omie_nfe ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ;")
    await conn.execute("ALTER TABLE omie_nfe ALTER COLUMN recebido_em SET DEFAULT now();")

    # Índices úteis
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_nfe_status ON omie_nfe(status);")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_nfe_idnf   ON omie_nfe(id_nf);")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_nfe_chave  ON omie_nfe(chave);")

# ------------------------------------------------------------
# Helpers de admin (auth)
# ------------------------------------------------------------
def _get_admin_secret() -> str:
    return ADMIN_SECRET

async def admin_auth(
    request: Request,
    x_admin_secret: Optional[str] = None,
    secret_q: Optional[str] = Query(None),
) -> bool:
    expected = _get_admin_secret()
    if not expected:
        raise HTTPException(status_code=401, detail="unauthorized")

    # Header X-Admin-Secret
    hdr = request.headers.get("x-admin-secret") or x_admin_secret
    if hdr and hdr == expected:
        return True

    # Authorization: Bearer <secret>
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer ") and auth.split(" ", 1)[1].strip() == expected:
        return True

    # Query param (?secret=) – útil para testes/ping
    if secret_q and secret_q == expected:
        return True

    raise HTTPException(status_code=401, detail="unauthorized")

# ------------------------------------------------------------
# Persistência simples
# ------------------------------------------------------------
async def queue_xml_event(conn: asyncpg.Connection,
                          chave: Optional[str],
                          id_nf: Optional[int],
                          data_emis: Optional[datetime]) -> None:
    await _safe_bootstrap(conn)
    await conn.execute(
        "INSERT INTO omie_nfe (chave, id_nf, data_emis, status, recebido_em) "
        "VALUES ($1, $2, $3, 'pendente_consulta', now());",
        chave, id_nf, data_emis
    )

async def set_xml_and_status(conn: asyncpg.Connection,
                             chave: Optional[str],
                             id_nf: Optional[int],
                             xml_text: str,
                             status: str = "processado_ok") -> None:
    await conn.execute(
        "UPDATE omie_nfe SET xml_text=$1, status=$2, last_error=NULL "
        "WHERE ctid IN (SELECT ctid FROM omie_nfe "
        "               WHERE status='pendente_consulta' "
        "               ORDER BY recebido_em ASC LIMIT 1);",
        xml_text, status
    )

async def mark_error(conn: asyncpg.Connection, err_msg: str) -> None:
    await conn.execute(
        "UPDATE omie_nfe SET status='processado_erro', last_error=$1 "
        "WHERE ctid IN (SELECT ctid FROM omie_nfe "
        "               WHERE status='pendente_consulta' "
        "               ORDER BY recebido_em ASC LIMIT 1);",
        err_msg[:1500]
    )

# ------------------------------------------------------------
# Integração Omie – (stub simples/robusta)
# ------------------------------------------------------------
async def _buscar_xml_por_chave(chave: Optional[str], dt_emis: Optional[datetime]) -> Optional[str]:
    """
    Busca o XML pela chave (ou janela de emissão). Implementação defensiva:
    - Se credenciais não estiverem definidas, devolve None sem quebrar o job.
    - Se a Omie retornar erro, devolve None (o item vai para 'processado_erro').
    """
    if not OMIE_APP_KEY or not OMIE_APP_SECRET or not OMIE_XML_URL:
        return None

    payload = {
        "call": OMIE_XML_LIST_CALL,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            # A API real da Omie exige estrutura específica.
            # Para não quebrar, mandamos um filtro básico por chave quando existir.
            "chaveNFe": chave or "",
            "pagina": 1,
            "registros_por_pagina": 50
        }]
    }

    timeout = httpx.Timeout(OMIE_TIMEOUT)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(OMIE_XML_URL, json=payload)
            # Se a Omie responder 4xx/5xx, não estoura – tratamos como não encontrado
            if resp.status_code >= 400:
                return None
            data = resp.json()
    except Exception:
        return None

    # Aqui você faria o parse real e pegaria o XML retornado.
    # Para manter robusto, devolvemos None se não achou.
    # Exemplo fictício:
    xml_text = None
    # if "xml" in data: xml_text = data["xml"]
    return xml_text

# ------------------------------------------------------------
# Job – processar pendentes
# ------------------------------------------------------------
async def _processar_pendentes(conn: asyncpg.Connection, limit: int):
    await _safe_bootstrap(conn)
    rows = await conn.fetch(
        "SELECT ctid, chave, id_nf, data_emis FROM omie_nfe "
        "WHERE status='pendente_consulta' "
        "ORDER BY recebido_em ASC LIMIT $1;",
        limit
    )
    ok, err, refs_ok = 0, 0, []
    for _ in rows:
        try:
            xml_text = await _buscar_xml_por_chave(None, None)  # ajuste conforme sua regra (chave/datas)
            if xml_text:
                await set_xml_and_status(conn, None, None, xml_text, "processado_ok")
                ok += 1
                refs_ok.append("ok")
            else:
                await mark_error(conn, "xml_not_found")
                err += 1
        except Exception:
            await mark_error(conn, "unexpected_error")
            err += 1
    return ok, err, refs_ok, len(rows)

# ------------------------------------------------------------
# Modo COMPAT e Webhook nativo
# ------------------------------------------------------------
def _find_first_key(d: Dict[str, Any], keys: set) -> Optional[str]:
    for k in keys:
        if k in d:
            return k
    return None

def _parse_iso_dt(txt: Optional[str]) -> Optional[datetime]:
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00"))
    except Exception:
        return None

async def handle_xml_event_from_dict(body: dict) -> dict:
    """
    Tratar um evento de NF-e vindo como dict:
    - Extrai chave, id_nf e data_emis quando existirem;
    - Enfileira como 'pendente_consulta' (será tratado no job).
    """
    try:
        evento = body.get("evento") if isinstance(body, dict) else None
        if not isinstance(evento, dict):
            # algumas integrações mandam o objeto direto
            evento = body

        chave = None
        id_nf = None
        data_emis = None

        if isinstance(evento, dict):
            # chave
            k = _find_first_key(evento, WANTED_CHAVE_KEYS)
            if k:
                chave = str(evento.get(k) or "").strip() or None
            # id_nf
            kid = _find_first_key(evento, WANTED_IDNF_KEYS)
            if kid:
                v = evento.get(kid)
                try:
                    id_nf = int(v) if v is not None and str(v).strip() != "" else None
                except Exception:
                    id_nf = None
            # data emissão
            data_emis = _parse_iso_dt(str(evento.get("data_emis") or ""))

        if not chave and id_nf is None:
            # nada para registrar
            return {"ok": True, "ignorado": True}

        pool = await get_pool()
        async with pool.acquire() as conn:
            await queue_xml_event(conn, chave, id_nf, data_emis)

        return {"ok": True, "queued": True, "ref": {"chave": chave, "id_nf": id_nf}}
    except Exception:
        return JSONResponse({"ok": False, "error": "internal_error"}, status_code=200)

@app.post("/omie/xml/webhook")
async def omie_xml_webhook(request: Request, token: str = Query(default="")):
    if OMIE_WEBHOOK_TOKEN_XML and token != OMIE_WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=401, detail="invalid token")
    body = await request.json()
    result = await handle_xml_event_from_dict(body)
    return result

# ------------------------------------------------------------
# Endpoints admin – seguros/robustos
# ------------------------------------------------------------
@app.post("/admin/nfe/reprocessar-pendentes")
async def nfe_reprocessar_pendentes(
    _: bool = Depends(admin_auth),
    limit_q: Optional[int] = Query(50),
    limit_f: Optional[int] = Form(None),
):
    try:
        limit = limit_f if limit_f is not None else limit_q or 50
        pool = await get_pool()
        async with pool.acquire() as conn:
            await _safe_bootstrap(conn)
            ok, err, refs_ok, found = await _processar_pendentes(conn, limit)
        return {"ok": True, "pendentes_encontrados": found, "processados_ok": ok, "processados_erro": err, "refs_ok": refs_ok}
    except HTTPException:
        raise
    except Exception:
        # nunca deixar estourar 500
        return JSONResponse({"ok": False, "error": "internal_error"}, status_code=200)

@app.post("/admin/nfe/reprocessar")
async def nfe_reprocessar(
    _: bool = Depends(admin_auth),
    chave_q: Optional[str] = Query(None),
    id_nf_q: Optional[int] = Query(None),
    data_emis_iso_q: Optional[str] = Query(None),
    chave_f: Optional[str] = Form(None),
    id_nf_f: Optional[int] = Form(None),
    data_emis_iso_f: Optional[str] = Form(None),
):
    """
    Reprocessa um único registro por chave OU id_nf.
    """
    try:
        chave = (chave_q or chave_f or "").strip() or None
        id_nf  = id_nf_q if id_nf_q is not None else id_nf_f
        data_emis_iso = data_emis_iso_q or data_emis_iso_f
        if not chave and id_nf is None:
            raise HTTPException(status_code=400, detail="Informe 'chave' ou 'id_nf'")
        dt_emis = _parse_iso_dt(data_emis_iso) if data_emis_iso else None

        pool = await get_pool()
        async with pool.acquire() as conn:
            await _safe_bootstrap(conn)
            xml_text = await _buscar_xml_por_chave(chave, dt_emis)
            if xml_text:
                await set_xml_and_status(conn, chave, id_nf, xml_text, "processado_ok")
                return {"ok": True, "ref": {"chave": chave, "id_nf": id_nf}}
            else:
                await mark_error(conn, "xml_not_found")
                return JSONResponse({"ok": False, "error": "xml_not_found"}, status_code=200)
    except HTTPException:
        raise
    except Exception:
        return JSONResponse({"ok": False, "error": "internal_error"}, status_code=200)

# ------------------------------------------------------------
# DIAGNÓSTICOS (remova depois de estabilizar)
# ------------------------------------------------------------
@app.get("/admin/_debug-secret")
async def debug_secret(_: bool = Depends(admin_auth)):
    s = _get_admin_secret()
    import hashlib
    sha8 = hashlib.sha256(s.encode()).hexdigest()[:8]
    return {"env_len": len(s), "env_sha8": sha8}

@app.get("/admin/nfe/_diag_conn")
async def nfe_diag_conn(_: bool = Depends(admin_auth)):
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            v = await conn.fetchval("SELECT 1;")
        return {"ok": True, "db": "ok", "select1": v}
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": "conn_failed", "type": e.__class__.__name__, "msg": str(e)},
            status_code=200,
        )

@app.get("/admin/nfe/_diag_raw")
async def nfe_diag_raw(_: bool = Depends(admin_auth)):
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await _safe_bootstrap(conn)
            pend = await conn.fetchval(
                "SELECT COUNT(*) FROM omie_nfe WHERE status='pendente_consulta'"
            )
        return {"ok": True, "db": "ok", "pendentes": int(pend)}
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": "diag_failed", "type": e.__class__.__name__, "msg": str(e)},
            status_code=200,
        )

# ------------------------------------------------------------
# Fim
# ------------------------------------------------------------
