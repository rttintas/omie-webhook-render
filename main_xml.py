# main_xml.py — Omie NF-e XML (inbox + consulta + reprocesso)
import os
import json
import logging
import traceback
import hashlib
import secrets
from typing import Any, Optional, Tuple
from datetime import datetime, timedelta, timezone

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Query, Form, Header, Depends
from fastapi.responses import JSONResponse

# ------------------------------------------------------------
# App & Logger
# ------------------------------------------------------------
app = FastAPI(title="RTT Omie - NFe/XML")
logger = logging.getLogger("uvicorn.error")

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
DATABASE_URL    = os.getenv("DATABASE_URL", "")

OMIE_APP_KEY    = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")

# Segredo: prioriza ADMIN_SECRET_XML; fallback ADMIN_SECRET; remove espaços
def get_admin_secret_env() -> str:
    return (os.getenv("ADMIN_SECRET_XML") or os.getenv("ADMIN_SECRET") or "").strip()

# Token só para o endpoint nativo de NFe (se/quando você apontar o webhook pra /xml/...)
OMIE_WEBHOOK_TOKEN_XML = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "")

# HTTP timeouts
OMIE_TIMEOUT = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))  # chamadas API Omie
URL_TIMEOUT  = int(os.getenv("URL_TIMEOUT_SECONDS", "20"))   # baixar XML por URL

# API Omie XML
OMIE_XML_URL       = os.getenv("OMIE_XML_URL", "https://app.omie.com.br/api/v1/contador/xml/")
OMIE_XML_LIST_CALL = os.getenv("OMIE_XML_LIST_CALL", "ListarDocumentos")

# Janela (dias) para procurar documento por data de emissão quando necessário
LOOKBACK_DAYS  = int(os.getenv("NFE_LOOKBACK_DAYS", "7"))
LOOKAHEAD_DAYS = int(os.getenv("NFE_LOOKAHEAD_DAYS", "1"))

_pool: Optional[asyncpg.Pool] = None

# ------------------------------------------------------------
# DEBUG TEMPORÁRIO — remova após o teste
# Mostra o tamanho e um hash parcial do segredo carregado (sem vazar valor)
# Montado em /xml/admin/_debug-secret porque este app é montado em /xml
@app.get("/admin/_debug-secret")
@app.get("/xml/admin/_debug-secret")  # alias (evita erro de path duplicado acidental)
def _debug_secret():
    s = get_admin_secret_env()
    return {"env_len": len(s), "env_sha8": hashlib.sha256(s.encode()).hexdigest()[:8]}

# ------------------------------------------------------------
# Auth: aceita segredo por Header, Bearer, Query e Form
async def admin_auth(
    secret_q: Optional[str] = Query(None),
    secret_f: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None),
    x_admin_secret: Optional[str] = Header(None),
):
    # prioridade: X-Admin-Secret > Bearer <token> > query > form
    provided = (
        x_admin_secret
        or (authorization.split(" ", 1)[1] if authorization and authorization.startswith("Bearer ") else None)
        or secret_q
        or secret_f
        or ""
    )
    provided = provided.strip()
    env_secret = get_admin_secret_env()

    # diagnóstico leve (sem expor segredo)
    recv_sha8 = hashlib.sha256(provided.encode()).hexdigest()[:8] if provided else ""
    env_sha8  = hashlib.sha256(env_secret.encode()).hexdigest()[:8] if env_secret else ""
    if not secrets.compare_digest(provided, env_secret):
        try:
            print({
                "tag": "nfe_auth_fail",
                "recv_len": len(provided), "env_len": len(env_secret),
                "recv_sha8": recv_sha8, "env_sha8": env_sha8
            })
        except Exception:
            pass
        raise HTTPException(status_code=401, detail="unauthorized")
    return True

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
# Campos esperados no webhook (conforme doc da Omie de NF-e):
# evento.nfe_chave, evento.nfe_xml, evento.nfe_danfe, evento.numero_nf, evento.série, evento.id_nf, evento.id_pedido, evento.data_emis, etc.
WANTED_CHAVE_KEYS  = {"nfe_chave", "nChave", "chave", "chNFe", "chaveNFe", "nfeChave"}
WANTED_IDNF_KEYS   = {"id_nf", "nIdNF", "idNFe", "idNotaFiscal"}
WANTED_IDPED_KEYS  = {"id_pedido", "nIdPedido", "idPedido"}
WANTED_NUM_KEYS    = {"numero_nf", "nNumero", "numero", "numeroNota", "numeroNFe"}
WANTED_SERIE_KEYS  = {"série", "cSerie", "serie", "serie_nfe"}
WANTED_XMLURL_KEYS = {"nfe_xml", "xml_url", "danfe_xml_url"}

def deep_find_first(obj: Any, keys: set[str]) -> Optional[Any]:
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

def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                return int(s)
            except Exception:
                return None
    return None

def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

# ------------------------------------------------------------
# Conexão + bootstrap
# ------------------------------------------------------------
async def get_pool() -> asyncpg.Pool:
    global _pool
    if not _pool:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL não configurada")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        logger.info({"tag": "startup", "msg": "Pool OK"})
    return _pool

async def _safe_bootstrap(conn: asyncpg.Connection):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS omie_nfe (
            id SERIAL PRIMARY KEY,
            id_nf BIGINT,
            id_pedido BIGINT,
            numero_nf TEXT,
            serie TEXT,
            chave TEXT,
            data_emis TIMESTAMPTZ,
            raw_evento JSONB,
            raw_xml TEXT,
            danfe_url TEXT,
            status TEXT,
            recebido_em TIMESTAMPTZ DEFAULT now(),
            consultado_em TIMESTAMPTZ,
            UNIQUE(chave)
        );

        CREATE INDEX IF NOT EXISTS idx_omie_nfe_status ON omie_nfe(status);
        CREATE INDEX IF NOT EXISTS idx_omie_nfe_idnf ON omie_nfe(id_nf);
        CREATE INDEX IF NOT EXISTS idx_omie_nfe_num ON omie_nfe(numero_nf);
        """
    )

# ------------------------------------------------------------
# Omie XML API
# ------------------------------------------------------------
async def omie_xml_listar_documentos(d_ini: Optional[str], d_fim: Optional[str],
                                     pagina: int = 1, por_pagina: int = 50) -> dict:
    """
    Chama ListarDocumentos com janela de emissão (datas DD/MM/AAAA).
    """
    payload = {
        "call": OMIE_XML_LIST_CALL,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "nPagina": pagina,
            "nRegPorPagina": por_pagina,
            "cModelo": "55",
            "dEmiInicial": d_ini or "",
            "dEmiFinal":   d_fim or ""
        }]
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(OMIE_TIMEOUT)) as client:
        logger.info({"tag": "omie_xml_out",
                     "payload": {**payload, "app_key": "***", "app_secret": "***"}})
        r = await client.post(OMIE_XML_URL, json=payload)
        try:
            data = r.json()
        except Exception:
            data = {"_non_json_body": r.text}
        logger.info({"tag": "omie_xml_in", "status": r.status_code, "has_result": isinstance(data, dict)})
        r.raise_for_status()
        return data

async def _baixar_url_texto(url: str) -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(URL_TIMEOUT)) as client:
            r = await client.get(url)
            if r.status_code == 200 and r.text:
                return r.text
    except Exception as e:
        logger.warning({"tag": "download_xml_fail", "err": str(e)})
    return None

# ------------------------------------------------------------
# Banco: helpers
# ------------------------------------------------------------
async def upsert_inbox_xml(conn: asyncpg.Connection,
                           chave: Optional[str],
                           id_nf: Optional[int],
                           id_pedido: Optional[int],
                           numero_nf: Optional[str],
                           serie: Optional[str],
                           data_emis: Optional[datetime],
                           danfe_url: Optional[str],
                           raw_evento: dict):
    if chave:
        await conn.execute(
            """
            INSERT INTO omie_nfe (chave, id_nf, id_pedido, numero_nf, serie, data_emis, danfe_url, raw_evento, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, 'pendente_consulta')
            ON CONFLICT (chave) DO UPDATE
               SET id_nf = EXCLUDED.id_nf,
                   id_pedido = EXCLUDED.id_pedido,
                   numero_nf = EXCLUDED.numero_nf,
                   serie = EXCLUDED.serie,
                   data_emis = EXCLUDED.data_emis,
                   danfe_url = COALESCE(EXCLUDED.danfe_url, omie_nfe.danfe_url),
                   raw_evento = EXCLUDED.raw_evento,
                   status = 'pendente_consulta',
                   recebido_em = now()
            """,
            chave, id_nf, id_pedido, numero_nf, serie, data_emis, danfe_url, json.dumps(raw_evento)
        )
    else:
        await conn.execute(
            """
            INSERT INTO omie_nfe (chave, id_nf, id_pedido, numero_nf, serie, data_emis, danfe_url, raw_evento, status)
            VALUES (NULL, $1, $2, $3, $4, $5, $6, $7::jsonb, 'pendente_consulta')
            """,
            id_nf, id_pedido, numero_nf, serie, data_emis, danfe_url, json.dumps(raw_evento)
        )

async def set_xml_and_status(conn: asyncpg.Connection, chave: Optional[str], id_nf: Optional[int], xml_text: str):
    if chave:
        await conn.execute(
            """
            UPDATE omie_nfe
               SET raw_xml = $1,
                   status = 'consultado',
                   consultado_em = now()
             WHERE chave = $2
            """,
            xml_text, chave
        )
    elif id_nf is not None:
        await conn.execute(
            """
            UPDATE omie_nfe
               SET raw_xml = $1,
                   status = 'consultado',
                   consultado_em = now()
             WHERE id_nf = $2
            """,
            xml_text, id_nf
        )
    else:
        raise ValueError("Sem chave ou id_nf para atualizar o XML.")

async def fetch_pendentes(conn: asyncpg.Connection, limit: int = 50):
    return await conn.fetch(
        """
        SELECT chave, id_nf, numero_nf, data_emis
          FROM omie_nfe
         WHERE status = 'pendente_consulta'
         ORDER BY recebido_em ASC
         LIMIT $1
        """,
        limit
    )

# ------------------------------------------------------------
# Endpoints básicos
# ------------------------------------------------------------
@app.get("/")
async def root():
    return {"status": "ok", "service": "nfe-xml"}

@app.get("/healthz")
async def healthz():
    return {"status": "healthy"}

# ------------------------------------------------------------
# Webhook NFe (nativo, quando você apontar a Omie para /xml/...)
# OBS: como este app é montado em /xml, o caminho final fica /xml/omie/xml/webhook
#     (ou ajuste aqui para outro path caso deseje)
# ------------------------------------------------------------
@app.post("/omie/xml/webhook")
async def omie_xml_webhook(request: Request, token: str = Query(default="")):
    if OMIE_WEBHOOK_TOKEN_XML and token != OMIE_WEBHOOK_TOKEN_XML:
        raise HTTPException(status_code=401, detail="invalid token")
    body = await request.json()
    result = await handle_xml_event_from_dict(body)
    return result

# ------------------------------------------------------------
# Função pública para MODO COMPAT (usada pelo app combinado no /omie/webhook)
# ------------------------------------------------------------
async def handle_xml_event_from_dict(body: dict) -> dict:
    """Trata um evento de NF-e vindo como dict (modo compat OU endpoint nativo)."""
    evento = body.get("evento") if isinstance(body, dict) else None

    chave = str(deep_find_first(body, WANTED_CHAVE_KEYS) or "") or None
    id_nf = _as_int(deep_find_first(body, WANTED_IDNF_KEYS))
    id_pedido = _as_int(deep_find_first(body, WANTED_IDPED_KEYS))

    numero_nf = None
    num_any = deep_find_first(body, WANTED_NUM_KEYS)
    if num_any is not None:
        numero_nf = str(num_any)

    serie = None
    serie_any = deep_find_first(body, WANTED_SERIE_KEYS)
    if serie_any is not None:
        serie = str(serie_any)

    data_emis_iso = None
    if isinstance(evento, dict):
        data_emis_iso = evento.get("data_emis") or evento.get("dhEmi")
    dt_emis = _parse_iso_dt(data_emis_iso)

    danfe_url = None
    xml_url = None
    if isinstance(evento, dict):
        danfe_url = evento.get("nfe_danfe") or evento.get("danfe")
        xml_url = evento.get("nfe_xml") or deep_find_first(body, WANTED_XMLURL_KEYS)

    logger.info({
        "tag": "omie_nfe_event_received",
        "chave": chave, "id_nf": id_nf, "id_pedido": id_pedido,
        "numero_nf": numero_nf, "serie": serie, "data_emis_iso": data_emis_iso,
        "xml_url_present": bool(xml_url)
    })

    pool = await get_pool()
    async with pool.acquire() as conn:
        await _safe_bootstrap(conn)
        try:
            # Inbox
            await upsert_inbox_xml(conn, chave, id_nf, id_pedido, numero_nf, serie, dt_emis, danfe_url, body)

            # Tentativa imediata: baixar XML pelo URL do evento (se existir)
            if xml_url:
                xml_text = await _baixar_url_texto(xml_url)
                if xml_text:
                    await set_xml_and_status(conn, chave, id_nf, xml_text)

        except Exception:
            logger.error({
                "tag": "omie_nfe_inbox_error",
                "err": traceback.format_exc(),
                "chave": chave, "id_nf": id_nf, "numero_nf": numero_nf
            })
            # aceita para processamento posterior
            return {"ok": False, "accepted": True}

    return {"ok": True}

# ------------------------------------------------------------
# Reprocesso
# ------------------------------------------------------------
def _janela_ddmmaa(dt: Optional[datetime]) -> Tuple[Optional[str], Optional[str]]:
    if dt is None:
        hoje = datetime.now(timezone.utc)
        d_ini = (hoje - timedelta(days=LOOKBACK_DAYS)).astimezone().strftime("%d/%m/%Y")
        d_fim = (hoje + timedelta(days=LOOKAHEAD_DAYS)).astimezone().strftime("%d/%m/%Y")
        return d_ini, d_fim
    else:
        d_ini_dt = (dt - timedelta(days=1))
        d_fim_dt = (dt + timedelta(days=1))
        return d_ini_dt.strftime("%d/%m/%Y"), d_fim_dt.strftime("%d/%m/%Y")

async def _buscar_xml_por_chave(chave: str, dt_emis: Optional[datetime]) -> Optional[str]:
    d_ini, d_fim = _janela_ddmmaa(dt_emis)
    pagina = 1
    while True:
        data = await omie_xml_listar_documentos(d_ini, d_fim, pagina=pagina, por_pagina=50)

        docs = data.get("documentos") or data.get("lista") or data.get("ListarDocumentos") or []
        if isinstance(docs, dict) and "documentos" in docs:
            docs = docs["documentos"]

        if isinstance(docs, list):
            for it in docs:
                if not isinstance(it, dict):
                    continue
                nChave = it.get("nChave") or it.get("nchave") or it.get("chave")
                if (nChave or "") == chave:
                    found = it.get("cXml") or it.get("xml") or it.get("conteudo")
                    if found:
                        return found

        tot = data.get("nTotPaginas") or data.get("total_paginas")
        if not tot or pagina >= int(tot):
            break
        pagina += 1

    return None

async def _processar_pendentes(conn: asyncpg.Connection, limit: int):
    ok = err = 0
    refs_ok = []
    rows = await fetch_pendentes(conn, limit=limit)

    for row in rows:
        chave = row["chave"]
        id_nf = row["id_nf"]
        dt_emis = row["data_emis"]

        try:
            logger.info({"tag": "nfe_reprocess_start", "chave": chave, "id_nf": id_nf})

            xml_text: Optional[str] = None
            if chave:
                xml_text = await _buscar_xml_por_chave(chave, dt_emis)

            if xml_text:
                await set_xml_and_status(conn, chave, id_nf, xml_text)
                ok += 1
                refs_ok.append(chave or id_nf)
                logger.info({"tag": "nfe_reprocess_ok", "ref": chave or id_nf})
            else:
                logger.warning({"tag": "nfe_reprocess_not_found", "ref": chave or id_nf})
        except Exception:
            err += 1
            logger.error({"tag": "nfe_reprocess_error", "ref": chave or id_nf, "err": traceback.format_exc()})

    return ok, err, refs_ok, len(rows)

# ------------------------------------------------------------
# ADMIN endpoints (com auth robusta)
# ------------------------------------------------------------
@app.post("/admin/nfe/reprocessar-pendentes")
async def nfe_reprocessar_pendentes(
    _: bool = Depends(admin_auth),
    limit_q: Optional[int] = Query(None),
    limit_f: Optional[int] = Form(None),
):
    try:
        limit = (limit_q if limit_q is not None else limit_f) or 50
        pool = await get_pool()
        async with pool.acquire() as conn:
            await _safe_bootstrap(conn)
            ok, err, refs_ok, found = await _processar_pendentes(conn, limit)
        return {"ok": True, "pendentes_encontrados": found, "processados_ok": ok, "processados_erro": err, "refs_ok": refs_ok}
    except HTTPException:
        raise
    except Exception:
        logger.error({"tag": "nfe_reprocess_pendentes_error", "err": traceback.format_exc()})
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
    try:
        chave = (chave_q or chave_f)
        id_nf = (id_nf_q if id_nf_q is not None else id_nf_f)
        data_emis_iso = (data_emis_iso_q or data_emis_iso_f)

        if not chave and id_nf is None:
            raise HTTPException(status_code=400, detail="Informe 'chave' ou 'id_nf'")


        dt_emis = _parse_iso_dt(data_emis_iso) if data_emis_iso else None
        pool = await get_pool()
        async with pool.acquire() as conn:
            await _safe_bootstrap(conn)
            if chave:
                xml_text = await _buscar_xml_por_chave(chave, dt_emis)
                if not xml_text:
                    raise HTTPException(status_code=404, detail="XML não encontrado por chave/datas")
                await set_xml_and_status(conn, chave, None, xml_text)
                ref = {"chave": chave}
            else:
                row = await conn.fetchrow(
                    "SELECT chave, data_emis FROM omie_nfe WHERE id_nf=$1 ORDER BY recebido_em DESC LIMIT 1",
                    id_nf
                )
                if not (row and row["chave"]):
                    raise HTTPException(status_code=404, detail="Sem chave associada a esse id_nf")
                xml_text = await _buscar_xml_por_chave(row["chave"], row["data_emis"])
                if not xml_text:
                    raise HTTPException(status_code=404, detail="XML não encontrado para id_nf")
                await set_xml_and_status(conn, row["chave"], id_nf, xml_text)
                ref = {"id_nf": id_nf, "chave": row["chave"]}
        return {"ok": True, "ref": ref}
    except HTTPException:
        raise
    except Exception:
        logger.error({"tag": "nfe_reprocess_one_error", "err": traceback.format_exc()})
        return JSONResponse({"ok": False, "error": "internal_error"}, status_code=200)
# ---------- DIAGNÓSTICO RÁPIDO ----------
@app.get("/admin/nfe/_diag")
async def nfe_diag(_: bool = Depends(admin_auth)):
    """
    Verifica conexão com o banco e quantos pendentes existem.
    NUNCA retorna 401 se o header X-Admin-Secret estiver correto.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await _safe_bootstrap(conn)
            pend = await conn.fetchval(
                "SELECT COUNT(*) FROM omie_nfe WHERE status='pendente_consulta'"
            )
        return {"ok": True, "db": "ok", "pendentes": int(pend)}
    except Exception:
        logger.error({"tag": "nfe_diag_error", "err": traceback.format_exc()})
        return JSONResponse({"ok": False, "error": "diag_failed"}, status_code=200)
# ---------- FIM DIAGNÓSTICO ----------
# ---------- DIAGNÓSTICOS VERBOSOS ----------
from fastapi import Depends
from fastapi.responses import JSONResponse

@app.get("/admin/nfe/_diag_conn")
async def nfe_diag_conn(_: bool = Depends(admin_auth)):
    """
    Testa somente conexão ao Postgres (SELECT 1).
    """
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
    """
    Igual ao _diag, mas devolve tipo e msg da exception para sabermos a causa.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await _safe_bootstrap(conn)  # cria tabela se faltar
            pend = await conn.fetchval(
                "SELECT COUNT(*) FROM omie_nfe WHERE status='pendente_consulta'"
            )
        return {"ok": True, "db": "ok", "pendentes": int(pend)}
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": "diag_failed", "type": e.__class__.__name__, "msg": str(e)},
            status_code=200,
        )
# ---------- FIM DIAGNÓSTICOS ----------


# ------------------------------------------------------------
# Rodar local:
# uvicorn main_xml:app --host 0.0.0.0 --port 8001
