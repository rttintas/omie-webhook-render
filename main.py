
# main.py — Omie pedidos + NF-e XML (backfill, sync e visão para expedição)
import os
import json
import logging
import traceback
from typing import Any, Optional
from datetime import datetime, timezone
from dateutil import parser as dtparse

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Query

# ------------------------------------------------------------
# Configuração
# ------------------------------------------------------------
app = FastAPI()
logger = logging.getLogger("uvicorn.error")

DATABASE_URL       = os.getenv("DATABASE_URL", "")
ADMIN_SECRET       = os.getenv("ADMIN_SECRET", "")
OMIE_APP_KEY       = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET    = os.getenv("OMIE_APP_SECRET", "")
OMIE_WEBHOOK_TOKEN = os.getenv("OMIE_WEBHOOK_TOKEN", "")
OMIE_TIMEOUT       = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

# APIs Omie
OMIE_PEDIDO_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_XML_URL    = "https://app.omie.com.br/api/v1/contador/xml/"

_pool: Optional[asyncpg.Pool] = None

# ------------------------------------------------------------
# Conexão com o banco / Migrações idempotentes
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
        -- Tabela de pedidos (inbox + detalhes)
        CREATE TABLE IF NOT EXISTS omie_pedido (
          pk bigserial PRIMARY KEY,
          recebido_em timestamptz NOT NULL DEFAULT now(),
          status text NOT NULL DEFAULT 'pendente_consulta'
            CHECK (status IN ('pendente_consulta','consultado','sem_chave')),
          numero text,                         -- nº do pedido (Omie)
          id_pedido_omie bigint,               -- id interno Omie
          codigo_pedido_integracao text,       -- ex.: OH...
          raw_basico  jsonb,                   -- payload básico (webhook)
          raw_detalhe jsonb                    -- payload de detalhes (ConsultarPedido)
        );

        -- Índices e restrições
        CREATE INDEX IF NOT EXISTS ix_omie_recebido   ON omie_pedido (recebido_em DESC);
        CREATE INDEX IF NOT EXISTS ix_omie_status     ON omie_pedido (status);
        CREATE INDEX IF NOT EXISTS ix_omie_integr     ON omie_pedido (codigo_pedido_integracao);

        -- Únicos parciais (evitam duplicar quando o valor existe, permitem múltiplos NULL)
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_numero
          ON omie_pedido (numero) WHERE numero IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_id
          ON omie_pedido (id_pedido_omie) WHERE id_pedido_omie IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_codint
          ON omie_pedido (codigo_pedido_integracao) WHERE codigo_pedido_integracao IS NOT NULL;

        -- Tabela de NF-e (XML)
        CREATE TABLE IF NOT EXISTS omie_nf_xml (
          id bigserial PRIMARY KEY,
          recebido_em timestamptz NOT NULL DEFAULT now(),
          id_nf bigint UNIQUE,                 -- nIdNF
          id_pedido_omie bigint,               -- nIdPedido (liga no pedido)
          n_chave text UNIQUE,                 -- nChave
          numero_nf text,                      -- nNumero (TEXT para preservar zeros à esquerda)
          dh_emissao timestamptz,              -- dEmi + hEmi
          xml text,                            -- cXml
          cliente_nome text,                   -- xNome (dest)
          pedido_marketplace text              -- xPed
        );
        CREATE INDEX IF NOT EXISTS ix_nf_pedido  ON omie_nf_xml (id_pedido_omie);
        CREATE INDEX IF NOT EXISTS ix_nf_emissao ON omie_nf_xml (dh_emissao DESC);

        -- View para expedição (join pronto)
        CREATE OR REPLACE VIEW vw_expedicao AS
        SELECT
          nf.recebido_em         AS recebido_xml_em,
          nf.numero_nf,
          nf.n_chave,
          nf.dh_emissao,
          nf.cliente_nome,
          nf.pedido_marketplace,
          p.numero               AS numero_pedido,
          p.id_pedido_omie,
          COALESCE(
            p.raw_detalhe->'pedido_venda_produto'->'cabecalho'->>'origem_pedido',
            p.raw_basico->'event'->>'origem_pedido'
          ) AS marketplace,
          p.raw_detalhe->'pedido_venda_produto'->'observacoes'->>'obs_venda' AS descricao_pedido,
          (
            SELECT COALESCE(SUM( (it->'produto'->>'quantidade')::numeric ),0)
            FROM jsonb_array_elements(p.raw_detalhe->'pedido_venda_produto'->'det') AS it
          )::numeric(12,3) AS qty_total
        FROM omie_nf_xml nf
        LEFT JOIN omie_pedido p
          ON p.id_pedido_omie = nf.id_pedido_omie;
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
WANTED_ID_KEYS     = {"idPedido", "id_pedido", "idPedidoOmie", "idPedidoVenda", "id_pedido_omie", "codigo_pedido"}
WANTED_NUM_KEYS    = {"numeroPedido", "numero_pedido", "numero"}
WANTED_INTEGR_KEYS = {"codigo_pedido_integracao", "codigoPedidoIntegracao"}

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

def deep_find_first(obj: Any, keys: set[str]) -> Optional[Any]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys:
                return v
            found = deep_find_first(v, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = deep_find_first(it, keys)
            if found is not None:
                return found
    return None

def looks_like_integr(code: Optional[str]) -> bool:
    return bool(code) and not str(code).isdigit()

# ------------------------------------------------------------
# Omie: ConsultarPedido
# ------------------------------------------------------------
def _param_consultar(
    codigo_pedido: int | None = None,
    codigo_pedido_integracao: str | None = None,
    idPedido_legacy: int | None = None,
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
    return param

async def omie_consultar_pedido(
    codigo_pedido: int | None = None,
    codigo_pedido_integracao: str | None = None,
    _idPedido_legacy: int | None = None,
) -> dict:
    payload = {
        "call": "ConsultarPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [_param_consultar(codigo_pedido, codigo_pedido_integracao, _idPedido_legacy)],
    }
    assert "idPedido" not in json.dumps(payload), "Não envie idPedido para ConsultarPedido"

    async with httpx.AsyncClient(timeout=httpx.Timeout(OMIE_TIMEOUT)) as client:
        logger.info({"tag": "omie_http_out", "payload": payload})
        r = await client.post(OMIE_PEDIDO_URL, json=payload)
        try:
            data = r.json()
        except Exception:
            data = {"_non_json_body": r.text}
        logger.info({"tag": "omie_http_in", "status": r.status_code, "fault": (data.get("faultstring") if isinstance(data, dict) else None)})
        r.raise_for_status()
        return data

# ------------------------------------------------------------
# Omie: PesquisarPedidos (backfill)
# ------------------------------------------------------------
async def _omie_pesquisar_pedidos(d_ini: datetime, d_fim: datetime, pagina: int, por_pagina: int) -> dict:
    def ddmmaaaa(d: datetime) -> str:
        return d.strftime("%d/%m/%Y")

    payload = {
        "call": "PesquisarPedidos",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "pagina": pagina,
            "registros_por_pagina": por_pagina,
            "filtrar_por_data_de": ddmmaaaa(d_ini),
            "filtrar_por_data_ate": ddmmaaaa(d_fim),
            "ordenar_por": "DATA",
            "ordem_decrescente": "N"
        }],
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(OMIE_TIMEOUT)) as client:
        r = await client.post(OMIE_PEDIDO_URL, json=payload)
        r.raise_for_status()
        return r.json()

# ------------------------------------------------------------
# Omie: ListarDocumentos (NF-e XML)
# ------------------------------------------------------------
async def _omie_listar_documentos_xml(pagina: int, por_pagina: int, d_ini: Optional[datetime], d_fim: Optional[datetime]) -> dict:
    def fmt(d: Optional[datetime]) -> str:
        return d.strftime("%d/%m/%Y") if d else ""

    payload = {
        "call": "ListarDocumentos",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "nPagina": pagina,
            "nRegPorPagina": por_pagina,
            "cModelo": "55",
            "dEmiInicial": fmt(d_ini),
            "dEmiFinal": fmt(d_fim),
        }],
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(OMIE_TIMEOUT)) as client:
        r = await client.post(OMIE_XML_URL, json=payload)
        r.raise_for_status()
        return r.json()

# ------------------------------------------------------------
# Banco: helpers de pedidos
# ------------------------------------------------------------
async def insert_inbox_safe(
    conn: asyncpg.Connection,
    numero: Optional[str],
    id_pedido: Optional[int],
    body: dict,
    codigo_pedido_integracao: Optional[str] = None,
):
    # 1) por id_pedido_omie
    if id_pedido is not None:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, $2, $3, $4, 'pendente_consulta')
            ON CONFLICT ON CONSTRAINT ux_omie_id
            DO UPDATE SET
              numero = EXCLUDED.numero,
              raw_basico = EXCLUDED.raw_basico,
              codigo_pedido_integracao = COALESCE(EXCLUDED.codigo_pedido_integracao, omie_pedido.codigo_pedido_integracao)
            """,
            numero, id_pedido, codigo_pedido_integracao, json.dumps(body),
        )
        logger.info({"tag": "inbox_upsert_by_id", "id_pedido": id_pedido})
        return

    # 2) por codigo_pedido_integracao
    if codigo_pedido_integracao:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, NULL, $2, $3, 'pendente_consulta')
            ON CONFLICT ON CONSTRAINT ux_omie_codint
            DO UPDATE SET
              numero = EXCLUDED.numero,
              raw_basico = EXCLUDED.raw_basico
            """,
            numero, codigo_pedido_integracao, json.dumps(body),
        )
        logger.info({"tag": "inbox_upsert_by_codint", "codigo": codigo_pedido_integracao})
        return

    # 3) por numero
    if numero is not None:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, NULL, NULL, $2, 'pendente_consulta')
            ON CONFLICT ON CONSTRAINT ux_omie_numero
            DO UPDATE SET
              raw_basico = EXCLUDED.raw_basico
            """,
            numero, json.dumps(body),
        )
        logger.info({"tag": "inbox_upsert_by_numero", "numero": numero})
        return

    # 4) sem nenhuma chave
    await conn.execute(
        """INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
           VALUES (NULL, NULL, NULL, $1, 'sem_chave')""",
        json.dumps(body),
    )
    logger.info({"tag": "inbox_inserted_sem_chave"})

async def update_pedido_detalhe(conn: asyncpg.Connection, detalhe: dict,
                                id_pedido_omie: Optional[int] = None,
                                codigo_pedido_integracao: Optional[str] = None):
    if id_pedido_omie is not None:
        await conn.execute(
            """UPDATE omie_pedido SET raw_detalhe=$1, status='consultado' WHERE id_pedido_omie=$2""",
            json.dumps(detalhe), id_pedido_omie,
        )
    elif codigo_pedido_integracao:
        await conn.execute(
            """UPDATE omie_pedido SET raw_detalhe=$1, status='consultado' WHERE codigo_pedido_integracao=$2""",
            json.dumps(detalhe), codigo_pedido_integracao,
        )
    else:
        raise ValueError("Sem chave para atualizar raw_detalhe")

async def fetch_pendentes(conn: asyncpg.Connection, limit: int = 50):
    return await conn.fetch(
        """SELECT id_pedido_omie, numero, codigo_pedido_integracao
           FROM omie_pedido
          WHERE status='pendente_consulta'
          ORDER BY recebido_em ASC
          LIMIT $1""",
        limit,
    )

async def fetch_keys_by_numero(conn: asyncpg.Connection, numero: str):
    return await conn.fetchrow(
        """SELECT id_pedido_omie, codigo_pedido_integracao
           FROM omie_pedido
          WHERE numero=$1
          ORDER BY recebido_em DESC
          LIMIT 1""",
        numero,
    )

# ------------------------------------------------------------
# Banco: helpers de NF XML
# ------------------------------------------------------------
def _xml_extract_fields(xml_str: str) -> dict:
    try:
        import xml.etree.ElementTree as ET
        ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
        root = ET.fromstring(xml_str.encode("utf-8"))
        xnome = root.find(".//nfe:dest/nfe:xNome", ns)
        xped  = root.find(".//nfe:compra/nfe:xPed", ns)
        return {
            "cliente_nome": (xnome.text.strip() if xnome is not None and xnome.text else None),
            "pedido_marketplace": (xped.text.strip() if xped is not None and xped.text else None),
        }
    except Exception:
        return {"cliente_nome": None, "pedido_marketplace": None}

async def upsert_nf_xml_row(conn: asyncpg.Connection, row: dict) -> None:
    id_nf   = int(row.get("nIdNF"))
    id_ped  = _as_int(row.get("nIdPedido"))
    n_chave = (row.get("nChave") or "").strip() or None
    n_num   = row.get("nNumero")
    dEmi    = row.get("dEmissao") or ""
    hEmi    = row.get("hEmissao") or ""
    xml     = row.get("cXml") or ""

    try:
        dh = dtparse.parse(f"{dEmi} {hEmi}", dayfirst=True)
        if dh.tzinfo is None:
            dh = dh.replace(tzinfo=timezone.utc)
    except Exception:
        dh = None

    parsed = _xml_extract_fields(xml)

    await conn.execute(
        """
        INSERT INTO omie_nf_xml (id_nf, id_pedido_omie, n_chave, numero_nf, dh_emissao, xml, cliente_nome, pedido_marketplace)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (id_nf) DO UPDATE SET
            id_pedido_omie = EXCLUDED.id_pedido_omie,
            n_chave        = EXCLUDED.n_chave,
            numero_nf      = EXCLUDED.numero_nf,
            dh_emissao     = EXCLUDED.dh_emissao,
            xml            = EXCLUDED.xml,
            cliente_nome   = COALESCE(EXCLUDED.cliente_nome, omie_nf_xml.cliente_nome),
            pedido_marketplace = COALESCE(EXCLUDED.pedido_marketplace, omie_nf_xml.pedido_marketplace)
        """,
        id_nf, id_ped, n_chave, n_num, dh, xml, parsed["cliente_nome"], parsed["pedido_marketplace"]
    )

# ------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------
@app.get("/")
async def healthz():
    return {"status": "ok"}

# Webhook da Omie (pedidos / NFe.*)
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str = Query(default="")):
    if OMIE_WEBHOOK_TOKEN and token != OMIE_WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

    try:
        body = await request.json()
    except Exception:
        body = {}

    # Log básico para eventos NFe (XML importa via /admin/sync-xml)
    if (isinstance(body, dict)
        and str(body.get("topic") or body.get("tópico") or "").lower().startswith("nfe.")):
        logger.info({"tag": "omie_nf_event", "msg": "evento de NF recebido", "body_has_xml_link": bool(deep_find_first(body, {"nfe_xml"}))})

    numero_any = deep_find_first(body, WANTED_NUM_KEYS)
    id_any     = deep_find_first(body, WANTED_ID_KEYS)
    integ_any  = deep_find_first(body, WANTED_INTEGR_KEYS)

    numero_str = str(numero_any) if numero_any is not None else None
    id_pedido  = _as_int(id_any)

    codigo_pedido_integracao = None
    if isinstance(integ_any, str) and integ_any:
        codigo_pedido_integracao = integ_any
    elif numero_str and looks_like_integr(numero_str):
        codigo_pedido_integracao = numero_str

    logger.info({"tag": "omie_webhook_received", "numero_pedido": numero_str, "id_pedido": id_pedido,
                "codigo_pedido_integracao": codigo_pedido_integracao})

    pool = await get_pool()
    async with pool.acquire() as conn:
        await _safe_bootstrap(conn)
        try:
            await insert_inbox_safe(conn, numero_str, id_pedido, body, codigo_pedido_integracao)
        except Exception:
            logger.error({"tag": "inbox_error", "err": traceback.format_exc(),
                          "numero": numero_str, "id_pedido": id_pedido,
                          "codigo_pedido_integracao": codigo_pedido_integracao})
            # aceita para processamento posterior, não falha o webhook
            raise HTTPException(status_code=202, detail="aceito para processamento")

    return {"ok": True}

# Reprocesso de pedidos pendentes
async def _processar_pendentes(conn: asyncpg.Connection, limit: int):
    ok = err = 0
    ids_ok: list[Any] = []
    rows = await fetch_pendentes(conn, limit=limit)
    for row in rows:
        idp    = row["id_pedido_omie"]
        integ  = row["codigo_pedido_integracao"]
        numero = row["numero"]
        try:
            logger.info({"tag": "reprocess_start", "id_pedido": idp, "integ": integ, "numero": numero})
            if idp is not None:
                detalhe = await omie_consultar_pedido(codigo_pedido=int(idp))
                await update_pedido_detalhe(conn, detalhe, id_pedido_omie=int(idp))
            elif integ:
                detalhe = await omie_consultar_pedido(codigo_pedido_integracao=str(integ))
                await update_pedido_detalhe(conn, detalhe, codigo_pedido_integracao=str(integ))
            elif numero is not None:
                keys = await fetch_keys_by_numero(conn, str(numero))
                if keys and (keys["id_pedido_omie"] or keys["codigo_pedido_integracao"]):
                    detalhe = await omie_consultar_pedido(
                        codigo_pedido=keys["id_pedido_omie"] if keys["id_pedido_omie"] else None,
                        codigo_pedido_integracao=keys["codigo_pedido_integracao"] if keys["codigo_pedido_integracao"] else None,
                    )
                    await update_pedido_detalhe(
                        conn, detalhe,
                        id_pedido_omie=keys["id_pedido_omie"],
                        codigo_pedido_integracao=keys["codigo_pedido_integracao"]
                    )
                else:
                    logger.warning({"tag": "reprocess_skip_sem_chave", "numero": numero})
                    continue
            else:
                logger.warning({"tag": "reprocess_skip_sem_chave", "numero": numero})
                continue

            logger.info({"tag": "reprocess_done", "id": idp, "integ": integ, "numero": numero})
            ok += 1
            ids_ok.append(idp if idp is not None else integ or numero)
        except Exception:
            err += 1
            logger.error({"tag": "reprocess_error", "id_pedido": idp, "integ": integ, "numero": numero, "err": traceback.format_exc()})
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
async def reprocessar_pedido(secret: str, numero: Optional[str] = None, codigo_pedido_integracao: Optional[str] = None):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    if numero is None and not codigo_pedido_integracao:
        raise HTTPException(status_code=400, detail="Informe 'numero' ou 'codigo_pedido_integracao'")

    pool = await get_pool()
    async with pool.acquire() as conn:
        await _safe_bootstrap(conn)
        ref = {}
        if numero is not None:
            keys = await fetch_keys_by_numero(conn, str(numero))
            if not keys:
                raise HTTPException(status_code=404, detail="Pedido não encontrado")
            if keys["id_pedido_omie"] is not None:
                detalhe = await omie_consultar_pedido(codigo_pedido=int(keys["id_pedido_omie"]))
                await update_pedido_detalhe(conn, detalhe, id_pedido_omie=int(keys["id_pedido_omie"]))
                ref = {"id_pedido_omie": int(keys["id_pedido_omie"])}
            elif keys["codigo_pedido_integracao"]:
                detalhe = await omie_consultar_pedido(codigo_pedido_integracao=str(keys["codigo_pedido_integracao"]))
                await update_pedido_detalhe(conn, detalhe, codigo_pedido_integracao=str(keys["codigo_pedido_integracao"]))
                ref = {"codigo_pedido_integracao": keys["codigo_pedido_integracao"]}
            else:
                raise HTTPException(status_code=404, detail="Pedido sem chave de consulta")
        else:
            detalhe = await omie_consultar_pedido(codigo_pedido_integracao=str(codigo_pedido_integracao))
            await update_pedido_detalhe(conn, detalhe, codigo_pedido_integracao=str(codigo_pedido_integracao))
            ref = {"codigo_pedido_integracao": codigo_pedido_integracao}

    return {"ok": True, "ref": ref}

@app.post("/admin/run-jobs")
async def run_jobs(secret: str, limit: int = 50):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        ok, err, ids_ok, found = await _processar_pendentes(conn, limit)
    return {"ok": True, "pendentes_encontrados": found, "processados_ok": ok, "processados_erro": err, "ids_ok": ids_ok}

# Backfill pedidos
@app.post("/admin/backfill")
async def backfill_pedidos(
    secret: str,
    inicio: str,
    fim: Optional[str] = None,
    por_pagina: int = 200,
    max_paginas: int = 100,
):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    d_ini = dtparse.parse(inicio)
    d_fim = dtparse.parse(fim) if fim else datetime.now(tz=timezone.utc)

    pool = await get_pool()
    importados = 0
    paginas = 0
    async with pool.acquire() as conn:
        page = 1
        while page <= max_paginas:
            data = await _omie_pesquisar_pedidos(d_ini, d_fim, page, por_pagina)
            lista = (data or {}).get("lista_pedidos", []) or (data or {}).get("pedido_venda_produto", [])
            if not lista:
                break
            for ped in lista:
                numero_any = deep_find_first(ped, WANTED_NUM_KEYS)
                id_any     = deep_find_first(ped, WANTED_ID_KEYS)
                integ_any  = deep_find_first(ped, WANTED_INTEGR_KEYS)

                numero_str = str(numero_any) if numero_any is not None else None
                integr     = None
                if isinstance(integ_any, str) and integ_any:
                    integr = integ_any
                elif isinstance(numero_any, str) and looks_like_integr(numero_any):
                    integr = numero_any

                await insert_inbox_safe(conn, numero_str, _as_int(id_any), ped, integr)
                importados += 1
            paginas += 1
            page += 1
    return {"ok": True, "importados": importados, "paginas": paginas, "msg": "Backfill concluído com sucesso."}

# Sync NF-e XML
@app.post("/admin/sync-xml")
async def sync_xml(
    secret: str,
    inicio: Optional[str] = None,
    fim: Optional[str] = None,
    por_pagina: int = 200,
    max_paginas: int = 200,
):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    d_ini = dtparse.parse(inicio) if inicio else None
    d_fim = dtparse.parse(fim) if fim else None

    pool = await get_pool()
    salvos = 0
    paginas = 0
    async with pool.acquire() as conn:
        page = 1
        while page <= max_paginas:
            data = await _omie_listar_documentos_xml(page, por_pagina, d_ini, d_fim)
            lista = (data or {}).get("lista", []) or (data or {}).get("documentos", [])
            if not lista:
                break
            for item in lista:
                try:
                    await upsert_nf_xml_row(conn, item)
                    salvos += 1
                except Exception:
                    logger.error({"tag":"sync_xml_err", "err": traceback.format_exc(), "item": item.get("nIdNF")})
            paginas += 1
            tot = (data or {}).get("nTotPaginas") or (data or {}).get("total_de_paginas")
            if isinstance(tot, int) and page >= tot:
                break
            page += 1
    return {"ok": True, "paginas": paginas, "xml_salvos_ou_atualizados": salvos}

# Prévia expedição
@app.get("/admin/expedicao-preview")
async def expedicao_preview(secret: str, limit: int = 50):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT *
              FROM vw_expedicao
             ORDER BY dh_emissao DESC NULLS LAST
             LIMIT $1
            """,
            limit,
        )
        return {"ok": True, "rows": [dict(r) for r in rows]}
