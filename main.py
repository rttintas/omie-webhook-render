# main.py
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
# Config
# ------------------------------------------------------------
app = FastAPI()
logger = logging.getLogger("uvicorn.error")

DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "troque-isto")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_WEBHOOK_TOKEN = os.getenv("OMIE_WEBHOOK_TOKEN", "")  # valida ?token=...
OMIE_TIMEOUT = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

# APIs Omie
OMIE_PEDIDO_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_XML_URL    = "https://app.omie.com.br/api/v1/contador/xml/"

_pool: Optional[asyncpg.Pool] = None


# ------------------------------------------------------------
# Conexão com o banco / Migrações leves
# ------------------------------------------------------------
async def get_pool() -> asyncpg.Pool:
    global _pool
    if not _pool:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL não configurada")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        logger.info({"tag": "startup", "msg": "Pool OK."})
    return _pool


async def _safe_bootstrap(conn: asyncpg.Connection):
    """
    Migrações idempotentes e seguras. Criam estruturas se não existirem.
    """
    # Tabela de pedidos (inbox + detalhes)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS omie_pedido (
          pk bigserial PRIMARY KEY,
          recebido_em timestamptz NOT NULL DEFAULT now(),
          status text NOT NULL DEFAULT 'pendente_consulta' CHECK (status IN ('pendente_consulta','consultado','sem_id')),
          numero text,
          id_pedido_omie bigint,
          codigo_pedido_integracao text,
          raw_basico  jsonb,
          raw_detalhe jsonb
        );
        """
    )
    
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_omie_pedido_status     ON omie_pedido (status);
        """
    )
    
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_omie_pedido_recebido   ON omie_pedido (recebido_em DESC);
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_omie_pedido_integr     ON omie_pedido (codigo_pedido_integracao);
        """
    )

    await conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_numero ON omie_pedido (numero) WHERE numero IS NOT NULL;
        """
    )
    
    await conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_id ON omie_pedido (id_pedido_omie) WHERE id_pedido_omie IS NOT NULL;
        """
    )

    await conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_codint ON omie_pedido (codigo_pedido_integracao) WHERE codigo_pedido_integracao IS NOT NULL;
        """
    )

    # Cache de NF (XML)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS omie_nf_xml (
          id bigserial PRIMARY KEY,
          recebido_em timestamptz NOT NULL DEFAULT now(),
          id_nf bigint UNIQUE,                 -- nIdNF da Omie
          id_pedido_omie bigint,               -- nIdPedido (liga no pedido)
          n_chave text UNIQUE,                 -- nChave
          numero_nf text,                      -- nNumero (usei TEXT porque pode vir com zero à esquerda)
          dh_emissao timestamptz,              -- dEmissao + hEmissao
          xml text,                            -- cXml (inteiro)
          cliente_nome text,                   -- xNome (dest)
          pedido_marketplace text              -- xPed
        );
        """
    )
    
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_nf_pedido ON omie_nf_xml (id_pedido_omie);
        """
    )

    # View para a automação de expedição (join já “pronto”)
    await conn.execute(
        """
        CREATE OR REPLACE VIEW vw_expedicao AS
        SELECT
          nf.recebido_em         AS recebido_xml_em,
          nf.numero_nf,
          nf.n_chave,
          nf.dh_emissao,
          nf.cliente_nome,
          nf.pedido_marketplace,                   -- xPed
          p.numero                AS pedido_numero,
          p.id_pedido_omie,
          COALESCE(
            p.raw_detalhe->'pedido_venda_produto'->'cabecalho'->>'origem_pedido',
            p.raw_basico->'event'->>'origem_pedido'
          )                     AS origem_pedido,
          -- descrição do pedido (do PEDIDO, não da NF)
          p.raw_detalhe->'pedido_venda_produto'->'observacoes'->>'obs_venda' AS descricao_pedido,
          -- quantidade total (soma dos itens)
          (
            SELECT COALESCE(SUM( (it->'produto'->>'quantidade')::numeric ),0)
            FROM jsonb_array_elements(p.raw_detalhe->'pedido_venda_produto'->'det') AS it
          )::numeric(12,3) AS qtd_total
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
# Utilitários (extração de chaves do payload)
# ------------------------------------------------------------
WANTED_ID_KEYS = {
    "idPedido", "id_pedido", "idPedidoOmie", "idPedidoVenda", "id_pedido_omie", "codigo_pedido"
}
WANTED_NUM_KEYS = {"numeroPedido", "numero_pedido", "numero", "codigo_pedido_integracao"}
WANTED_INTEGR_KEYS = {"codigo_pedido_integracao", "codigoPedidoIntegracao"}


def _as_int(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, int): return v
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try: return int(s)
            except Exception: return None
    return None


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


def looks_like_integr(code: Optional[str]) -> bool:
    return bool(code) and not str(code).isdigit()


# ------------------------------------------------------------
# Omie: ConsultarPedido (blindado — nunca manda idPedido)
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
# Omie: Backfill de pedidos por intervalo de data
# (usa 'PesquisarPedidos' – datas em DD/MM/AAAA – paginação)
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
            "ordem_descrescente": "N"
        }],
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(OMIE_TIMEOUT)) as client:
        r = await client.post(OMIE_PEDIDO_URL, json=payload)
        r.raise_for_status()
        return r.json()


# ------------------------------------------------------------
# Omie: ListarDocumentos (NF-e XML) — contador/xml
# ------------------------------------------------------------
async def _omie_listar_documentos_xml(pagina: int, por_pagina: int, d_ini: Optional[datetime], d_fim: Optional[datetime]) -> dict:
    # datas no formato DD/MM/AAAA (pode ser vazio)
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
    if id_pedido is None and not codigo_pedido_integracao:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, NULL, NULL, $2, 'sem_id')
            """,
            numero, json.dumps(body),
        )
        logger.info({"tag": "inbox_inserted_sem_id", "numero": str(numero)})
        return

    try:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, $2, $3, $4, 'pendente_consulta')
            ON CONFLICT ON CONSTRAINT ux_omie_id DO UPDATE SET
               numero     = EXCLUDED.numero,
               raw_basico = EXCLUDED.raw_basico,
               codigo_pedido_integracao = COALESCE(EXCLUDED.codigo_pedido_integracao, omie_pedido.codigo_pedido_integracao)
            """,
            numero, id_pedido, codigo_pedido_integracao, json.dumps(body),
        )
    except Exception:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, $2, $3, $4, 'pendente_consulta')
            """,
            numero, id_pedido, codigo_pedido_integracao, json.dumps(body),
        )


async def update_pedido_detalhe(
    conn: asyncpg.Connection, 
    detalhe: dict,
    id_pedido_omie: Optional[int] = None,
    codigo_pedido_integracao: Optional[str] = None
):
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
    """
    Extrai xNome (cliente) e xPed (pedido marketplace) do XML.
    """
    try:
        import xml.etree.ElementTree as ET
        ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
        root = ET.fromstring(xml_str.encode("utf-8"))
        # nfeProc > NFe > infNFe
        xnome = root.find(".//nfe:dest/nfe:xNome", ns)
        xped  = root.find(".//nfe:compra/nfe:xPed", ns)
        return {
            "cliente_nome": (xnome.text.strip() if xnome is not None and xnome.text else None),
            "pedido_marketplace": (xped.text.strip() if xped is not None and xped.text else None),
        }
    except Exception:
        return {"cliente_nome": None, "pedido_marketplace": None}

async def upsert_nf_xml_row(conn: asyncpg.Connection, row: dict) -> None:
    """
    row: item de ListarDocumentos
      chaves: nIdNF, nIdPedido, nChave, nNumero, dEmissao, hEmissao, cXml
    """
    id_nf   = int(row.get("nIdNF"))
    id_ped  = _as_int(row.get("nIdPedido"))
    n_chave = row.get("nChave")
    n_num   = row.get("nNumero") # Alterado para TEXT na tabela, então não precisa ser int
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
# Endpoints básicos
# ------------------------------------------------------------
@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/healthz")
async def healthz():
    return {"status": "healthy"}


# ------------------------------------------------------------
# Webhook da Omie (pedidos / NFe.NotaAutorizada)
# ------------------------------------------------------------
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str = Query(default="")):
    if OMIE_WEBHOOK_TOKEN and token != OMIE_WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")

    body = await request.json()

    # Se for evento de NF autorizada
    if isinstance(body, dict) and str(body.get("tópico", "") or "").lower().startswith("nfe."):
        logger.info({"tag": "omie_nf_event", "msg": "evento de NF recebido"})
        # Aqui, vamos chamar a função para inserir a nota fiscal na tabela dedicada
        await upsert_nf_xml_row(conn, body.get("evento", {}))
    
    # Lógica para eventos de pedido (segue o fluxo original para compatibilidade)
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
        {"tag": "omie_webhook_received", "numero_pedido": str(numero), "id_pedido": id_pedido,
         "codigo_pedido_integracao": codigo_pedido_integracao}
    )

    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await _safe_bootstrap(conn)
            await insert_inbox_safe(conn, numero, id_pedido, body, codigo_pedido_integracao)
        except Exception:
            logger.error(
                {"tag": "inbox_error", "err": traceback.format_exc(),
                 "numero": str(numero), "id_pedido": id_pedido,
                 "codigo_pedido_integracao": codigo_pedido_integracao}
            )
            raise HTTPException(status_code=202, detail="aceito para processamento")

    return {"ok": True}


# ... (restante do código, sem alterações no final) ...
# Você pode remover as linhas duplicadas do backfill, sync-xml e expedicao-preview
# A versão correta já estava no código que você me enviou.
# O problema estava apenas na fusão do topo do arquivo.