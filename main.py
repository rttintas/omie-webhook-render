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
# Config
# ------------------------------------------------------------
app = FastAPI()
logger = logging.getLogger("uvicorn.error")

DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
OMIE_WEBHOOK_TOKEN = os.getenv("OMIE_WEBHOOK_TOKEN", "")  # valida ?token=...
OMIE_TIMEOUT = int(os.getenv("OMIE_TIMEOUT_SECONDS", "30"))

# APIs Omie
OMIE_PEDIDO_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_XML_URL = "https://app.omie.com.br/api/v1/contador/xml/"

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
    await conn.execute(
        """
        -- Tabela de pedidos (inbox + detalhes)
        CREATE TABLE IF NOT EXISTS omie_pedido (
          pk bigserial PRIMARY KEY,
          recebido_em timestamptz NOT NULL DEFAULT now(),
          status text NOT NULL DEFAULT 'pendente_consulta'
            CHECK (status IN ('pendente_consulta','consultado','sem_chave')),
          numero text,                         -- nº do pedido (Omie)
          id_pedido_omie bigint,              -- id interno Omie
          codigo_pedido_integracao text,      -- ex.: OH...
          raw_basico  jsonb,                  -- payload do evento (Omie Connect)
          raw_detalhe jsonb                   -- payload da consulta de detalhes
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

    # Cache de NF-e (XML)
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

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_nf_emissao ON omie_nf_xml (dh_emissao DESC);
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
          ) AS marketplace,
          -- descrição do pedido (do PEDIDO, não da NF)
          p.raw_detalhe->'pedido_venda_produto'->'observacoes'->>'obs_venda' AS descricao_pedido,
          -- quantidade total (soma dos itens)
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
# Utilitários (extração de chaves do payload)
# ------------------------------------------------------------
WANTED_ID_KEYS = {"idPedido", "id_pedido", "idPedidoOmie", "idPedidoVenda", "id_pedido_omie", "codigo_pedido"}
WANTED_NUM_KEYS = {"numeroPedido", "numero_pedido", "numero"}
WANTED_INTEGR_KEYS = {"codigo_pedido_integracao", "codigoPedidoIntegracao"}


def _as_int(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, int): return v
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                return int(s)
            except Exception:
                return None
    return None


def deep_find_first(obj: Any, keys: set[str]) -> Optional[Any]:
    """Procura recursivamente a 1ª ocorrência de qualquer chave do conjunto."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys:
                return v
            found = deep_find_first(v, keys)
            if found is not None:
                return found
        for v in obj.values():
            got = deep_find_first(v, keys)
            if got is not None:
                return got
    elif isinstance(obj, list):
        for it in obj:
            found = deep_find_first(it, keys)
            if found is not None:
                return found
    return None


def looks_like_integr(code: Optional[str]) -> bool:
    """Heurística simples para códigos de integração (ex.: OH15186144)."""
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
            "filtrar