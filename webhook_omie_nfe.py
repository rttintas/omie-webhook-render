# webhook_omie_nfe.py
import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse

APP_NAME = "omie-webhook"
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
PG_DSN = os.getenv("PG_DSN", "")

app = FastAPI(title=APP_NAME, version="1.0.0")

_pool: Optional[asyncpg.Pool] = None

DDL_SQL = """
-- 1) Log bruto dos webhooks
CREATE TABLE IF NOT EXISTS omie_webhook_events (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_id TEXT UNIQUE,
  payload JSONB NOT NULL,
  processed BOOLEAN NOT NULL DEFAULT false,
  processed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_omie_webhook_events_proc ON omie_webhook_events(processed, event_type);
CREATE INDEX IF NOT EXISTS idx_omie_webhook_events_ts ON omie_webhook_events(event_ts);

-- 2) NF-e (resumo)
CREATE TABLE IF NOT EXISTS omie_nfe (
  chave_nfe TEXT PRIMARY KEY,
  numero TEXT,
  serie TEXT,
  emitida_em TIMESTAMPTZ,
  cnpj_emitente TEXT,
  cnpj_destinatario TEXT,
  valor_total NUMERIC(15,2),
  status TEXT,
  xml TEXT,
  pdf_url TEXT,
  last_event_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS omie_nfe_itens (
  chave_nfe TEXT REFERENCES omie_nfe(chave_nfe) ON DELETE CASCADE,
  linha INT,
  codigo_produto TEXT,
  descricao TEXT,
  quantidade NUMERIC(15,3),
  unidade TEXT,
  vl_unitario NUMERIC(15,6),
  PRIMARY KEY (chave_nfe, linha)
);

-- 3) Pedidos de venda (resumo)
CREATE TABLE IF NOT EXISTS omie_pedido (
  id_pedido TEXT PRIMARY KEY,
  numero_pedido TEXT,
  status TEXT,
  cliente_codigo TEXT,
  cliente_nome TEXT,
  emissao TIMESTAMPTZ,
  valor_total NUMERIC(15,2),
  last_event_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS omie_pedido_itens (
  id_pedido TEXT REFERENCES omie_pedido(id_pedido) ON DELETE CASCADE,
  linha INT,
  codigo_produto TEXT,
  descricao TEXT,
  quantidade NUMERIC(15,3),
  unidade TEXT,
  vl_unitario NUMERIC(15,6),
  PRIMARY KEY (id_pedido, linha)
);
"""

# ---------- utils ----------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _json_dumps_canonical(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def _hash_event(payload: Dict[str, Any]) -> str:
    """
    Gera um hash determinístico do payload para idempotência.
    Se o Omie enviar algum ID próprio (ex.: id, guid), ele é incorporado automaticamente.
    """
    material = _json_dumps_canonical(payload)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()

def _get(payload: Dict[str, Any], *keys, default=None):
    """
    Busca tolerante com múltiplas chaves possíveis.
    """
    for k in keys:
        if k in payload and payload[k] not in (None, ""):
            return payload[k]
    return default

def _to_ts(val: Any) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, (int, float)):
        # epoch seconds
        return datetime.fromtimestamp(float(val), tz=timezone.utc)
    if isinstance(val, str):
        try:
            # tenta ISO
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            # tenta yyyy-mm-dd
            try:
                d = datetime.strptime(val, "%Y-%m-%d")
                return d.replace(tzinfo=timezone.utc)
            except Exception:
                return None
    return None

# ---------- processamento de eventos ----------

NFE_EVENT_STATUS = {
    "NFe.NotaAutorizada": "AUTORIZADA",
    "NFe.NotaCancelada": "CANCELADA",
    "NFe.NotaDevolucaoAutorizada": "DEVOLUCAO_AUTORIZADA",
}

PEDIDO_EVENT_STATUS = {
    "VendaProduto.Incluida": "INCLUIDA",
    "VendaProduto.Faturada": "FATURADA",
    "VendaProduto.Cancelada": "CANCELADA",
    "VendaProduto.Devolvida": "DEVOLVIDA",
    "VendaProduto.EtapaAlterada": "ETAPA_ALTERADA",
    "VendaProduto.Alterada": "ALTERADA",
    "VendaProduto.Excluida": "EXCLUIDA",
}

async def _process_event(conn: asyncpg.Connection, event_type: str, payload: Dict[str, Any]) -> None:
    """
    Processa apenas os domínios principais (NF-e e Pedido de Venda) por enquanto.
    Todos os outros ficam no log bruto para evolução futura.
    """
    # ---------- NF-e ----------
    if event_type in NFE_EVENT_STATUS:
        status = NFE_EVENT_STATUS[event_type]
        chave = _get(payload, "chaveNFe", "nChaveNFe", "chave_nfe", "nChaveNF")
        if not chave:
            # alguns webhooks podem aninhar em 'data'/'nota'
            data = _get(payload, "data", "nota", default={}) or {}
            chave = _get(data, "chaveNFe", "nChaveNFe", "chave_nfe", "nChaveNF")
        numero = _get(payload, "cNumNFe", "numero", "nNF")
        serie = _get(payload, "serie", "cSerie", "nSerie")
        emissao = _to_ts(_get(payload, "dEmissao", "dDataEmisNFe", "emissao"))
        cnpj_emit = _get(payload, "cnpjEmitente", "emitenteCNPJ", "CNPJEmit")
        cnpj_dest = _get(payload, "cnpjDestinatario", "destinatarioCNPJ", "CNPJDest")
        vtotal = _get(payload, "valorTotal", "vNF", "total")

        if not chave:
            # sem chave não dá pra indexar na tabela de NF; apenas log já está salvo
            return

        await conn.execute(
            """
            INSERT INTO omie_nfe (chave_nfe, numero, serie, emitida_em, cnpj_emitente, cnpj_destinatario, valor_total, status, last_event_at)
            VALUES ($1,$2,$3,$4,$5,$6,CAST(NULLIF($7,'') AS NUMERIC),$8,$9)
            ON CONFLICT (chave_nfe) DO UPDATE
              SET numero = COALESCE(EXCLUDED.numero, omie_nfe.numero),
                  serie = COALESCE(EXCLUDED.serie, omie_nfe.serie),
                  emitida_em = COALESCE(EXCLUDED.emitida_em, omie_nfe.emitida_em),
                  cnpj_emitente = COALESCE(EXCLUDED.cnpj_emitente, omie_nfe.cnpj_emitente),
                  cnpj_destinatario = COALESCE(EXCLUDED.cnpj_destinatario, omie_nfe.cnpj_destinatario),
                  valor_total = COALESCE(EXCLUDED.valor_total, omie_nfe.valor_total),
                  status = EXCLUDED.status,
                  last_event_at = EXCLUDED.last_event_at
            """,
            chave, str(numero) if numero else None, str(serie) if serie else None,
            emissao, cnpj_emit, cnpj_dest, str(vtotal) if vtotal is not None else None,
            status, _now_utc()
        )
        return

    # ---------- Pedido de venda ----------
    if event_type in PEDIDO_EVENT_STATUS:
        status = PEDIDO_EVENT_STATUS[event_type]
        id_pedido = _get(payload, "idPedido", "id_pedido", "cCodPedido", "codigo_pedido", "IdPedido")
        if not id_pedido:
            data = _get(payload, "data", "pedido", default={}) or {}
            id_pedido = _get(data, "idPedido", "id_pedido", "cCodPedido", "codigo_pedido", "IdPedido")

        numero = _get(payload, "numero_pedido", "numeroPedido", "numero", "cNumPedido")
        cliente_codigo = _get(payload, "cliente_codigo", "codCliente", "codigoCliente", "cCodCli")
        cliente_nome = _get(payload, "cliente_nome", "nomeCliente", "razaoSocial")
        emissao = _to_ts(_get(payload, "emissao", "dEmissao", "dataEmissao"))
        vtotal = _get(payload, "valor_total", "valorTotal", "vTotal")

        if not id_pedido:
            return

        await conn.execute(
            """
            INSERT INTO omie_pedido (id_pedido, numero_pedido, status, cliente_codigo, cliente_nome, emissao, valor_total, last_event_at)
            VALUES ($1,$2,$3,$4,$5,$6,CAST(NULLIF($7,'') AS NUMERIC),$8)
            ON CONFLICT (id_pedido) DO UPDATE
              SET numero_pedido = COALESCE(EXCLUDED.numero_pedido, omie_pedido.numero_pedido),
                  status = EXCLUDED.status,
                  cliente_codigo = COALESCE(EXCLUDED.cliente_codigo, omie_pedido.cliente_codigo),
                  cliente_nome = COALESCE(EXCLUDED.cliente_nome, omie_pedido.cliente_nome),
                  emissao = COALESCE(EXCLUDED.emissao, omie_pedido.emissao),
                  valor_total = COALESCE(EXCLUDED.valor_total, omie_pedido.valor_total),
                  last_event_at = EXCLUDED.last_event_at
            """,
            str(id_pedido), str(numero) if numero else None, status,
            cliente_codigo, cliente_nome, emissao,
            str(vtotal) if vtotal is not None else None,
            _now_utc()
        )
        return

    # Outros eventos: apenas gravados no log bruto (já acontece no fluxo principal)
    return

# ---------- FastAPI lifecycle ----------

@app.on_event("startup")
async def startup() -> None:
    global _pool
    if not PG_DSN:
        print("[WARN] PG_DSN não definido. O app sobe, mas não gravará no banco.")
        return
    _pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
    async with _pool.acquire() as conn:
        await conn.execute(DDL_SQL)
    print("[OK] Pool conectado e DDL aplicada.")

@app.on_event("shutdown")
async def shutdown() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

# ---------- rotas ----------

@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True, "app": APP_NAME, "db": bool(_pool)}

def _extract_event_type(body: Dict[str, Any]) -> str:
    return (
        body.get("evento")
        or body.get("event")
        or body.get("event_type")
        or body.get("tipo")
        or body.get("Tipo")
        or "desconhecido"
    )

@app.post("/omie/webhook")
async def omie_webhook(
    request: Request,
    token: str = Query(None, description="Token simples de autenticação")
):
    if WEBHOOK_TOKEN and token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")

    try:
        body = await request.json()
    except Exception:
        body = {}

    event_type = _extract_event_type(body)
    event_id = _get(body, "event_id", "id", "guid")
    if not event_id:
        event_id = _hash_event(body)

    event_ts = _to_ts(_get(body, "event_ts", "timestamp", "dataHora", "dhEvento")) or _now_utc()

    # Se não houver pool (PG_DSN ausente ou erro), apenas ecoa.
    if not _pool:
        return JSONResponse({"ok": True, "warning": "sem banco (PG_DSN ausente)", "event_type": event_type})

    async with _pool.acquire() as conn:
        # 1) grava log bruto idempotente
        inserted = True
        try:
            await conn.execute(
                """
                INSERT INTO omie_webhook_events (source, event_type, event_ts, event_id, payload, processed)
                VALUES ($1,$2,$3,$4,$5,false)
                ON CONFLICT (event_id) DO NOTHING
                """,
                "Omie", event_type, event_ts, event_id, json.loads(_json_dumps_canonical(body))
            )
        except asyncpg.UniqueViolationError:
            inserted = False

        # 2) processa se novo (ou reprocessa se quiser)
        try:
            await _process_event(conn, event_type, body)
            # marca processado
            await conn.execute(
                """
                UPDATE omie_webhook_events
                   SET processed = true, processed_at = now()
                 WHERE event_id = $1
                """,
                event_id
            )
        except Exception as e:
            # não derruba o webhook; apenas retorna 200 com aviso (Omie tende a re-tentar)
            return JSONResponse(
                {"ok": True, "event_type": event_type, "stored": inserted, "processed": False, "error": str(e)}
            )

    return JSONResponse({"ok": True, "event_type": event_type, "stored": inserted, "processed": True})
