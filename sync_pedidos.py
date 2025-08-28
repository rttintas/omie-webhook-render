# -*- coding: utf-8 -*-
"""
Sincroniza pedidos OMIE -> Postgres (schema omie).
Upsert em omie.omie_pedidos e omie.omie_pedido_itens; checkpoint em omie.sync_control (entidade='pedidos').
"""
import json
import datetime as dt
from typing import Any, Dict, Iterable

from sync_common import (
    parse_args, setup_logger, get_conn,
    get_checkpoint, set_checkpoint, paginar, APP_KEY, APP_SECRET
)

ENTIDADE = "pedidos"
URL  = "https://app.omie.com.br/api/v1/produtos/pedido/"
CALL = "ListarPedidos"  # nome da call usado na paginação do seu sync_common

def _parse_date(s: Any):
    if not s:
        return None
    if isinstance(s, dt.date) and not isinstance(s, dt.datetime):
        return s
    ss = str(s).strip()
    try:
        if "-" in ss:
            return dt.date.fromisoformat(ss[:10])
        if "/" in ss:
            d, m, y = ss.split("/")
            return dt.date(int(y), int(m), int(d))
    except Exception:
        return None
    return None

def _parse_ts(s: Any):
    if not s:
        return None
    if isinstance(s, dt.datetime):
        return s
    if isinstance(s, dt.date):
        return dt.datetime.combine(s, dt.time.min)
    ss = str(s).strip().replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(ss.replace("T", " "))
    except Exception:
        d = _parse_date(ss)
        return dt.datetime.combine(d, dt.time.min) if d else None

def map_pedido(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "codigo_pedido":      p.get("codigo_pedido") or p.get("id_pedido"),
        "numero_pedido":      p.get("numero_pedido"),
        "codigo_cliente_omie":p.get("codigo_cliente_omie") or p.get("codigo_cliente"),
        "etapa":              p.get("etapa") or p.get("status"),
        "data_prevista":      _parse_date(p.get("data_previsao") or p.get("data_previsao_faturamento")),
        "quantidade_itens":   p.get("quantidade_itens") or (len(p.get("itens", [])) if isinstance(p.get("itens"), list) else None),
        "obs_venda":          p.get("observacao") or p.get("obs_venda"),
        "created_at":         _parse_ts(p.get("created_at") or p.get("data_inclusao") or p.get("dInc")),
        "updated_at":         _parse_ts(p.get("updated_at") or p.get("data_alteracao") or p.get("dAlt")),
        "raw_json":           json.dumps(p, ensure_ascii=False),
    }

def iter_itens(p: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    itens = p.get("itens") or p.get("pedido_itens") or []
    for i, it in enumerate(itens, start=1):
        yield {
            "codigo_pedido":  p.get("codigo_pedido") or p.get("id_pedido"),
            "sequencia":      it.get("sequencia") or i,
            "codigo_produto": it.get("codigo_produto") or it.get("id_produto"),
            "descricao":      it.get("descricao") or it.get("produto"),
            "cfop":           it.get("cfop"),
            "quantidade":     it.get("quantidade"),
            "unidade":        it.get("unidade"),
            "valor_unitario": it.get("valor_unitario"),
            "valor_total":    it.get("valor_total"),
        }

UPSERT_PED_SQL = """
INSERT INTO omie.omie_pedidos
(codigo_pedido, numero_pedido, codigo_cliente_omie, etapa, data_prevista,
 quantidade_itens, obs_venda, raw_json, created_at, updated_at)
VALUES
(%(codigo_pedido)s, %(numero_pedido)s, %(codigo_cliente_omie)s, %(etapa)s, %(data_prevista)s,
 %(quantidade_itens)s, %(obs_venda)s, %(raw_json)s::jsonb, %(created_at)s, %(updated_at)s)
ON CONFLICT (codigo_pedido) DO UPDATE SET
  numero_pedido      = EXCLUDED.numero_pedido,
  codigo_cliente_omie= EXCLUDED.codigo_cliente_omie,
  etapa              = EXCLUDED.etapa,
  data_prevista      = EXCLUDED.data_prevista,
  quantidade_itens   = EXCLUDED.quantidade_itens,
  obs_venda          = EXCLUDED.obs_venda,
  raw_json           = EXCLUDED.raw_json,
  created_at         = COALESCE(omie.omie_pedidos.created_at, EXCLUDED.created_at),
  updated_at         = EXCLUDED.updated_at;
"""

UPSERT_ITEM_SQL = """
INSERT INTO omie.omie_pedido_itens
(codigo_pedido, sequencia, codigo_produto, descricao, cfop, quantidade, unidade, valor_unitario, valor_total)
VALUES
(%(codigo_pedido)s, %(sequencia)s, %(codigo_produto)s, %(descricao)s, %(cfop)s,
 %(quantidade)s, %(unidade)s, %(valor_unitario)s, %(valor_total)s)
ON CONFLICT (codigo_pedido, sequencia) DO UPDATE SET
  codigo_produto = EXCLUDED.codigo_produto,
  descricao      = EXCLUDED.descricao,
  cfop           = EXCLUDED.cfop,
  quantidade     = EXCLUDED.quantidade,
  unidade        = EXCLUDED.unidade,
  valor_unitario = EXCLUDED.valor_unitario,
  valor_total    = EXCLUDED.valor_total;
"""

def main():
    args   = parse_args()
    logger = setup_logger(f"sync_{ENTIDADE}", debug=args.debug)

    with get_conn() as conn, conn.cursor() as cur:
        base_dt = args.desde
        desde   = get_checkpoint(ENTIDADE, base_dt)
        logger.info("== INÍCIO SYNC_PEDIDOS ==  checkpoint atual: %s", desde)
        ultimo: dt.datetime = desde

        for pagina, lista in paginar(URL, CALL, APP_KEY, APP_SECRET, desde_dt=desde):
            for p in lista:
                ped = map_pedido(p)
                cur.execute(UPSERT_PED_SQL, ped)
                for it in iter_itens(p):
                    cur.execute(UPSERT_ITEM_SQL, it)
                ts = ped.get("updated_at") or ped.get("created_at")
                if ts and ts > ultimo:
                    ultimo = ts
            conn.commit()
            logger.debug("Página %s aplicada. Parcial checkpoint=%s", pagina, ultimo)

        set_checkpoint(ENTIDADE, ultimo)
        logger.info("== FIM SYNC_PEDIDOS ==  novo checkpoint: %s", ultimo)

if __name__ == "__main__":
    main()
