# -*- coding: utf-8 -*-
"""
Sincroniza produtos OMIE -> Postgres (schema omie).
Upsert em omie.omie_produtos; checkpoint em omie.sync_control (entidade='produtos').
"""
import json
import datetime as dt
from typing import Any, Dict

from sync_common import (
    parse_args, setup_logger, get_conn,
    get_checkpoint, set_checkpoint, paginar, APP_KEY, APP_SECRET
)

ENTIDADE = "produtos"
URL  = "https://app.omie.com.br/api/v1/geral/produtos/"
CALL = "ListarProdutos"

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
        return None

def map_produto(pr: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "codigo_produto":  pr.get("codigo_produto") or pr.get("id_produto"),
        "codigo_sku":      pr.get("codigo"),
        "descricao":       pr.get("descricao") or pr.get("descricao_produto"),
        "unidade":         pr.get("unidade"),
        "ncm":             pr.get("ncm"),
        "ean":             pr.get("ean"),
        "valor_unitario":  pr.get("valor_unitario") or pr.get("preco_unitario"),
        "marca":           pr.get("marca"),
        "produto_variacao": bool(pr.get("variacao")) if "variacao" in pr else None,
        "raw_json":        json.dumps(pr, ensure_ascii=False),
        "created_at":      _parse_ts(pr.get("created_at") or pr.get("dInc")),
        "updated_at":      _parse_ts(pr.get("updated_at") or pr.get("dAlt")),
    }

UPSERT_SQL = """
INSERT INTO omie.omie_produtos
(codigo_produto, codigo_sku, descricao, unidade, ncm, ean, valor_unitario,
 marca, produto_variacao, raw_json, created_at, updated_at)
VALUES
(%(codigo_produto)s, %(codigo_sku)s, %(descricao)s, %(unidade)s, %(ncm)s, %(ean)s, %(valor_unitario)s,
 %(marca)s, %(produto_variacao)s, %(raw_json)s::jsonb, %(created_at)s, %(updated_at)s)
ON CONFLICT (codigo_produto) DO UPDATE SET
  codigo_sku      = EXCLUDED.codigo_sku,
  descricao       = EXCLUDED.descricao,
  unidade         = EXCLUDED.unidade,
  ncm             = EXCLUDED.ncm,
  ean             = EXCLUDED.ean,
  valor_unitario  = EXCLUDED.valor_unitario,
  marca           = EXCLUDED.marca,
  produto_variacao= EXCLUDED.produto_variacao,
  raw_json        = EXCLUDED.raw_json,
  created_at      = COALESCE(omie.omie_produtos.created_at, EXCLUDED.created_at),
  updated_at      = EXCLUDED.updated_at;
"""

def main():
    args   = parse_args()
    logger = setup_logger(f"sync_{ENTIDADE}", debug=args.debug)

    with get_conn() as conn, conn.cursor() as cur:
        base_dt = args.desde
        desde   = get_checkpoint(ENTIDADE, base_dt)
        logger.info("== INÍCIO SYNC_PRODUTOS ==  checkpoint atual: %s", desde)
        ultimo: dt.datetime = desde

        for pagina, lista in paginar(URL, CALL, APP_KEY, APP_SECRET, desde_dt=desde):
            for pr in lista:
                row = map_produto(pr)
                cur.execute(UPSERT_SQL, row)
                ts = row.get("updated_at") or row.get("created_at")
                if ts and ts > ultimo:
                    ultimo = ts
            conn.commit()
            logger.debug("Página %s aplicada. Parcial checkpoint=%s", pagina, ultimo)

        set_checkpoint(ENTIDADE, ultimo)
        logger.info("== FIM SYNC_PRODUTOS ==  novo checkpoint: %s", ultimo)

if __name__ == "__main__":
    main()
