# -*- coding: utf-8 -*-
"""
Sincroniza clientes OMIE -> Postgres (schema omie).
Upsert em omie.omie_clientes e checkpoint em omie.sync_control (entidade='clientes').
"""
import json
import datetime as dt
from typing import Any, Dict

from sync_common import (
    parse_args, setup_logger, get_conn,
    get_checkpoint, set_checkpoint, paginar, APP_KEY, APP_SECRET
)

ENTIDADE = "clientes"
URL  = "https://app.omie.com.br/api/v1/geral/clientes/"
CALL = "ListarClientes"  # compatível com paginação -> clientes_cadastro

def _parse_date(s: Any):
    if not s:
        return None
    if isinstance(s, dt.date) and not isinstance(s, dt.datetime):
        return s
    if isinstance(s, dt.datetime):
        return s.date()
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
        # ISO? 2025-08-25T12:00:00 or 2025-08-25 12:00:00
        ss2 = ss.replace("T", " ")
        return dt.datetime.fromisoformat(ss2)
    except Exception:
        # última tentativa: só data
        d = _parse_date(ss)
        return dt.datetime.combine(d, dt.time.min) if d else None

def map_row(cli: Dict[str, Any]) -> Dict[str, Any]:
    """Extrai campos conhecidos; guarda json completo em raw_json."""
    return {
        "codigo_cliente_omie": cli.get("codigo_cliente_omie") or cli.get("codigo_cliente"),
        "codigo_integracao":   cli.get("codigo_cliente_integracao") or cli.get("codigo_integracao"),
        "cnpj_cpf":            cli.get("cnpj_cpf"),
        "razao_social":        cli.get("razao_social"),
        "nome_fantasia":       cli.get("nome_fantasia"),
        "email":               cli.get("email"),
        "telefone":            cli.get("telefone1") or cli.get("telefone"),
        "cep":                 cli.get("cep"),
        "cidade":              cli.get("cidade"),
        "uf":                  cli.get("estado") or cli.get("uf"),
        "dinc":                _parse_date(cli.get("dInc") or cli.get("data_inclusao")),
        "dalt":                _parse_date(cli.get("dAlt") or cli.get("data_alteracao")),
        # timestamps
        "created_at":          _parse_ts(cli.get("created_at") or cli.get("dInc")),
        "updated_at":          _parse_ts(cli.get("updated_at") or cli.get("dAlt")),
        "raw_json":            json.dumps(cli, ensure_ascii=False),
    }

UPSERT_SQL = """
INSERT INTO omie.omie_clientes
(codigo_cliente_omie, codigo_integracao, cnpj_cpf, razao_social, nome_fantasia,
 email, telefone, cep, cidade, uf, dinc, dalt, raw_json, created_at, updated_at)
VALUES
(%(codigo_cliente_omie)s, %(codigo_integracao)s, %(cnpj_cpf)s, %(razao_social)s, %(nome_fantasia)s,
 %(email)s, %(telefone)s, %(cep)s, %(cidade)s, %(uf)s, %(dinc)s, %(dalt)s, %(raw_json)s::jsonb,
 %(created_at)s, %(updated_at)s)
ON CONFLICT (codigo_cliente_omie) DO UPDATE SET
  codigo_integracao = EXCLUDED.codigo_integracao,
  cnpj_cpf          = EXCLUDED.cnpj_cpf,
  razao_social      = EXCLUDED.razao_social,
  nome_fantasia     = EXCLUDED.nome_fantasia,
  email             = EXCLUDED.email,
  telefone          = EXCLUDED.telefone,
  cep               = EXCLUDED.cep,
  cidade            = EXCLUDED.cidade,
  uf                = EXCLUDED.uf,
  dinc              = EXCLUDED.dinc,
  dalt              = EXCLUDED.dalt,
  raw_json          = EXCLUDED.raw_json,
  created_at        = COALESCE(omie.omie_clientes.created_at, EXCLUDED.created_at),
  updated_at        = EXCLUDED.updated_at;
"""

def main():
    args   = parse_args()
    logger = setup_logger(f"sync_{ENTIDADE}", debug=args.debug)

    with get_conn() as conn, conn.cursor() as cur:
        base_dt = args.desde
        desde   = get_checkpoint(ENTIDADE, base_dt)
        logger.info("== INÍCIO SYNC_CLIENTES ==  checkpoint atual: %s", desde)
        ultimo: dt.datetime = desde

        for pagina, lista in paginar(URL, CALL, APP_KEY, APP_SECRET, desde_dt=desde):
            for cli in lista:
                row = map_row(cli)
                cur.execute(UPSERT_SQL, row)
                ts = row.get("updated_at") or row.get("created_at")
                if ts and ts > ultimo:
                    ultimo = ts
            conn.commit()
            logger.debug("Página %s aplicada. Parcial checkpoint=%s", pagina, ultimo)

        set_checkpoint(ENTIDADE, ultimo)
        logger.info("== FIM SYNC_CLIENTES ==  novo checkpoint: %s", ultimo)

if __name__ == "__main__":
    main()
