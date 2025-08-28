# -*- coding: utf-8 -*-
"""
Sincroniza pedidos e itens do Omie -> Postgres (incremental por data)
- Lista pedidos por período usando ListarPedidos
- Para cada pedido, consulta detalhes (itens) via ConsultarPedido
- UPSERT em omie.omie_pedidos e omie.omie_pedido_itens
- Mantém posição em omie.sync_control (last_success_at)
Requisitos: requests, psycopg2-binary, python-dotenv, tenacity
"""

import os, sys, json, datetime as dt
from typing import Any, Dict, List, Tuple
import requests
from tenacity import retry, wait_exponential, stop_after_attempt
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ------------------------
# Config / Ambiente (.env)
# ------------------------
load_dotenv()

OMIE_APP_KEY    = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

PGHOST     = os.getenv("PGHOST", "localhost")
PGPORT     = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE")
PGUSER     = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")

# nome da coluna de sequência na tabela de itens
ITEM_SEQ_COLUMN = os.getenv("ITEM_SEQ_COLUMN", "sequencia")

# Endpoints/métodos (pelos seus logs)
OMIE_BASE_URL = "https://app.omie.com.br/api/v1"
SERVICE_PEDIDOS = "produtos/pedido"
METHOD_LISTAR_PEDIDOS = "ListarPedidos"
METHOD_CONSULTAR_PEDIDO = "ConsultarPedido"

# Paginacão padrão do Omie
PAGE_SIZE = int(os.getenv("OMIE_PAGE_SIZE", "200"))

# Em qual “fonte” vamos anotar a posição
SYNC_SOURCE = "omie_pedidos"

PG_DSN = f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} user={PGUSER} password={PGPASSWORD}"

# ------------------------
# SQL auxiliares
# ------------------------
SQL_ENSURE_SYNC_TABLE = """
CREATE TABLE IF NOT EXISTS omie.sync_control (
    source TEXT PRIMARY KEY,
    last_success_at timestamptz
);
"""

SQL_GET_LAST_SYNC = """
SELECT COALESCE((
    SELECT last_success_at FROM omie.sync_control WHERE source = %s
), '1970-01-01'::timestamptz);
"""

SQL_SET_LAST_SYNC = """
INSERT INTO omie.sync_control (source, last_success_at)
VALUES (%s, now())
ON CONFLICT (source) DO UPDATE SET last_success_at = EXCLUDED.last_success_at;
"""

# UPSERTs — ajuste os campos se tiverem nomes diferentes no seu schema
SQL_UPSERT_PEDIDOS = """
INSERT INTO omie.omie_pedidos (
  codigo_pedido, numero_pedido, codigo_cliente_omie, etapa, data_previsao,
  obs_venda, raw_json, created_at, updated_at
) VALUES %s
ON CONFLICT (codigo_pedido) DO UPDATE SET
  numero_pedido = EXCLUDED.numero_pedido,
  codigo_cliente_omie = EXCLUDED.codigo_cliente_omie,
  etapa = EXCLUDED.etapa,
  data_previsao = EXCLUDED.data_previsao,
  obs_venda = EXCLUDED.obs_venda,
  raw_json = EXCLUDED.raw_json,
  created_at = EXCLUDED.created_at,
  updated_at = EXCLUDED.updated_at;
"""

SQL_UPSERT_ITENS = f"""
INSERT INTO omie.omie_pedido_itens (
  codigo_pedido, {ITEM_SEQ_COLUMN}, codigo_produto, descricao, cfop,
  quantidade, unidade, valor_unitario, valor_total
) VALUES %s
ON CONFLICT (codigo_pedido, {ITEM_SEQ_COLUMN}) DO UPDATE SET
  codigo_produto = EXCLUDED.codigo_produto,
  descricao = EXCLUDED.descricao,
  cfop = EXCLUDED.cfop,
  quantidade = EXCLUDED.quantidade,
  unidade = EXCLUDED.unidade,
  valor_unitario = EXCLUDED.valor_unitario,
  valor_total = EXCLUDED.valor_total;
"""

# ------------------------
# Chamadas Omie
# ------------------------
@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(6))
def omie_call(service: str, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{OMIE_BASE_URL}/{service}/"
    body = {
        "call": method,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [payload],
    }
    r = requests.post(url, json=body, timeout=60)
    if r.status_code == 429:
        raise RuntimeError("Omie rate-limited (429)")
    r.raise_for_status()
    return r.json()

def listar_pedidos_intervalo(dt_de: dt.datetime, dt_ate: dt.datetime, page: int) -> Dict[str, Any]:
    # Omie costuma esperar dd/mm/YYYY
    payload = {
        "pagina": page,
        "registros_por_pagina": PAGE_SIZE,
        "apenas_importado_api": "N",
        # nomes de filtros variam entre APIs; estes funcionam para a maioria
        "filtrar_por_data_de":  dt_de.strftime("%d/%m/%Y"),
        "filtrar_por_data_ate": dt_ate.strftime("%d/%m/%Y"),
        "ordenar_por": "DESC",
    }
    return omie_call(SERVICE_PEDIDOS, METHOD_LISTAR_PEDIDOS, payload)

def consultar_pedido(codigo_pedido: int) -> Dict[str, Any]:
    payload = {"codigo_pedido": codigo_pedido}
    return omie_call(SERVICE_PEDIDOS, METHOD_CONSULTAR_PEDIDO, payload)

# ------------------------
# Mapeamento JSON -> colunas
# ------------------------
def normalize_dt(val: Any) -> str:
    """Converte para ISO (string) se vier formato livre do Omie."""
    if not val:
        return None
    if isinstance(val, (dt.datetime, dt.date)):
        return dt.datetime.fromtimestamp(val.timestamp()).isoformat()
    # tenta dd/mm/YYYY HH:MM:SS
    try:
        return dt.datetime.strptime(str(val), "%d/%m/%Y %H:%M:%S").isoformat()
    except Exception:
        pass
    # tenta dd/mm/YYYY
    try:
        return dt.datetime.strptime(str(val), "%d/%m/%Y").isoformat()
    except Exception:
        pass
    return str(val)

def map_pedidos(resp: Dict[str, Any]) -> Tuple[List[Tuple], List[int]]:
    """
    Aceita tanto chaves 'pedidos' quanto 'pedido_venda_produto'.
    Retorna: linhas_pedidos, lista_codigos
    """
    arr = []
    codigos = []

    registros = resp.get("pedidos") or resp.get("pedido_venda_produto") or []
    for p in registros:
        # em algumas respostas o pedido vem aninhado em 'pedido'
        base = p.get("pedido") or p

        # campos típicos (ajuste se sua conta tiver nomes diferentes)
        codigo_pedido = int(base.get("codigo_pedido") or base.get("codigo_pedido_omie") or 0)
        if not codigo_pedido:
            # se não tiver código, ignora
            continue

        numero_pedido = str(base.get("numero_pedido") or base.get("numero"))
        codigo_cliente_omie = int(base.get("codigo_cliente_omie") or base.get("codigo_cliente") or 0)
        etapa = base.get("etapa") or base.get("etapa_pedido")
        data_previsao = base.get("data_previsao") or base.get("data_previsao_faturamento")
        obs_venda = base.get("obs_venda") or base.get("observacao")

        created_at = normalize_dt(base.get("created_at")) or dt.datetime.utcnow().isoformat()
        updated_at = normalize_dt(base.get("updated_at")) or created_at

        raw_json = json.dumps(p, ensure_ascii=False)

        arr.append((
            codigo_pedido, numero_pedido, codigo_cliente_omie, etapa, data_previsao,
            obs_venda, raw_json, created_at, updated_at
        ))
        codigos.append(codigo_pedido)

    return arr, codigos

def map_itens(resp: Dict[str, Any], codigo_pedido: int) -> List[Tuple]:
    """
    Itens podem vir em 'det', 'produtos', 'itens' ou dentro de 'pedido'->'det'.
    Campo de sequência pode ser 'nItem', 'sequencia', 'sequencia_item' ou faltante.
    """
    linhas = []
    det = (resp.get("pedido") or {}).get("det") or \
          resp.get("produtos") or resp.get("itens") or resp.get("det") or []

    if isinstance(det, dict):  # às vezes vem dict { 'produto': {...}}
        det = det.get("produto") or det.get("item") or []

    if not isinstance(det, list):
        det = [det]

    seq_auto = 0
    for it in det:
        prod = it.get("produto") or it.get("item") or it

        seq = (prod.get("nItem") or prod.get("sequencia") or prod.get("sequencia_item"))
        if seq is None:
            seq_auto += 1
            seq = seq_auto
        seq = int(seq)

        codigo_produto = str(prod.get("codigo_produto") or prod.get("codigo") or "")
        descricao = prod.get("descricao_produto") or prod.get("descricao") or prod.get("nome") or ""
        cfop = prod.get("cfop")
        try:
            quantidade = float(prod.get("quantidade") or prod.get("qtde") or 0)
        except Exception:
            quantidade = 0.0
        unidade = prod.get("unidade") or prod.get("und")
        try:
            vunit = float(prod.get("valor_unitario") or prod.get("vl_unit") or prod.get("valor_unit") or 0)
        except Exception:
            vunit = 0.0
        try:
            vtot = float(prod.get("valor_total") or prod.get("vl_total") or 0)
        except Exception:
            vtot = round(quantidade * vunit, 2)

        linhas.append((
            codigo_pedido, seq, codigo_produto, descricao, cfop,
            quantidade, unidade, vunit, vtot
        ))

    return linhas

# ------------------------
# Loop principal
# ------------------------
def main(since_iso: str = None, full: bool = False):
    if not all([OMIE_APP_KEY, OMIE_APP_SECRET, PGDATABASE, PGUSER, PGPASSWORD]):
        print("[ERRO] Variáveis de ambiente ausentes. Configure o .env.")
        sys.exit(2)

    with psycopg2.connect(PG_DSN) as con:
        con.autocommit = False
        cur = con.cursor()

        # garante tabela de controle
        cur.execute(SQL_ENSURE_SYNC_TABLE)
        con.commit()

        # define intervalo
        if full:
            dt_de = dt.datetime(1970, 1, 1)
        elif since_iso:
            dt_de = dt.datetime.fromisoformat(since_iso)
        else:
            cur.execute(SQL_GET_LAST_SYNC, (SYNC_SOURCE,))
            dt_de = cur.fetchone()[0]
            if isinstance(dt_de, dt.datetime):
                dt_de = dt_de.replace(tzinfo=None)
        dt_ate = dt.datetime.now()

        print(f"[SYNC] Pedidos de {dt_de:%d/%m/%Y} até {dt_ate:%d/%m/%Y}")

        total_ped, total_itens = 0, 0
        page = 1
        while True:
            resp = listar_pedidos_intervalo(dt_de, dt_ate, page)

            linhas_ped, codigos = map_pedidos(resp)
            if not linhas_ped:
                # pode ser que método traga outra chave de paginação
                # tenta encerrar se página retornou vazia
                if page == 1:
                    print("[SYNC] Nenhum pedido no intervalo.")
                break

            execute_values(cur, SQL_UPSERT_PEDIDOS, linhas_ped, page_size=PAGE_SIZE)
            total_ped += len(linhas_ped)

            # Para cada pedido, baixa itens e upserta
            for cod in codigos:
                try:
                    det = consultar_pedido(cod)
                    linhas_it = map_itens(det, cod)
                    if linhas_it:
                        execute_values(cur, SQL_UPSERT_ITENS, linhas_it, page_size=PAGE_SIZE)
                        total_itens += len(linhas_it)
                except Exception as e:
                    # não aborta toda a página por causa de um pedido
                    print(f"[WARN] Falha ao consultar itens do pedido {cod}: {e}")

            con.commit()

            # Paginação: tenta ler metadados do Omie
            total_paginas = resp.get("total_de_paginas") or resp.get("total_paginas") or None
            pagina_atual  = resp.get("pagina") or page
            if total_paginas and pagina_atual >= total_paginas:
                break

            page += 1

        # marca sucesso e atualiza estatísticas
        cur.execute(SQL_SET_LAST_SYNC, (SYNC_SOURCE,))
        cur.execute("ANALYZE omie.omie_pedidos; ANALYZE omie.omie_pedido_itens;")
        con.commit()

        print(f"[OK] UPSERT pedidos: {total_ped} | itens: {total_itens}")

if __name__ == "__main__":
    # Exemplos:
    #   python sync_omie.py                 -> incremental desde último sucesso
    #   python sync_omie.py 2025-08-01     -> incremental desde data
    #   python sync_omie.py full           -> carga completa
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    if arg == "full":
        main(full=True)
    elif arg:
        main(since_iso=arg)
    else:
        main()
