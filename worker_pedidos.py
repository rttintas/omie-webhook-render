# -*- coding: utf-8 -*-
"""
Worker que lê jobs em public.omie_jobs (tipo 'pedido_by_numero'),
consulta a Omie e grava pedido + itens nas tabelas:
  - public.omie_pedido
  - public.omie_pedido_itens

Variáveis de ambiente exigidas:
  - PG_DSN  (ex.: 'dbname=neondb user=neondb_owner password=... host=... port=5432 sslmode=require')
  - OMIE_APP_KEY
  - OMIE_APP_SECRET
"""

import os
import time
import json
import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Tuple, Optional

import psycopg2
import psycopg2.extras
import requests

# --------------------------------------------------------------------
# Config & helpers
# --------------------------------------------------------------------
PG_DSN        = os.getenv("PG_DSN")
OMIE_APP_KEY  = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

OMIE_URL_PEDIDOS = "https://app.omie.com.br/api/v1/produtos/pedido/"

LOG = logging.getLogger("worker_pedidos")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

if not PG_DSN:
    raise SystemExit("Falta PG_DSN no ambiente.")
if not OMIE_APP_KEY or not OMIE_APP_SECRET:
    raise SystemExit("Faltam OMIE_APP_KEY/OMIE_APP_SECRET no ambiente.")

# --------------------------------------------------------------------
# DB
# --------------------------------------------------------------------
@contextmanager
def get_conn():
    conn = psycopg2.connect(PG_DSN)  # sslmode=require já vai no DSN
    try:
        yield conn
    finally:
        conn.close()

def fetch_one_job(cur) -> Optional[Dict[str, Any]]:
    cur.execute("""
        SELECT id, tipo, payload_json
          FROM public.omie_jobs
         WHERE status = 'new'
         ORDER BY created_at
         LIMIT 1
        FOR UPDATE SKIP LOCKED
    """)
    row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "tipo": row[1], "payload": row[2]}

def mark_job(cur, job_id: int, status: str, erro: Optional[str] = None):
    cur.execute("""
        UPDATE public.omie_jobs
           SET status = %s,
               erro   = %s,
               updated_at = now()
         WHERE id = %s
    """, (status, erro, job_id))

# --------------------------------------------------------------------
# Parsers (baseado no seu sync_pedidos.py)  :contentReference[oaicite:2]{index=2}
# --------------------------------------------------------------------
def _norm_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def iter_itens(p: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Aceita vários formatos que a Omie retorna:
      - "itens": [ { sequencia/codigo_produto/descricao/quantidade/unidade/valor_unitario ... } ]
      - "pedido_itens": idem
      - "lista_itens": idem
    """
    itens = (
        p.get("itens")
        or p.get("pedido_itens")
        or p.get("lista_itens")
        or []
    )
    for i, it in enumerate(itens, start=1):
        yield {
            "linha":          it.get("sequencia") or it.get("item") or i,
            "codigo_produto": _norm_text(it.get("codigo_produto") or it.get("id_produto") or it.get("codigo")),
            "descricao":      _norm_text(it.get("descricao") or it.get("produto") or it.get("descricao_produto")),
            "quantidade":     it.get("quantidade"),
            "unidade":        _norm_text(it.get("unidade")),
            "vl_unitario":    it.get("valor_unitario") or it.get("vl_unitario") or it.get("preco_unitario"),
        }

# --------------------------------------------------------------------
# Omie API
# --------------------------------------------------------------------
def omie_post(call: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Chama a Omie (POST) e retorna dict. Lança HTTPError em !=200."""
    payload = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [params],
    }
    r = requests.post(OMIE_URL_PEDIDOS, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    # Alguns erros vêm 200 mas com "faultstring" / "faultcode" no corpo
    if isinstance(data, dict) and ("faultstring" in data or "faultcode" in data):
        raise requests.HTTPError(f"Omie error: {data}")
    return data

def carregar_pedido_por_numero(numero_pedido: str) -> Dict[str, Any]:
    """
    1) Tenta ConsultarPedido (por número)
    2) Se falhar (erro 4xx/5xx) tenta ListarPedidos com filtro numero_pedido
    Retorna sempre um dict 'pedido' com itens.
    """
    # 1) ConsultarPedido
    try:
        data = omie_post("ConsultarPedido", {"numero_pedido": numero_pedido})
        if isinstance(data, dict) and data:
            return data
    except requests.HTTPError as e:
        LOG.warning("ConsultarPedido falhou (%s). Tentando ListarPedidos...", e)

    # 2) ListarPedidos
    data = omie_post("ListarPedidos", {"numero_pedido": numero_pedido})
    # Pode vir paginado; garantimos 1 pedido
    if isinstance(data, dict) and "lista_pedidos" in data and data["lista_pedidos"]:
        # alguns retornos: {"lista_pedidos":[{...}],"pagina":1,...}
        return data["lista_pedidos"][0]
    return data  # fallback

# --------------------------------------------------------------------
# UPSERTs – ajuste para os nomes de colunas existentes no seu DB
# (pedido já estava gravando no seu worker anterior; deixo seguro/estável)
# --------------------------------------------------------------------
UPSERT_PEDIDO = """
INSERT INTO public.omie_pedido
  (id_pedido, numero_pedido, cliente, marketplace, emitida_em, outras_info, raw_json)
VALUES
  (%(id_pedido)s, %(numero_pedido)s, %(cliente)s, %(marketplace)s, %(emitida_em)s, %(outras_info)s::jsonb, %(raw_json)s::jsonb)
ON CONFLICT (id_pedido) DO UPDATE SET
  numero_pedido = EXCLUDED.numero_pedido,
  cliente       = EXCLUDED.cliente,
  marketplace   = EXCLUDED.marketplace,
  emitida_em    = EXCLUDED.emitida_em,
  outras_info   = EXCLUDED.outras_info,
  raw_json      = EXCLUDED.raw_json;
"""

UPSERT_ITEM = """
INSERT INTO public.omie_pedido_itens
  (id_pedido, linha, codigo_produto, descricao, quantidade, unidade, vl_unitario)
VALUES
  (%(id_pedido)s, %(linha)s, %(codigo_produto)s, %(descricao)s, %(quantidade)s, %(unidade)s, %(vl_unitario)s)
ON CONFLICT (id_pedido, linha) DO UPDATE SET
  codigo_produto = EXCLUDED.codigo_produto,
  descricao      = EXCLUDED.descricao,
  quantidade     = EXCLUDED.quantidade,
  unidade        = EXCLUDED.unidade,
  vl_unitario    = EXCLUDED.vl_unitario;
"""

def map_pedido_basico(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrai só o que temos certeza que existe nas suas tabelas.
    (seu pedido já estava gravando; mantemos compatível)
    """
    return {
        "id_pedido":    _norm_text(p.get("codigo_pedido") or p.get("id_pedido") or p.get("codigo")),
        "numero_pedido":_norm_text(p.get("numero_pedido") or p.get("numero")),
        "cliente":      _norm_text(p.get("razao_social") or p.get("cliente") or p.get("nome_cliente")),
        "marketplace":  _norm_text(p.get("marketplace") or (p.get("outras_informacoes") or {}).get("marketplace")),
        "emitida_em":   _norm_text(p.get("data_emissao") or p.get("emissao") or p.get("data_fechamento")),
        "outras_info":  json.dumps(p.get("outras_informacoes") or {}, ensure_ascii=False),
        "raw_json":     json.dumps(p, ensure_ascii=False),
    }

# --------------------------------------------------------------------
# Handlers
# --------------------------------------------------------------------
def handle_pedido_by_numero(cur, payload: Dict[str, Any]) -> Tuple[str, int]:
    """
    Lê payload {"numero_pedido": "..."} e grava pedido + itens.
    Retorna (numero_pedido, qtd_itens_gravados).
    """
    numero = str(payload.get("numero_pedido") or "").strip()
    if not numero:
        raise ValueError("payload.numero_pedido está vazio.")

    pedido = carregar_pedido_por_numero(numero)

    # 1) upsert do cabeçalho
    ped_row = map_pedido_basico(pedido)
    if not ped_row["id_pedido"]:
        # Se a Omie não retorna id/codigo, chaveamos por numero_pedido
        ped_row["id_pedido"] = ped_row["numero_pedido"]
    cur.execute(UPSERT_PEDIDO, ped_row)

    # 2) itens (robusto)
    id_pedido = ped_row["id_pedido"]
    n_itens = 0
    for it in iter_itens(pedido):
        it_row = {"id_pedido": id_pedido, **it}
        cur.execute(UPSERT_ITEM, it_row)
        n_itens += 1

    return numero, n_itens

HANDLERS = {
    "pedido_by_numero": handle_pedido_by_numero,
}

# --------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------
def main():
    LOG.info("Worker de pedidos iniciado. Aguardando jobs em public.omie_jobs ...")
    while True:
        with get_conn() as conn, conn.cursor() as cur:
            job = fetch_one_job(cur)
            if not job:
                conn.commit()
                time.sleep(1.0)
                continue

            job_id = job["id"]
            tipo   = job["tipo"]
            payload = job["payload"] or {}
            try:
                handler = HANDLERS.get(tipo)
                if not handler:
                    raise ValueError(f"tipo de job desconhecido: {tipo}")

                numero, n_itens = handler(cur, payload)
                mark_job(cur, job_id, "done", None)
                conn.commit()
                LOG.info("[OK] Pedido %s: pedido + %d itens gravados.", numero, n_itens)
            except Exception as e:
                conn.rollback()
                mark_job(cur, job_id, "error", str(e))
                conn.commit()
                LOG.exception("Falha no job %s (%s): %s", job_id, tipo, e)

        # pequena pausa entre ciclos
        time.sleep(0.3)

if __name__ == "__main__":
    main()
