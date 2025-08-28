# -*- coding: utf-8 -*-
"""
Sincronização de PEDIDOS Omie.

Ajustes principais:
- Tenta múltiplos endpoints (vendas/pedido e produtos/pedido)
- Tratamento robusto de erros/respostas inesperadas
- --desde e --debug preservados
- Ponto de integração para seu upsert (onde indicado)
"""

from __future__ import annotations
from datetime import datetime
from sync_common import (
    APP_KEY, APP_SECRET, DB_SCHEMA,
    parse_args, _parse_date, paginar, omie_call
)

# Alguns ambientes do Omie variam o endpoint
URLS_CALLS = [
    ("https://app.omie.com.br/api/v1/vendas/pedido/",   "ListarPedidos"),
    ("https://app.omie.com.br/api/v1/produtos/pedido/", "ListarPedidos"),
]

TAM_PAGINA = 100

# ----------------------------------------------------------------------
# (Opcional) Seu upsert aqui.
# Se você já tem suas funções de upsert, chame-as no lugar deste stub.
# ----------------------------------------------------------------------
def upsert_pedido_stub(obj) -> None:
    """
    Substitua pelo seu upsert real no banco.
    """
    # exemplo: print(obj.get("cabecalho", {}).get("codigo_pedido"))
    return

# ----------------------------------------------------------------------
def escolher_endpoint() -> tuple[str, str]:
    """
    Testa cada (URL, CALL) e retorna o primeiro que responder 200/JSON.
    """
    for u, c in URLS_CALLS:
        try:
            _ = omie_call(u, c, APP_KEY, APP_SECRET,
                          {"pagina": 1, "registros_por_pagina": 1})
            return u, c
        except Exception as e:
            print(f"[INFO] Falhou {u} ({c}): {e}")
    raise RuntimeError("Nenhum endpoint de pedidos respondeu corretamente.")

# ----------------------------------------------------------------------
def main():
    args = parse_args()
    desde_dt = _parse_date(args.desde)

    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [INFO] == INÍCIO SYNC_PEDIDOS ==")
    if args.desde:
        print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [INFO] desde = {desde_dt}")

    url, call = escolher_endpoint()
    upserts = 0

    for pagina, data in paginar(url, call, APP_KEY, APP_SECRET,
                                desde_dt=desde_dt, tam_pagina=TAM_PAGINA):

        # possíveis chaves nas respostas de pedidos
        lista = (
            data.get("pedido_venda_produto")
            or data.get("lista_pedidos")
            or data.get("pedido_venda")
            or []
        )

        for obj in lista:
            upsert_pedido_stub(obj)  # <<< troque pelo seu upsert real
            upserts += 1

        print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [INFO] página {pagina} ok — "
              f"total upserts: {upserts}")

    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [INFO] == FIM SYNC_PEDIDOS == "
          f"Total upserts: {upserts}")

if __name__ == "__main__":
    main()
