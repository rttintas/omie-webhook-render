# -*- coding: utf-8 -*-
"""
Sincronização de PRODUTOS Omie.

Ajustes principais:
- Fallback automático entre ListarProdutos e ListarProdutosResumido
- Tratamento aprimorado de erros (via sync_common.omie_call)
- --desde e --debug preservados
- Ponto de integração para seu upsert (onde indicado)
"""

from __future__ import annotations
from datetime import datetime
from sync_common import (
    APP_KEY, APP_SECRET, DB_SCHEMA,
    parse_args, _parse_date, paginar, omie_call
)

# Endpoints/calls
URL = "https://app.omie.com.br/api/v1/geral/produtos/"
CALL_PREFERIDA = "ListarProdutos"
CALL_FALLBACK  = "ListarProdutosResumido"

TAM_PAGINA = 100

# ----------------------------------------------------------------------
# (Opcional) Seu upsert aqui.
# Se você já tem suas funções de upsert, chame-as no lugar deste stub.
# ----------------------------------------------------------------------
def upsert_produto_stub(obj) -> None:
    """
    Substitua pelo seu upsert real no banco.
    Este stub existe apenas para manter o script executável.
    """
    # exemplo: print(obj.get("codigo_produto") or obj.get("codigo"))
    return

# ----------------------------------------------------------------------
def escolher_call() -> str:
    """
    Testa a call preferida. Se falhar, usa a de fallback.
    """
    call = CALL_PREFERIDA
    try:
        _ = omie_call(
            URL, call, APP_KEY, APP_SECRET,
            {"pagina": 1, "registros_por_pagina": 1}
        )
    except Exception as e:
        print(f"[INFO] CALL {CALL_PREFERIDA} falhou no teste: {e}. "
              f"Tentando {CALL_FALLBACK}...")
        call = CALL_FALLBACK
        # garante que o fallback responde
        _ = omie_call(
            URL, call, APP_KEY, APP_SECRET,
            {"pagina": 1, "registros_por_pagina": 1}
        )
    return call

# ----------------------------------------------------------------------
def main():
    args = parse_args()
    desde_dt = _parse_date(args.desde)

    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [INFO] == INÍCIO SYNC_PRODUTOS ==")
    if args.desde:
        print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [INFO] desde = {desde_dt}")

    call = escolher_call()
    upserts = 0

    for pagina, data in paginar(URL, call, APP_KEY, APP_SECRET,
                                desde_dt=desde_dt, tam_pagina=TAM_PAGINA):
        # possíveis chaves dependendo da call/conta
        lista = (
            data.get("produtos_cadastro")
            or data.get("lista_produtos")
            or data.get("produto_servico_cadastro")
            or []
        )

        for obj in lista:
            upsert_produto_stub(obj)  # <<< troque pelo seu upsert real
            upserts += 1

        print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [INFO] página {pagina} ok — "
              f"total upserts: {upserts}")

    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [INFO] == FIM SYNC_PRODUTOS == "
          f"Total upserts: {upserts}")

if __name__ == "__main__":
    main()
