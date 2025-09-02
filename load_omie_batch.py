#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Carga de pedidos (Omie) -> PostgreSQL
Versão: 2025-08-31 21:45 resilient-no-conflict
- Sem ON CONFLICT (funciona mesmo sem PK/índice)
- UPSERT manual (UPDATE; se 0 linhas, INSERT)
- Compatível com tabelas existentes (singular): omie_pedido, omie_pedido_itens, omie_pedido_parcelas
"""

import os
import sys
import json
import decimal
import datetime
import requests
import psycopg
from typing import Any, Dict, List

__VERSION__ = "2025-08-31 21:45 resilient-no-conflict"


# -------------------------------- Utils --------------------------------

def _s(v):
    return None if v is None else str(v)

def _d(v):
    if v is None or v == "":
        return None
    try:
        return decimal.Decimal(str(v).replace(",", "."))
    except Exception:
        return None

def log(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


# ------------------------------ Postgres -------------------------------

def _pg():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Defina DATABASE_URL no ambiente.")
    return psycopg.connect(dsn)

def _ensure_schema(con):
    """Cria somente se não existir, sem PK/UK pra não conflitar com o que já existe."""
    with con.cursor() as cur:
        # Cabeçalho de pedidos (singular)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS omie_pedido (
            id_pedido        text,
            numero           text,
            data_emissao     text,
            cliente_nome     text,
            valor_total      numeric,
            situacao         text,
            origem           text,
            codigo_cliente   text,
            bloqueado        text,
            quantidade_itens integer,
            raw              jsonb,
            created_at       timestamptz DEFAULT now(),
            updated_at       timestamptz DEFAULT now()
        );
        """)
        # Itens
        cur.execute("""
        CREATE TABLE IF NOT EXISTS omie_pedido_itens (
            id_pedido        text,
            numero           text,
            linha            integer,
            codigo           text,
            descricao        text,
            quantidade       numeric,
            valor_unit       numeric,
            valor_total      numeric,
            unidade          text,
            cfop             text,
            ncm              text,
            valor_mercadoria numeric,
            valor_desconto   numeric,
            tipo_desconto    text,
            created_at       timestamptz DEFAULT now(),
            updated_at       timestamptz DEFAULT now()
        );
        """)
        # Parcelas
        cur.execute("""
        CREATE TABLE IF NOT EXISTS omie_pedido_parcelas (
            id_pedido        text,
            numero           text,
            numero_parcela   integer,
            data_vencimento  text,
            percentual       numeric,
            valor            numeric,
            created_at       timestamptz DEFAULT now(),
            updated_at       timestamptz DEFAULT now()
        );
        """)
    con.commit()


# ---------------------------- Normalização -----------------------------

def norm_pedido(raw: Dict[str, Any]) -> Dict[str, Any]:
    cab = (raw or {}).get("cabecalho", {}) or {}
    return {
        "id_pedido": str(cab.get("codigo_pedido_integracao") or cab.get("codigo_pedido")),
        "numero": str(cab.get("codigo_pedido_integracao") or cab.get("codigo_pedido")),
        "data_emissao": cab.get("data_previsao"),
        "cliente_nome": cab.get("nome_cliente") or "",
        "valor_total": cab.get("valor_total_pedido") or 0,
        "situacao": cab.get("etapa"),
        "origem": cab.get("origem") or "",
        "codigo_cliente": cab.get("codigo_cliente"),
        "bloqueado": cab.get("bloqueado", "N"),
        "quantidade_itens": cab.get("quantidade_itens", 0),
        "itens": norm_itens(raw),
        "parcelas": norm_parcelas(raw),
        "raw": raw,
    }

def norm_itens(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    dets = (raw or {}).get("det", []) or []
    out = []
    for idx, det in enumerate(dets, 1):
        prod = (det or {}).get("produto", {}) or {}
        out.append({
            "linha": idx,
            "codigo": prod.get("codigo_produto"),
            "descricao": prod.get("descricao"),
            "quantidade": prod.get("quantidade"),
            "valor_unit": prod.get("valor_unitario"),
            "valor_total": prod.get("valor_total") or prod.get("valor_mercadoria"),
            "unidade": prod.get("unidade"),
            "cfop": prod.get("cfop"),
            "ncm": prod.get("ncm"),
            "valor_mercadoria": prod.get("valor_mercadoria"),
            "valor_desconto": prod.get("valor_desconto"),
            "tipo_desconto": prod.get("tipo_desconto"),
        })
    return out

def norm_parcelas(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    lista = ((raw or {}).get("lista_parcelas") or {}).get("parcela", []) or []
    out = []
    for p in lista:
        out.append({
            "numero_parcela": p.get("numero_parcela"),
            "data_vencimento": p.get("data_vencimento"),
            "percentual": p.get("percentual"),
            "valor": p.get("valor"),
        })
    return out


# ----------------------------- Persistência ----------------------------

def upsert_pedido(con, p: Dict[str, Any]):
    """UPSERT manual no cabeçalho (sem ON CONFLICT)."""
    with con.cursor() as cur:
        # UPDATE
        cur.execute("""
            UPDATE omie_pedido
               SET data_emissao=%s,
                   cliente_nome=%s,
                   valor_total=%s,
                   situacao=%s,
                   origem=%s,
                   codigo_cliente=%s,
                   bloqueado=%s,
                   quantidade_itens=%s,
                   raw=%s,
                   updated_at=now()
             WHERE id_pedido=%s AND numero=%s
        """, (
            _s(p["data_emissao"]), _s(p["cliente_nome"]), _d(p["valor_total"]),
            _s(p["situacao"]), _s(p["origem"]), _s(p["codigo_cliente"]),
            _s(p["bloqueado"]), int(p["quantidade_itens"] or 0),
            json.dumps(p["raw"], ensure_ascii=False),
            _s(p["id_pedido"]), _s(p["numero"])
        ))
        if cur.rowcount == 0:
            # INSERT
            cur.execute("""
                INSERT INTO omie_pedido
                    (id_pedido, numero, data_emissao, cliente_nome, valor_total,
                     situacao, origem, codigo_cliente, bloqueado, quantidade_itens, raw,
                     created_at, updated_at)
                VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
            """, (
                _s(p["id_pedido"]), _s(p["numero"]), _s(p["data_emissao"]),
                _s(p["cliente_nome"]), _d(p["valor_total"]), _s(p["situacao"]),
                _s(p["origem"]), _s(p["codigo_cliente"]), _s(p["bloqueado"]),
                int(p["quantidade_itens"] or 0),
                json.dumps(p["raw"], ensure_ascii=False),
            ))

def replace_itens(con, p: Dict[str, Any]):
    with con.cursor() as cur:
        cur.execute("DELETE FROM omie_pedido_itens WHERE id_pedido=%s AND numero=%s",
                    (_s(p["id_pedido"]), _s(p["numero"])))
        for it in p["itens"]:
            cur.execute("""
                INSERT INTO omie_pedido_itens
                    (id_pedido, numero, linha, codigo, descricao,
                     quantidade, valor_unit, valor_total, unidade,
                     cfop, ncm, valor_mercadoria, valor_desconto, tipo_desconto,
                     created_at, updated_at)
                VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
            """, (
                _s(p["id_pedido"]), _s(p["numero"]), int(it.get("linha") or 0),
                _s(it.get("codigo")), _s(it.get("descricao")),
                _d(it.get("quantidade")), _d(it.get("valor_unit")), _d(it.get("valor_total")),
                _s(it.get("unidade")), _s(it.get("cfop")), _s(it.get("ncm")),
                _d(it.get("valor_mercadoria")), _d(it.get("valor_desconto")), _s(it.get("tipo_desconto"))
            ))

def replace_parcelas(con, p: Dict[str, Any]):
    with con.cursor() as cur:
        cur.execute("DELETE FROM omie_pedido_parcelas WHERE id_pedido=%s AND numero=%s",
                    (_s(p["id_pedido"]), _s(p["numero"])))
        for par in p["parcelas"]:
            cur.execute("""
                INSERT INTO omie_pedido_parcelas
                    (id_pedido, numero, numero_parcela, data_vencimento,
                     percentual, valor, created_at, updated_at)
                VALUES
                    (%s,%s,%s,%s,%s,%s, now(), now())
            """, (
                _s(p["id_pedido"]), _s(p["numero"]),
                int(par.get("numero_parcela") or 0),
                _s(par.get("data_vencimento")),
                _d(par.get("percentual")),
                _d(par.get("valor")),
            ))

def persistir_pedido(p: Dict[str, Any]):
    with _pg() as con:
        _ensure_schema(con)
        upsert_pedido(con, p)
        replace_itens(con, p)
        replace_parcelas(con, p)
        con.commit()


# ------------------------------ Omie API -------------------------------

def baixar_pedidos() -> List[Dict[str, Any]]:
    """ListarPedidos — pagina única (ajuste se quiser paginação)."""
    url = "https://app.omie.com.br/api/v1/produtos/pedido/"
    payload = {
        "call": "ListarPedidos",
        "app_key": os.environ.get("OMIE_APP_KEY"),
        "app_secret": os.environ.get("OMIE_APP_SECRET"),
        "param": [{
            "pagina": 1,
            "registros_por_pagina": 100,
            "apenas_importado_api": "N"
        }]
    }
    resp = requests.post(url, json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Omie HTTP {resp.status_code}: {resp.text[:400]}")
    data = resp.json()
    return data.get("pedido_venda_produto", []) or []


# --------------------------------- Main --------------------------------

def main():
    log(f"Iniciando carga Omie - versão {__VERSION__}")
    log("Baixando PEDIDOS (ListarPedidos)…")
    pedidos_raw = baixar_pedidos()
    log(f"Total pedidos recebidos : {len(pedidos_raw)}")

    for raw in pedidos_raw:
        p = norm_pedido(raw)
        persistir_pedido(p)

    log("Carga finalizada com sucesso.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERRO durante a carga:", repr(e))
        raise
