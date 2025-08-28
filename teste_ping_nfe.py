# -*- coding: utf-8 -*-
"""
Ping mínimo NF-e (OMIE) — sem filtros no request
- Endpoint: /api/v1/produtos/nfe/
- CALL: ListarNFe
- Request só com nPagina / nRegPorPagina
- Filtro local: tpNF = 'S' (saída) e dEmissao >= CHECKPOINT_DATA
"""

import os
import json
from datetime import datetime, date
import requests

# ========= CONFIG =========
URL  = "https://app.omie.com.br/api/v1/produtos/nfe/"
CALL = "ListarNFe"

# ajuste aqui o seu ponto de partida (data mínima de emissão)
CHECKPOINT_DATA = date(2025, 8, 22)

# quantidade por página para amostrar (pode aumentar)
N_REG_POR_PAGINA = 50

APP_KEY    = os.getenv("OMIE_APP_KEY")
APP_SECRET = os.getenv("OMIE_APP_SECRET")
# ==========================

def omie_call(url, call_name, app_key, app_secret, params):
    payload = {
        "call": call_name,
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [params],
    }
    resp = requests.post(url, json=payload, timeout=40)
    print(f"STATUS: {resp.status_code}")
    try:
        data = resp.json()
        print("TEXTO :", json.dumps(data, ensure_ascii=False)[:400])
    except Exception:
        print("TEXTO :", (resp.text or "")[:400])
        data = None
    return resp.status_code, data

def parse_emissao(item) -> date | None:
    candidatos = ["dEmissao", "data_emissao", "dataEmissao", "dtEmissao",
                  "dEmi", "emissao", "dataEmi"]
    raw = None
    for k in candidatos:
        if k in item and item[k]:
            raw = item[k]
            break
    if not raw:
        return None
    # tenta alguns formatos comuns
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(str(raw), fmt).date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(str(raw)[:10]).date()
    except Exception:
        return None

def is_saida(item) -> bool:
    tp = (item.get("tpNF") or item.get("tipoNF") or item.get("tipoNota") or "").upper()
    if tp in ("S", "SAIDA", "SAÍDA"):
        return True
    if tp in ("E", "ENTRADA"):
        return False
    # se a OMIE não mandar, não bloqueio
    return True

def main():
    if not APP_KEY or not APP_SECRET:
        print(">> ERRO: defina OMIE_APP_KEY e OMIE_APP_SECRET nas variáveis de ambiente.")
        return

    print("== Teste NFe / ListarNFe (sem filtros no request) ==")
    params = {"nPagina": 1, "nRegPorPagina": N_REG_POR_PAGINA}
    status, data = omie_call(URL, CALL, APP_KEY, APP_SECRET, params)

    if status != 200 or not isinstance(data, dict):
        print(">> Falha no ping. Acima você tem o STATUS e o corpo retornado.")
        return

    # campos comuns do envelope
    n_pag        = data.get("nPagina")
    n_tot_pag    = data.get("nTotPaginas")
    lista        = data.get("listagemNfe") or data.get("listagemNFe") or []

    print(f"\nEnvelope: nPagina={n_pag} | nTotPaginas={n_tot_pag} | itens_recebidos={len(lista)}")

    # aplica filtro local (saída + data >= checkpoint)
    filtrados = []
    for it in lista:
        if not is_saida(it):
            continue
        d = parse_emissao(it)
        if (d is None) or (d >= CHECKPOINT_DATA):
            filtrados.append(it)

    print(f"Após filtro local (tpNF='S' e dEmissao>={CHECKPOINT_DATA}): {len(filtrados)} itens\n")

    # mostra amostra de até 5 itens
    for i, it in enumerate(filtrados[:5], start=1):
        numero = it.get("numero", it.get("numero_nf", ""))
        chave  = it.get("chave", it.get("chave_nfe", ""))
        dEmi   = (parse_emissao(it) or "")
        tpNF   = (it.get("tpNF") or "").upper()
        cli    = it.get("cliente", "") or it.get("destinatario", "")
        print(f"[{i}] numero={numero} | chave={chave} | dEmissao={dEmi} | tpNF={tpNF} | cliente='{cli}'")

    print("\n>> Se acima veio STATUS=200 e apareceram itens após o filtro, está tudo OK para migrar o mesmo padrão para o sync_nfe.py.")

if __name__ == "__main__":
    main()
