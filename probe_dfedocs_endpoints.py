# probe_dfedocs_endpoints.py
import os, sys, json, requests
from datetime import date

APP_KEY = os.getenv("OMIE_APP_KEY")
APP_SECRET = os.getenv("OMIE_APP_SECRET")

if not (APP_KEY and APP_SECRET):
    print("[ERRO] Configure OMIE_APP_KEY e OMIE_APP_SECRET no shell antes de rodar.")
    sys.exit(1)

# Use uma chave de 44 dígitos que exista aí e a data de emissão correspondente
CHAVE = (os.getenv("CHAVE_TESTE") or "35250806241162000106550010000095041899293824").strip()
EMISSAO = (os.getenv("EMISSAO_TESTE") or str(date.today())).strip()

# Matriz de tentativas: (url_base, call, param_builder)
def p_dfedocs_obter(chave, emissao):
    return {"cNumNFe":"", "nChaveNFe":chave, "dDataEmisNFe":emissao}
def p_dfedocs_listar_por_chave(chave, emissao):
    return {"nChaveNFe":chave, "filtrar_por":{"nChaveNFe":chave}, "pagina":1, "registros_por_pagina":20}
def p_nfe_obterNFe(chave, emissao):
    return {"cNumNFe":"", "nChaveNFe":chave, "dDataEmisNFe":emissao}
def p_nfe_obterNF(chave, emissao):
    return {"cNumNF":"", "nChaveNF":chave, "dDataEmisNF":emissao}
def p_nfe_obter(chave, emissao):
    return {"cNumNFe":"", "cNumNF":"", "nChaveNFe":chave, "nChaveNF":chave, "dDataEmisNFe":emissao, "dDataEmisNF":emissao}

tests = [
    ("https://app.omie.com.br/api/v1/produtos/dfedocs/","Obter",     p_dfedocs_obter),
    ("https://app.omie.com.br/api/v1/produtos/dfedocs/","Listar",    p_dfedocs_listar_por_chave),
    ("https://app.omie.com.br/api/v1/produtos/nfe/",    "ObterNFe",  p_nfe_obterNFe),
    ("https://app.omie.com.br/api/v1/produtos/nfe/",    "ObterNF",   p_nfe_obterNF),
    ("https://app.omie.com.br/api/v1/produtos/nfe/",    "Obter",     p_nfe_obter),
]

def try_call(url, call, make_param):
    body = {
        "call": call,
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [ make_param(CHAVE, EMISSAO) ],
    }
    r = requests.post(url, json=body, timeout=60)
    ok = (200 <= r.status_code < 300)
    # Algumas APIs da Omie respondem 200 mesmo com erro, então olhamos o JSON
    try:
        data = r.json()
    except Exception:
        data = {"_raw": r.text}

    print(f"\n=== Tentando {url} call={call}")
    print("HTTP:", r.status_code)
    print((json.dumps(data, ensure_ascii=False)[:4000]))

    # heurística de sucesso: tem alguma das chaves típicas
    payload = json.dumps(data)
    has_doc = any(k in payload for k in ["xml", "cXmlNFe", "cPdf", "cLinkPortal", "codigo_status", "codStatus"])
    if ok and has_doc:
        print("\n>>> PARECE OK com:", url, call)
        return True
    return False

def main():
    achou = False
    for url, call, fn in tests:
        try:
            if try_call(url, call, fn):
                achou = True
        except Exception as e:
            print(f"[ERRO] {url} {call}: {e}")
    if not achou:
        print("\n[NADA ENCONTRADO] Nenhuma combinação respondeu como válida para sua conta.")

if __name__ == "__main__":
    main()
