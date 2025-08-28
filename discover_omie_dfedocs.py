# -*- coding: utf-8 -*-
import os, json, datetime, requests

APP_KEY    = (os.environ.get("OMIE_APP_KEY") or "").strip()
APP_SECRET = (os.environ.get("OMIE_APP_SECRET") or "").strip()
assert APP_KEY and APP_SECRET, "Configure OMIE_APP_KEY e OMIE_APP_SECRET"

# chaves de exemplo: pego da tabela (se existir) ou deixa manual
CHAVES = [
    "35250806241162000106550010000095041899293824",
    "35250806241162000106550010000095051006208727",
]
HOJE = datetime.date.today().isoformat()

ENDPOINTS = [
    "https://app.omie.com.br/api/v1/produtos/dfedocs/",
    "https://app.omie.com.br/api/v1/produtos/nfe/",
    "https://app.omie.com.br/api/v1/vendas/nfe/",
    "https://app.omie.com.br/api/v1/produtos/nf/",
    "https://app.omie.com.br/api/v1/produtos/dfedown/",
    "https://app.omie.com.br/api/v1/fiscal/nfe/",
]
METHODS = [
    "ConsultarVersao", "ObterNFe", "ObterNF", "Obter",
    "ObterPorChave", "PesquisarDFe", "Pesquisar",
    "Listar", "ListarNFe", "ListarNFeEmitidas", "EmitidasListar",
    "Get", "GetByKey"
]

def variantes_params(chave, emissao):
    return [
        # variantes “curtas”
        {"nChaveNFe": chave},
        {"nChaveNF": chave},
        {"nChaveNFe": chave, "dDataEmisNFe": emissao},
        {"nChaveNF": chave, "dDataEmisNF": emissao},
        # variantes “com num”
        {"cNumNFe": "", "cNumNF": "", "nChaveNFe": chave, "nChaveNF": chave,
         "dDataEmisNFe": emissao, "dDataEmisNF": emissao},
        # algumas APIs aceitam “filtro”
        {"filtrar_por": {"nChaveNFe": chave}, "pagina": 1, "registros_por_pagina": 20},
    ]

def tenta(url, call, param):
    body = {"call": call, "app_key": APP_KEY, "app_secret": APP_SECRET, "param": [param]}
    try:
        r = requests.post(url, json=body, timeout=45)
    except Exception as e:
        return {"ok": False, "status": "NETWORK", "detail": str(e)}
    try:
        j = r.json()
    except Exception:
        j = {"raw": r.text[:800]}
    # Heurística: 200 + sem {"status":"error"} -> candidato a OK
    if r.status_code == 200 and not (isinstance(j, dict) and j.get("status") == "error"):
        return {"ok": True, "http": r.status_code, "json": j}
    # 500 com SOAP/JSON explicando tag errada => método existe mas params errados
    msg = None
    if isinstance(j, dict):
        msg = j.get("message") or j.get("faultstring") or j.get("status") or str(j)[:800]
    else:
        msg = str(j)[:800]
    return {"ok": False, "http": r.status_code, "detail": msg}

achados = []
for url in ENDPOINTS:
    for call in METHODS:
        for chave in CHAVES:
            for p in variantes_params(chave, HOJE):
                res = tenta(url, call, p)
                head = (res.get("detail") or res.get("json") or "") if not res["ok"] else "OK"
                print(f"[TEST] {url} | {call} | {list(p.keys())} -> {'OK' if res['ok'] else 'FAIL'} ({res.get('http')}) {str(head)[:120]}")
                if res["ok"]:
                    achados.append({"url": url, "call": call, "param": p, "sample": res.get("json")})
                    break
            if achados:
                break
        if achados:
            break
    # não “break” aqui: pode haver mais de um válido, mas o primeiro já resolve
# grava sugestão em arquivo para usar no sync
with open("omie_dfedocs_discovered.json", "w", encoding="utf-8") as f:
    json.dump(achados, f, ensure_ascii=False, indent=2)
print("\n==== RESUMO ====")
print(json.dumps(achados[:1], ensure_ascii=False, indent=2))
print("\nSe houver um item acima, use a URL/call encontrados com o script de sync.")
