# -*- coding: utf-8 -*-
"""
Sincroniza DF-e / NFe (XML/PDF) da Omie para o Postgres.
- Lê chaves em omie.omie_nfe
- Chama a API Omie (endpoint/call configuráveis com --url/--call ou tentativas padrão)
- Faz UPSERT em omie.omie_nfe_docs

Requisitos:
  pip install requests psycopg2-binary

Ambiente:
  set OMIE_APP_KEY=...
  set OMIE_APP_SECRET=...
  set PG_DSN=host=localhost port=5432 dbname=... user=... password=...

Uso:
  # apenas faltantes (sem xml ainda)
  python -u sync_dfedocs_obternfe_por_banco.py --somente-faltantes --limit 50 --debug

  # por período de emissão
  python -u sync_dfedocs_obternfe_por_banco.py --desde 2025-08-01 --ate 2025-08-28 --limit 500

  # forçando endpoint/método (depois que você souber quais funcionam na sua conta)
  python -u sync_dfedocs_obternfe_por_banco.py --somente-faltantes --url "https://app.omie.com.br/api/v1/produtos/nfe/" --call "ObterNFe" --limit 50 --debug
"""
import os
import sys
import json
import argparse
from datetime import date, datetime
import requests
import psycopg2
from psycopg2.extras import DictCursor

APP_KEY = os.getenv("OMIE_APP_KEY")
APP_SECRET = os.getenv("OMIE_APP_SECRET")
PG_DSN = os.getenv("PG_DSN")

def log(msg):
    print(msg, flush=True)

def dlog(enabled, *parts):
    if enabled:
        print("[DEBUG]", *parts, flush=True)

# ---------- Montagem dos parâmetros para cada call ----------
def p_dfedocs_obter(chave, emissao, numero=None):
    return {"cNumNFe": str(numero or ""), "nChaveNFe": chave, "dDataEmisNFe": str(emissao or "")}

def p_dfedocs_listar_por_chave(chave, emissao, numero=None):
    # alguns tenants têm Listar com esse shape
    return {"nChaveNFe": chave, "filtrar_por": {"nChaveNFe": chave}, "pagina": 1, "registros_por_pagina": 50}

def p_nfe_obterNFe(chave, emissao, numero=None):
    return {"cNumNFe": str(numero or ""), "nChaveNFe": chave, "dDataEmisNFe": str(emissao or "")}

def p_nfe_obterNF(chave, emissao, numero=None):
    return {"cNumNF": str(numero or ""), "nChaveNF": chave, "dDataEmisNF": str(emissao or "")}

def p_nfe_obter(chave, emissao, numero=None):
    return {
        "cNumNFe": str(numero or ""),
        "cNumNF": str(numero or ""),
        "nChaveNFe": chave,
        "nChaveNF": chave,
        "dDataEmisNFe": str(emissao or ""),
        "dDataEmisNF": str(emissao or ""),
    }

# ---------- Heurística para extrair dados do retorno ----------
def pick(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, ""):
            return d[k]
    return default

def parse_payload(data):
    """
    Tenta localizar XML/PDF/link/status em estruturas comuns dos retornos Omie.
    Retorna dict com campos normalizados (podem vir None).
    """
    # Muitos endpoints retornam algo dentro de "documento", "dfedoc", "nf", etc.
    candidates = []
    if isinstance(data, dict):
        candidates.append(data)
        for v in data.values():
            if isinstance(v, dict):
                candidates.append(v)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                candidates.extend(v)

    out = {
        "xml": None,
        "pdf": None,
        "link": None,
        "cod_status": None,
        "des_status": None,
    }

    for obj in candidates:
        xml = pick(obj, "xml", "cXmlNFe", "cXml", "cXmlNF")
        pdf = pick(obj, "pdf", "cPdf", "cPDF")
        link = pick(obj, "cLinkPortal", "link_portal", "link", "url")
        cod = pick(obj, "cod_status", "cCodStatus", "codigo_status", "codigoStatus")
        des = pick(obj, "des_status", "cDesStatus", "descricao_status", "descricaoStatus", "mensagem")
        if any([xml, pdf, link, cod, des]):
            out["xml"] = xml or out["xml"]
            out["pdf"] = pdf or out["pdf"]
            out["link"] = link or out["link"]
            out["cod_status"] = (str(cod) if cod is not None else out["cod_status"])
            out["des_status"] = (str(des) if des is not None else out["des_status"])
    return out

# ---------- DB ----------
def get_conn():
    if not PG_DSN:
        log("[ERRO] PG_DSN não configurado.")
        sys.exit(2)
    return psycopg2.connect(PG_DSN)

def listar_chaves(conn, somente_faltantes, limite, d_de=None, d_ate=None, debug=False):
    cur = conn.cursor(cursor_factory=DictCursor)
    if somente_faltantes:
        sql = """
        SELECT
            n.numero_nfe,
            n.chave_nfe,
            n.data_emissao
        FROM omie.omie_nfe n
        LEFT JOIN omie.omie_nfe_docs d ON d.chave_nfe = n.chave_nfe
        WHERE n.chave_nfe IS NOT NULL
          AND (d.chave_nfe IS NULL OR d.xml IS NULL OR d.xml = '')
        ORDER BY n.data_emissao DESC NULLS LAST
        LIMIT %s
        """
        params = [limite]
    else:
        sql = """
        SELECT
            n.numero_nfe,
            n.chave_nfe,
            n.data_emissao
        FROM omie.omie_nfe n
        WHERE n.chave_nfe IS NOT NULL
          AND n.data_emissao >= %s
          AND n.data_emissao <= %s
        ORDER BY n.data_emissao DESC NULLS LAST
        LIMIT %s
        """
        params = [d_de, d_ate, limite]

    dlog(debug, "SQL listar_chaves:\n", sql, params)
    cur.execute(sql, params)
    return cur.fetchall()

def upsert_doc(conn, chave_nfe, dados, debug=False):
    cur = conn.cursor()
    sql = """
    INSERT INTO omie.omie_nfe_docs
        (chave_nfe, xml, pdf, link_portal, cod_status, des_status, fetched_at)
    VALUES
        (%s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (chave_nfe) DO UPDATE SET
        xml         = EXCLUDED.xml,
        pdf         = EXCLUDED.pdf,
        link_portal = EXCLUDED.link_portal,
        cod_status  = EXCLUDED.cod_status,
        des_status  = EXCLUDED.des_status,
        fetched_at  = NOW();
    """
    params = [
        chave_nfe,
        dados.get("xml"),
        dados.get("pdf"),
        dados.get("link"),
        dados.get("cod_status"),
        dados.get("des_status"),
    ]
    dlog(debug, "UPSERT", params)
    cur.execute(sql, params)

# ---------- Omie ----------
def tentar_um(url, call, param, debug=False):
    body = {
        "call": call,
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [param],
    }
    if debug:
        short = json.dumps(body, ensure_ascii=False)
        if len(short) > 500:
            short = short[:500] + "... (trunc)"
        dlog(True, f"POST {url} call={call} body={short}")
    r = requests.post(url, json=body, timeout=60)
    ok_http = 200 <= r.status_code < 300
    try:
        data = r.json()
    except Exception:
        data = {"_raw": r.text}

    if debug:
        dlog(True, "HTTP", r.status_code, (json.dumps(data, ensure_ascii=False)[:1000]))

    # Heurística de “sucesso útil”: presença de campos de retorno comuns
    payload = json.dumps(data, ensure_ascii=False)
    has_doc = any(k in payload for k in ["xml", "cXmlNFe", "cPdf", "cLinkPortal", "cod_status", "cCodStatus"])
    return ok_http and has_doc, data, r.status_code

def obter_documento(numero, chave, emissao, args):
    """
    Devolve (ok, data, url_usada, call_usado, http_status)
    """
    # Se o usuário informou override, tenta somente aquilo
    attempts = []
    if args.url and args.call:
        def p_def(ch, em, num):
            return {
                "cNumNFe": str(num or ""),
                "cNumNF": str(num or ""),
                "nChaveNFe": ch,
                "nChaveNF": ch,
                "dDataEmisNFe": str(em or ""),
                "dDataEmisNF": str(em or ""),
            }
        attempts = [(args.url, args.call, p_def)]
    else:
        # Tentativas padrão (alguns tenants expõem por dfedocs, outros por nfe)
        attempts = [
            ("https://app.omie.com.br/api/v1/produtos/dfedocs/", "Obter",    p_dfedocs_obter),
            ("https://app.omie.com.br/api/v1/produtos/dfedocs/", "Listar",   p_dfedocs_listar_por_chave),
            ("https://app.omie.com.br/api/v1/produtos/nfe/",     "ObterNFe", p_nfe_obterNFe),
            ("https://app.omie.com.br/api/v1/produtos/nfe/",     "ObterNF",  p_nfe_obterNF),
            ("https://app.omie.com.br/api/v1/produtos/nfe/",     "Obter",    p_nfe_obter),
        ]

    erros = []
    for url, call, fnp in attempts:
        ok, data, http = tentar_um(url, call, fnp(chave, emissao, numero), args.debug)
        if ok:
            return True, data, url, call, http
        # junta mensagem de erro
        msg = ""
        try:
            if isinstance(data, dict):
                msg = data.get("message") or data.get("faultstring") or json.dumps(data, ensure_ascii=False)[:200]
            else:
                msg = str(data)[:200]
        except Exception:
            msg = f"HTTP {http}"
        erros.append(f"{url} {call}: HTTP {http}: {msg}")
    return False, {"errors": erros}, None, None, None

# ---------- Main ----------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", help="YYYY-MM-DD")
    ap.add_argument("--ate", help="YYYY-MM-DD")
    ap.add_argument("--somente-faltantes", action="store_true")
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--debug", action="store_true")
    # overrides:
    ap.add_argument("--url", help="Override da URL da API (ex.: https://app.omie.com.br/api/v1/produtos/nfe/)")
    ap.add_argument("--call", help="Override do método/call (ex.: ObterNFe, Obter, Listar)")
    return ap.parse_args()

def main():
    if not APP_KEY or not APP_SECRET:
        log("[ERRO] OMIE_APP_KEY/OMIE_APP_SECRET não configurados.")
        sys.exit(2)

    args = parse_args()
    d_de = args.desde
    d_ate = args.ate

    with get_conn() as conn:
        # Coleta chaves
        if args.somente_faltantes:
            log(f"[INFO] Iniciando sync via ObterNFe | desde: None | ate: None | somente_faltantes: {args.somente_faltantes} | limit: {args.limit}")
            chaves = listar_chaves(conn, True, args.limit, debug=args.debug)
        else:
            if not (d_de and d_ate):
                log("[ERRO] Para rodar por período, informe --desde e --ate (YYYY-MM-DD).")
                sys.exit(2)
            log(f"[INFO] Iniciando sync via ObterNFe | desde: {d_de} | ate: {d_ate} | somente_faltantes: {args.somente_faltantes} | limit: {args.limit}")
            chaves = listar_chaves(conn, False, args.limit, d_de, d_ate, debug=args.debug)

        log(f"[INFO] Total de chaves a processar: {len(chaves)}")

        ok_total = 0
        fail_total = 0

        for idx, row in enumerate(chaves, start=1):
            numero = row["numero_nfe"]
            chave = row["chave_nfe"]
            emissao = row["data_emissao"]
            log(f"[{idx}/{len(chaves)}] chave={chave} | num={numero} | emissao={emissao}")

            ok, data, url, call, http = obter_documento(numero, chave, emissao, args)
            if not ok:
                # junta mensagens
                errs = data.get("errors") if isinstance(data, dict) else [str(data)]
                log("FAIL -> " + " | ".join(errs))
                fail_total += 1
                continue

            # normaliza campos
            doc = parse_payload(data)
            upsert_doc(conn, chave, doc, debug=args.debug)
            ok_total += 1

        log(f"[FIM] OK: {ok_total} | Falhas: {fail_total}")

if __name__ == "__main__":
    main()
