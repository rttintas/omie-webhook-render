# sync_xml_listardocumentos.py
import os, sys, argparse, time, json, base64
from datetime import datetime, date
import requests
import psycopg2
import psycopg2.extras as extras

ENDPOINT = "https://app.omie.com.br/api/v1/contador/xml/"

APP_KEY = os.getenv("OMIE_APP_KEY")
APP_SECRET = os.getenv("OMIE_APP_SECRET")
PG_DSN = os.getenv("PG_DSN")

if not APP_KEY or not APP_SECRET:
    print("[ERRO] Defina OMIE_APP_KEY e OMIE_APP_SECRET.")
    sys.exit(1)
if not PG_DSN:
    print("[ERRO] Defina PG_DSN.")
    sys.exit(1)

def req_json(call: str, params: dict):
    body = {
        "call": call,
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [params],
    }
    r = requests.post(ENDPOINT, json=body, timeout=60)
    # Omie às vezes responde 200 com erro em JSON; checamos ambos
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"HTTP {r.status_code} sem JSON: {r.text[:500]}")
    if r.status_code != 200 or (isinstance(data, dict) and data.get("status")=="error"):
        raise RuntimeError(f"Falha {call}: HTTP {r.status_code} | {data}")
    return data

def conectar_pg():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    return conn

UPSERT_SQL = """
INSERT INTO omie.omie_nfe_docs (
    chave_nfe, xml, cod_status, des_status, fetched_at, n_id_nf, url_danfe
) VALUES (%(chave_nfe)s, %(xml)s, %(cod_status)s, %(des_status)s, NOW(), %(n_id_nf)s, %(url_danfe)s)
ON CONFLICT (chave_nfe) DO UPDATE SET
    xml = EXCLUDED.xml,
    cod_status = EXCLUDED.cod_status,
    des_status = EXCLUDED.des_status,
    fetched_at = NOW(),
    n_id_nf = COALESCE(EXCLUDED.n_id_nf, omie_nfe_docs.n_id_nf),
    url_danfe = COALESCE(EXCLUDED.url_danfe, omie_nfe_docs.url_danfe)
"""

SQL_LISTAR_FALTANTES = """
SELECT n.chave_nfe, n.data_emissao
FROM omie.omie_nfe n
LEFT JOIN omie.omie_nfe_docs d ON d.chave_nfe = n.chave_nfe
WHERE n.chave_nfe IS NOT NULL
  AND (d.chave_nfe IS NULL OR d.xml IS NULL OR d.xml = '')
ORDER BY n.data_emissao DESC NULLS LAST
LIMIT %s
"""

def listar_documentos_periodo(d_ini: str, d_fim: str, limit_por_pagina=500):
    pagina = 1
    total = 0
    while True:
        params = {
            "pagina": pagina,
            "regPorPagina": limit_por_pagina,
            "filtrar_por": {
                "dEmiInicial": d_ini,
                "dEmiFinal": d_fim,
            }
        }
        data = req_json("ListarDocumentos", params)
        itens = data.get("documentosEncontrados") or data.get("docsEncontrados") or []
        if not itens:
            break
        for it in itens:
            yield it
        total += len(itens)
        # parada de paginação
        if len(itens) < limit_por_pagina:
            break
        pagina += 1

def extrair_campos(item):
    # Campos podem variar levemente de nome; cobrimos os prováveis
    chave = item.get("cChaveNFe") or item.get("chaveNFe") or item.get("cChaveNF") or ""
    xml = item.get("cXml") or item.get("xml") or ""
    n_id = item.get("nIdNFe") or item.get("nIdNF") or item.get("nCodNF")
    # Alguns retornos trazem XML base64; se for o caso, tentamos decodificar
    if xml and xml.strip().startswith("PD94bWwg"):  # "<?xml" em base64
        try:
            xml = base64.b64decode(xml).decode("utf-8", "replace")
        except Exception:
            pass
    return chave, xml, n_id

def get_url_danfe(n_id_nf):
    if not n_id_nf:
        return None
    try:
        data = req_json("GetUrlDanfe", {"nCodNF": int(n_id_nf)})
        return data.get("urlDanfe") or data.get("cUrlDanfe")
    except Exception:
        return None

def salvar(conn, row):
    with conn.cursor() as cur:
        extras.execute_batch(cur, UPSERT_SQL, [row], page_size=1)

def por_periodo(conn, d_ini, d_fim, pegar_danfe):
    ok = falhas = 0
    for item in listar_documentos_periodo(d_ini, d_fim):
        try:
            chave, xml, n_id = extrair_campos(item)
            if not chave or not xml:
                raise RuntimeError(f"Sem chave ou xml no item: {list(item.keys())}")
            url_danfe = get_url_danfe(n_id) if pegar_danfe else None
            row = {
                "chave_nfe": chave,
                "xml": xml,
                "cod_status": 200,
                "des_status": "OK",
                "n_id_nf": n_id,
                "url_danfe": url_danfe,
            }
            salvar(conn, row)
            ok += 1
            print(f"[OK] chave={chave} | n_id={n_id} | danfe={'sim' if url_danfe else 'não'}")
        except Exception as e:
            falhas += 1
            print(f"[ERRO] {e}")
    print(f"[FIM] OK: {ok} | Falhas: {falhas}")

def faltantes(conn, limit, pegar_danfe):
    ok = falhas = 0
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
        cur.execute(SQL_LISTAR_FALTANTES, (limit,))
        chaves = cur.fetchall()
    if not chaves:
        print("[INFO] Nada faltando.")
        return
    # Monta um intervalo largo de datas para reduzir chamadas
    d_ini = "2000-01-01"
    d_fim = date.today().isoformat()
    # Criamos um índice auxiliar por chave
    pendentes = {x["chave_nfe"]: x for x in chaves}
    for item in listar_documentos_periodo(d_ini, d_fim):
        try:
            chave, xml, n_id = extrair_campos(item)
            if not chave or chave not in pendentes:
                continue
            if not xml:
                continue
            url_danfe = get_url_danfe(n_id) if pegar_danfe else None
            row = {
                "chave_nfe": chave,
                "xml": xml,
                "cod_status": 200,
                "des_status": "OK",
                "n_id_nf": n_id,
                "url_danfe": url_danfe,
            }
            salvar(conn, row)
            ok += 1
            del pendentes[chave]
            print(f"[OK] chave={chave} | n_id={n_id} | danfe={'sim' if url_danfe else 'não'}")
            if ok >= limit:
                break
        except Exception as e:
            falhas += 1
            print(f"[ERRO] {e}")
    # As que sobraram não apareceram no período amplo (raríssimo)
    if pendentes:
        print(f"[WARN] {len(pendentes)} chaves não apareceram no ListarDocumentos.")
    print(f"[FIM] OK: {ok} | Falhas: {falhas}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", help="YYYY-MM-DD")
    ap.add_argument("--ate", help="YYYY-MM-DD")
    ap.add_argument("--somente-faltantes", action="store_true")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--pegar-danfe", action="store_true")
    args = ap.parse_args()

    print(f"[INFO] Fonte: ListarDocumentos | desde={args.desde} | ate={args.ate} | faltantes={args.somente_faltantes} | limit={args.limit} | danfe={args.pegar_danfe}")

    conn = conectar_pg()
    try:
        if args.somente_faltantes:
            faltantes(conn, args.limit, args.pegar_danfe)
        else:
            if not args.desde or not args.ate:
                print("[ERRO] Informe --desde e --ate ou use --somente-faltantes.")
                sys.exit(2)
            por_periodo(conn, args.desde, args.ate, args.pegar_danfe)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
