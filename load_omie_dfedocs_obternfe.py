# load_omie_dfedocs_obternfe.py
import os, sys, csv, json, time
from datetime import datetime
from typing import Iterable, List
import requests
import psycopg2
from psycopg2.extras import execute_values

OMIE_URL = "https://app.omie.com.br/api/v1/produtos/dfedocs/"

APP_KEY = os.getenv("OMIE_APP_KEY")
APP_SECRET = os.getenv("OMIE_APP_SECRET")
PG_DSN = os.getenv("PG_DSN")

if not APP_KEY or not APP_SECRET or not PG_DSN:
    print("[ERRO] Defina OMIE_APP_KEY, OMIE_APP_SECRET e PG_DSN nas variáveis de ambiente.")
    sys.exit(1)

def load_ids_from_csv(path: str) -> List[int]:
    ids = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        if "id" not in r.fieldnames:
            raise RuntimeError(f"CSV '{path}' precisa ter a coluna 'id'")
        for row in r:
            v = row["id"].strip()
            if v:
                ids.append(int(v))
    return ids

def load_ids_from_args(arg: str) -> List[int]:
    return [int(x.strip()) for x in arg.split(",") if x.strip()]

def omie_obter_nfe(nidnfe: int) -> dict:
    payload = {
        "call": "ObterNfe",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [{"nIdNFe": nidnfe}],
    }
    r = requests.post(OMIE_URL, json=payload, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} OMIE")
    data = r.json()
    if isinstance(data, dict) and data.get("status") == "error":
        # erro "bonitinho" da OMIE
        raise RuntimeError(f"OMIE: {data.get('message')}")
    return data  # deve conter campos do ObterNFeResponse

def upsert_omie_nfe(conn, rows: Iterable[dict]):
    """
    rows: cada item contendo as colunas mapeadas para omie.omie_nfe
    """
    sql = """
    INSERT INTO omie.omie_nfe
      (chave_nfe, numero_nfe, data_emissao, danfe_url, xml_distr, portal_link, cod_status, des_status)
    VALUES %s
    ON CONFLICT (chave_nfe) DO UPDATE SET
       numero_nfe = EXCLUDED.numero_nfe,
       data_emissao = EXCLUDED.data_emissao,
       danfe_url = EXCLUDED.danfe_url,
       xml_distr = EXCLUDED.xml_distr,
       portal_link = EXCLUDED.portal_link,
       cod_status = EXCLUDED.cod_status,
       des_status = EXCLUDED.des_status;
    """
    values = []
    for r in rows:
        values.append((
            r["chave_nfe"],
            r["numero_nfe"],
            r["data_emissao"],
            r["danfe_url"],
            r["xml_distr"],
            r["portal_link"],
            r["cod_status"],
            r["des_status"],
        ))
    with conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=100)

def main():
    # parse argumentos simples
    args = sys.argv[1:]
    csv_path = None
    ids_arg = None
    if "--csv" in args:
        csv_path = args[args.index("--csv")+1]
    if "--ids" in args:
        ids_arg = args[args.index("--ids")+1]

    if not csv_path and not ids_arg:
        print("Uso:")
        print("  python -u load_omie_dfedocs_obternfe.py --csv ids.csv")
        print("  ou")
        print("  python -u load_omie_dfedocs_obternfe.py --ids 11111,22222")
        sys.exit(2)

    ids = []
    if csv_path:
        ids = load_ids_from_csv(csv_path)
    if ids_arg:
        ids += load_ids_from_args(ids_arg)

    print(f"[INFO] Total de IDs (nIdNFe): {len(ids)}")
    ok, falhas = 0, 0
    buffer = []

    conn = psycopg2.connect(PG_DSN)

    for i, nid in enumerate(ids, 1):
        try:
            data = omie_obter_nfe(nid)
            # mapeamento dos campos (adequar se sua conta usa outros nomes):
            numero_nfe   = (data.get("cNumNfe") or "").strip() or None
            chave_nfe    = (data.get("nChaveNfe") or "").strip() or None
            data_emissao = (data.get("dDataEmisNfe") or "").strip() or None
            xml          = data.get("cXmlNfe") or ""
            pdf_link     = (data.get("cPdf") or "").strip() or None
            portal_link  = (data.get("cLinkPortal") or "").strip() or None
            cod_status   = (data.get("cCodStatus") or "").strip() or None
            des_status   = (data.get("cDesStatus") or "").strip() or None

            if not chave_nfe:
                raise RuntimeError("Sem nChaveNfe no retorno.")

            row = {
                "chave_nfe":   chave_nfe,
                "numero_nfe":  numero_nfe,
                "data_emissao": data_emissao,  # Postgres converte 'YYYY-MM-DD' automaticamente para DATE
                "danfe_url":   pdf_link,
                "xml_distr":   xml,
                "portal_link": portal_link,
                "cod_status":  cod_status,
                "des_status":  des_status,
            }
            buffer.append(row)
            ok += 1

            if len(buffer) >= 100:
                upsert_omie_nfe(conn, buffer)
                buffer.clear()

            if i % 50 == 0:
                print(f"[INFO] Processados {i}/{len(ids)}")

        except Exception as e:
            falhas += 1
            print(f"[WARN] nIdNFe={nid}: {e}")
            time.sleep(0.3)  # evita “martelar” a API em casos de falha

    if buffer:
        upsert_omie_nfe(conn, buffer)

    conn.close()
    print(f"[FIM] OK: {ok} | Falhas: {falhas}")

if __name__ == "__main__":
    main()
