# -*- coding: utf-8 -*-
import os, sys, csv, argparse, datetime, json
import requests, psycopg2
from psycopg2.extras import execute_values

OMIE_URL = "https://app.omie.com.br/api/v1/produtos/dfedocs/"

def omie_obternfe_por_chave(chave, app_key, app_secret):
    body = {
        "call": "ObterNfe",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [{"nChaveNfe": chave}],
    }
    r = requests.post(OMIE_URL, json=body, timeout=60)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"status_http": r.status_code, "text": r.text}

def upsert(conn, rows):
    sql = """
    INSERT INTO omie.omie_nfe (
      chave_nfe, numero_nfe, data_emissao, xml_distr,
      danfe_url, link_portal, cod_status, des_status, fetched_at
    )
    VALUES %s
    ON CONFLICT (chave_nfe) DO UPDATE SET
      numero_nfe = EXCLUDED.numero_nfe,
      data_emissao = EXCLUDED.data_emissao,
      xml_distr   = EXCLUDED.xml_distr,
      danfe_url   = EXCLUDED.danfe_url,
      link_portal = EXCLUDED.link_portal,
      cod_status  = EXCLUDED.cod_status,
      des_status  = EXCLUDED.des_status,
      fetched_at  = EXCLUDED.fetched_at;
    """
    tuples = [(
        r["chave_nfe"], r["numero_nfe"], r["data_emissao"], r["xml_distr"],
        r["danfe_url"], r["link_portal"], r["cod_status"], r["des_status"], r["fetched_at"]
    ) for r in rows]
    with conn, conn.cursor() as cur:
        execute_values(cur, sql, tuples)

def ler_csv(caminho):
    chaves = []
    with open(caminho, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            v = (row.get("chave") or row.get("nChaveNfe") or "").strip()
            if v:
                chaves.append(v)
    return chaves

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chaves", help="Lista separada por vírgula com chaves (44 dígitos)")
    ap.add_argument("--csv", help="CSV com coluna 'chave' ou 'nChaveNfe'")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    app_key = os.getenv("OMIE_APP_KEY")
    app_secret = os.getenv("OMIE_APP_SECRET")
    dsn = os.getenv("PG_DSN")
    if not (app_key and app_secret and dsn):
        print("[ERRO] Defina OMIE_APP_KEY, OMIE_APP_SECRET e PG_DSN.")
        sys.exit(2)

    # monta lista de chaves
    chaves = []
    if args.chaves:
        chaves = [x.strip() for x in args.chaves.split(",") if x.strip()]
    elif args.csv:
        chaves = ler_csv(args.csv)
    else:
        print("Use --chaves 44d,44d... ou --csv chaves.csv")
        sys.exit(1)

    conn = psycopg2.connect(dsn)
    ok, falha = 0, 0
    batch = []
    agora = datetime.datetime.now()

    print(f"[INFO] Total de chaves: {len(chaves)}")
    for ch in chaves:
        if len(ch) != 44 or not ch.isdigit():
            print(f"[WARN] Chave inválida: {ch!r}")
            falha += 1
            continue

        status, data = omie_obternfe_por_chave(ch, app_key, app_secret)
        if isinstance(data, dict) and (data.get("nChaveNfe") or data.get("cXmlNfe") or data.get("cNumNfe")):
            row = {
                "chave_nfe":  data.get("nChaveNfe") or ch,
                "numero_nfe": data.get("cNumNfe"),
                "data_emissao": data.get("dDataEmisNfe"),
                "xml_distr":  data.get("cXmlNfe"),
                "danfe_url":  data.get("cPdf"),
                "link_portal": data.get("cLinkPortal"),
                "cod_status":  data.get("cCodStatus"),
                "des_status":  data.get("cDesStatus"),
                "fetched_at":  agora,
            }
            batch.append(row)
            print(f"[OK] {ch}  status={row['cod_status']}  num={row['numero_nfe']}")
            ok += 1
        else:
            print(f"[FAIL] {ch} -> HTTP={status} retorno={json.dumps(data, ensure_ascii=False)[:300]}")
            falha += 1

    if batch and not args.dry_run:
        upsert(conn, batch)
    conn.close()
    print(f"[FIM] OK: {ok} | Falhas: {falha}")

if __name__ == "__main__":
    main()
