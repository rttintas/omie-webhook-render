# salvar como fetch_nfe_xml.py (ou incorporar no worker)

import os, time, logging, json
import requests
import psycopg2
from contextlib import contextmanager
from datetime import datetime, timedelta, date

APP_KEY = os.getenv("OMIE_APP_KEY")
APP_SECRET = os.getenv("OMIE_APP_SECRET")
PG_DSN = os.getenv("PG_DSN")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s")

API_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"  # mesma raiz; o método é passado no body
DOC_URL = "https://app.omie.com.br/api/v1/produtos/notasfiscais/"  # ajuste de rota conforme seu contrato

# Omie usa POST com {"call": "...", "app_key": "...", "app_secret": "...", "params":[{...}]}

def omie_call(call: str, params: dict, url: str = DOC_URL):
    payload = {
        "call": call,
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "params": [params],
    }
    for attempt in range(5):
        resp = requests.post(url, json=payload, timeout=60)
        if resp.status_code >= 500:
            time.sleep(1.5 * (attempt+1))
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()  # levanta o último erro


@contextmanager
def get_conn():
    conn = psycopg2.connect(PG_DSN, sslmode="require")
    try:
        yield conn
    finally:
        conn.close()

UPSERT_NFE = """
INSERT INTO public.omie_nfe
  (chave_nfe, numero, serie, emitida_em, valor_total, xml, raw_json)
VALUES
  (%(chave_nfe)s, %(numero)s, %(serie)s, %(emitida_em)s, %(valor_total)s, %(xml)s, %(raw_json)s)
ON CONFLICT (chave_nfe) DO UPDATE SET
  numero      = EXCLUDED.numero,
  serie       = EXCLUDED.serie,
  emitida_em  = EXCLUDED.emitida_em,
  valor_total = EXCLUDED.valor_total,
  xml         = EXCLUDED.xml,
  raw_json    = EXCLUDED.raw_json,
  updated_at  = now();
"""

def salvar_nfe(cur, doc):
    # Campos típicos do retorno (confirme os nomes da sua rota; muitos retornam exatamente estes):
    # nChave, nNumero, cSerie, dEmissao, nValor, cXml
    row = {
        "chave_nfe": doc.get("nChave"),
        "numero":    str(doc.get("nNumero") or ""),
        "serie":     str(doc.get("cSerie")  or ""),
        "emitida_em": None,
        "valor_total": None,
        "xml":       doc.get("cXml"),
        "raw_json":  json.dumps(doc, ensure_ascii=False)
    }
    # dEmissao pode vir "YYYY-MM-DD" ou "DD/MM/YYYY" dependendo do método
    d = doc.get("dEmissao")
    if d:
        try:
            row["emitida_em"] = datetime.fromisoformat(d)
        except Exception:
            try:
                row["emitida_em"] = datetime.strptime(d, "%d/%m/%Y")
            except Exception:
                pass
    if doc.get("nValor") is not None:
        try:
            row["valor_total"] = float(str(doc["nValor"]).replace(",", "."))
        except Exception:
            pass

    cur.execute(UPSERT_NFE, row)


def listar_xml_por_periodo(dt_ini: date, dt_fim: date, pagina: int = 1, por_pagina: int = 50):
    """
    Chama o método que retorna lista de documentos com cXml (ex.: 'ListarDocumentos')
    Filtros típicos: data de emissao inicial/final
    """
    params = {
        "pagina": pagina,
        "registros_por_pagina": por_pagina,
        "ordenar_por": "DATA_EMISSAO",
        "ordem_decrescente": "N",
        "filtrar_por_data_de": dt_ini.strftime("%d/%m/%Y"),
        "filtrar_por_data_ate": dt_fim.strftime("%d/%m/%Y"),
        # inclua filtros adicionais se quiser (ex.: modelo=55, apenas NF-e)
    }
    return omie_call("ListarDocumentos", params)  # ajuste o 'call' e 'url' caso sua conta use outro módulo


def backfill_xml(quantos_dias: int = 30):
    dt_fim = date.today()
    dt_ini = dt_fim - timedelta(days=quantos_dias)

    total_gravadas = 0
    pagina = 1
    with get_conn() as conn, conn.cursor() as cur:
        while True:
            data = listar_xml_por_periodo(dt_ini, dt_fim, pagina=pagina)
            docs = (data.get("documentos") or
                    data.get("documentosEncontrados") or
                    data.get("documentosEncontradosArray") or [])
            if not docs:
                break

            for doc in docs:
                if not doc.get("cXml"):
                    # às vezes o método permite retornar sem o corpo do XML – ignore ou outra chamada de detalhe
                    continue
                salvar_nfe(cur, doc)
                total_gravadas += 1

            conn.commit()
            logging.info("Página %s gravada, total até agora: %s", pagina, total_gravadas)

            # paginação
            proxima = data.get("pagina_proxima") or data.get("paginaSeguinte")
            if not proxima:
                break
            pagina = int(proxima)

    logging.info("Backfill concluído. XMLs gravados/atualizados: %s", total_gravadas)


if __name__ == "__main__":
    if not (APP_KEY and APP_SECRET and PG_DSN):
        raise SystemExit("Configure OMIE_APP_KEY, OMIE_APP_SECRET e PG_DSN no ambiente.")
    backfill_xml(quantos_dias=120)  # rode uma primeira carga (ajuste a janela)
