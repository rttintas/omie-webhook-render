# ingest_omie_xml.py
import os
import json
import logging
from contextlib import contextmanager

import psycopg2
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PG_DSN = os.environ["PG_DSN"]
OMIE_APP_KEY = os.environ["OMIE_APP_KEY"]
OMIE_APP_SECRET = os.environ["OMIE_APP_SECRET"]

# Ajuste conforme seu ambiente/rota Omie:
NFE_LISTAR_DOCS_URL = "https://app.omie.com.br/api/v1/nfe/consultar/"
CALL_NAME = "ListarDocumentos"  # o método que retorna cXml na sua conta

@contextmanager
def get_conn():
    conn = psycopg2.connect(PG_DSN)
    try:
        yield conn
    finally:
        conn.close()

def omie_call(url, call, params):
    payload = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [params],
    }
    r = requests.post(url, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_xml_from_url(xml_url):
    r = requests.get(xml_url, timeout=60)
    if r.status_code == 200:
        return r.text
    raise requests.HTTPError(f"XML URL GET failed: {r.status_code}", response=r)

def fetch_xml_via_api(numero=None, serie=None, chave=None):
    """
    Tenta puxar o XML (cXml) pela API da Omie,
    primeiro por numero/série; se não vier, por chave.
    """
    # 1) por número/série
    if numero:
        params = {
            "pagina": 1,
            "registros_por_pagina": 1,
            "apenas_importados": "N",
            "nNumero": str(numero),
        }
        if serie:
            params["cSerie"] = str(serie)
        try:
            data = omie_call(NFE_LISTAR_DOCS_URL, CALL_NAME, params)
            arr = data.get("documentosEncontradosArray") or data.get("listaDocumentos") or []
            if arr:
                doc = arr[0]
                cxml = doc.get("cXml") or doc.get("xml") or ""
                if cxml:
                    return cxml
        except Exception as e:
            logging.warning("Fallback numero/serie falhou: %s", e)

    # 2) por chave
    if chave:
        params = {
            "pagina": 1,
            "registros_por_pagina": 1,
            "apenas_importados": "N",
            "nChave": str(chave),
        }
        try:
            data = omie_call(NFE_LISTAR_DOCS_URL, CALL_NAME, params)
            arr = data.get("documentosEncontradosArray") or data.get("listaDocumentos") or []
            if arr:
                doc = arr[0]
                cxml = doc.get("cXml") or doc.get("xml") or ""
                if cxml:
                    return cxml
        except Exception as e:
            logging.warning("Fallback por chave falhou: %s", e)

    return None

UPSERT_NFE = """
INSERT INTO public.omie_nfe
  (chave_nfe, numero, serie, emitida_em, cnpj_emitente, cnpj_destinatario,
   valor_total, status, xml, pdf_url, last_event_at)
VALUES
  (%(chave_nfe)s, %(numero)s, %(serie)s, %(emitida_em)s, %(cnpj_emitente)s,
   %(cnpj_destinatario)s, %(valor_total)s, %(status)s, %(xml)s, %(pdf_url)s, %(last_event_at)s)
ON CONFLICT (chave_nfe) DO UPDATE SET
  numero            = EXCLUDED.numero,
  serie             = EXCLUDED.serie,
  emitida_em        = EXCLUDED.emitida_em,
  cnpj_emitente     = EXCLUDED.cnpj_emitente,
  cnpj_destinatario = EXCLUDED.cnpj_destinatario,
  valor_total       = EXCLUDED.valor_total,
  status            = EXCLUDED.status,
  xml               = EXCLUDED.xml,
  pdf_url           = EXCLUDED.pdf_url,
  last_event_at     = EXCLUDED.last_event_at;
"""

def table_has_processed_column(cur):
    cur.execute("""
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name   = 'omie_webhook_events'
           AND column_name  = 'processed'
        LIMIT 1;
    """)
    return cur.fetchone() is not None

def select_webhooks(cur, limit, use_processed):
    if use_processed:
        cur.execute("""
            SELECT id,
                   event_ts,
                   (payload->'event'->>'nfe_xml') AS nfe_xml_url,
                   (payload->'event'->>'numero')   AS nf_numero,
                   (payload->'event'->>'serie')    AS nf_serie,
                   (payload->'event'->>'chave')    AS nf_chave,
                   payload
              FROM public.omie_webhook_events
             WHERE processed = false
               AND (payload->'event' ? 'nfe_xml')
             ORDER BY event_ts ASC
             LIMIT %s;
        """, (limit,))
    else:
        cur.execute("""
            SELECT id,
                   event_ts,
                   (payload->'event'->>'nfe_xml') AS nfe_xml_url,
                   (payload->'event'->>'numero')   AS nf_numero,
                   (payload->'event'->>'serie')    AS nf_serie,
                   (payload->'event'->>'chave')    AS nf_chave,
                   payload
              FROM public.omie_webhook_events
             WHERE (payload->'event' ? 'nfe_xml')
             ORDER BY event_ts ASC
             LIMIT %s;
        """, (limit,))
    return cur.fetchall()

def mark_processed(cur, web_id, use_processed):
    if use_processed:
        cur.execute(
            "UPDATE public.omie_webhook_events SET processed = true, processed_at = now() WHERE id = %s;",
            (web_id,)
        )

def process_pending(limit=100):
    with get_conn() as conn:
        cur = conn.cursor()
        use_processed = table_has_processed_column(cur)

        rows = select_webhooks(cur, limit, use_processed)
        logging.info("Encontrados %d webhooks com nfe_xml", len(rows))

        for (web_id, event_ts, xml_url, nf_numero, nf_serie, nf_chave, payload) in rows:
            try:
                xml_text = None
                try:
                    xml_text = fetch_xml_from_url(xml_url)
                except requests.HTTPError as e:
                    code = e.response.status_code if e.response is not None else None
                    if code in (403, 404):
                        logging.warning("Link XML retornou %s. Tentando via API...", code)
                        xml_text = fetch_xml_via_api(numero=nf_numero, serie=nf_serie, chave=nf_chave)
                    else:
                        raise

                if not xml_text:
                    raise RuntimeError("Não foi possível obter o XML (link e API falharam).")

                # Se quiser enriquecer mais com dados do payload, preencha abaixo.
                row = {
                    "chave_nfe":         nf_chave,
                    "numero":            nf_numero,
                    "serie":             nf_serie,
                    "emitida_em":        None,
                    "cnpj_emitente":     None,
                    "cnpj_destinatario": None,
                    "valor_total":       None,
                    "status":            None,
                    "xml":               xml_text,
                    "pdf_url":           None,
                    "last_event_at":     event_ts,
                }

                cur.execute(UPSERT_NFE, row)
                mark_processed(cur, web_id, use_processed)
                conn.commit()
                logging.info("[OK] webhook %s processado.", web_id)

            except Exception as e:
                conn.rollback()
                logging.error("ERRO no webhook %s: %s", web_id, e)

def main():
    logging.info("Ingest de XML iniciado.")
    process_pending(limit=100)

if __name__ == "__main__":
    main()
