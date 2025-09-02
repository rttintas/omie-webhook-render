# ingest_nf_xml.py
import os
import re
import requests
import psycopg2
import psycopg2.extras
import xml.etree.ElementTree as ET
from dateutil import parser as dtp

PG_DSN = os.environ.get("PG_DSN") or "dbname=neondb user=public password=SUASENHA host=SEU_HOST port=5432"

# --- Helpers XML -------------------------------------------------------------

NS = {
    'nfe': 'http://www.portalfiscal.inf.br/nfe'
}
# Muitos XMLs vêm sem prefixo; tentamos sem namespace também
def find_one(el, xpath):
    node = el.find(xpath, NS) or el.find(xpath)
    return node.text.strip() if (node is not None and node.text is not None) else None

def iter_many(el, xpath):
    nodes = el.findall(xpath, NS)
    return nodes if nodes else el.findall(xpath)

def parse_nfe_xml(xml_bytes):
    """
    Retorna dicionário com cabeçalho e lista de itens.
    Aceita procNFe e NFe "puro".
    """
    root = ET.fromstring(xml_bytes)

    # Suporta <nfeProc><NFe>…</NFe></nfeProc> e <NFe> direto
    nfe_el = root.find('.//nfe:NFe', NS) or root.find('.//NFe')
    if nfe_el is None:
        raise ValueError("NFe não encontrada no XML")

    ide = nfe_el.find('.//nfe:ide', NS) or nfe_el.find('.//ide')
    emit = nfe_el.find('.//nfe:emit', NS) or nfe_el.find('.//emit')
    dest = nfe_el.find('.//nfe:dest', NS) or nfe_el.find('.//dest')
    total = nfe_el.find('.//nfe:ICMSTot', NS) or nfe_el.find('.//ICMSTot')

    # Chave da NFe (infNFe Id="NFe3519..."), remover o prefixo "NFe"
    infNFe = nfe_el.find('.//nfe:infNFe', NS) or nfe_el.find('.//infNFe')
    chave = None
    if infNFe is not None and 'Id' in infNFe.attrib:
        chave = re.sub(r'^NFe', '', infNFe.attrib['Id'])

    cab = {
        'nf_numero'    : find_one(ide, './/nfe:nNF') or find_one(ide, './/nNF'),
        'nf_serie'     : find_one(ide, './/nfe:serie') or find_one(ide, './/serie'),
        'chave_nfe'    : chave,
        'emitida_em'   : find_one(ide, './/nfe:dhEmi') or find_one(ide, './/dhEmi') or find_one(ide, './/dEmi'),
        'cnpj_emitente': find_one(emit, './/nfe:CNPJ') or find_one(emit, './/CNPJ'),
        'razao_emitente': find_one(emit, './/nfe:xNome') or find_one(emit, './/xNome'),
        'cnpj_dest'    : find_one(dest, './/nfe:CNPJ') or find_one(dest, './/CNPJ') or find_one(dest, './/nfe:CPF') or find_one(dest, './/CPF'),
        'razao_dest'   : find_one(dest, './/nfe:xNome') or find_one(dest, './/xNome'),
        'valor_total'  : find_one(total, './/nfe:vNF') or find_one(total, './/vNF'),
    }

    # Normaliza data
    if cab['emitida_em']:
        try:
            cab['emitida_em'] = dtp.parse(cab['emitida_em'])
        except Exception:
            cab['emitida_em'] = None

    # Itens
    itens = []
    for i, det in enumerate(iter_many(nfe_el, './/nfe:det') or iter_many(nfe_el, './/det'), start=1):
        prod = det.find('.//nfe:prod', NS) or det.find('.//prod')
        if prod is None:
            continue
        itens.append({
            'linha'    : i,
            'sku'      : find_one(prod, './/nfe:cProd') or find_one(prod, './/cProd'),
            'descricao': find_one(prod, './/nfe:xProd') or find_one(prod, './/xProd'),
            'ncm'      : find_one(prod, './/nfe:NCM')  or find_one(prod, './/NCM'),
            'cfop'     : find_one(prod, './/nfe:CFOP') or find_one(prod, './/CFOP'),
            'qtd'      : find_one(prod, './/nfe:qCom') or find_one(prod, './/qCom'),
            'v_unit'   : find_one(prod, './/nfe:vUnCom') or find_one(prod, './/vUnCom'),
            'v_total'  : find_one(prod, './/nfe:vProd') or find_one(prod, './/vProd')
        })

    return cab, itens

# --- DB ops ------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(PG_DSN)

def select_webhook_nf(conn):
    """
    Busca eventos com URL de XML que ainda não estão em omie_nfe.
    """
    sql = """
        SELECT w.event_ts, w.id_nf, w.serie, w.xml_url
          FROM public.vw_webhook_nf w
         WHERE w.xml_url IS NOT NULL
           AND w.xml_url <> ''
           AND NOT EXISTS (
                 SELECT 1 FROM public.omie_nfe n
                  WHERE n.xml_url = w.xml_url
                )
         ORDER BY w.event_ts DESC
         LIMIT 200;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()

def upsert_nf(conn, cab, itens, id_nf, xml_url):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.omie_nfe
                (id_nf, nf_numero, nf_serie, chave_nfe, emitida_em,
                 cnpj_emitente, razao_emitente, cnpj_dest, razao_dest, valor_total, xml_url)
            VALUES
                (%s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s)
            ON CONFLICT (nf_numero, nf_serie) DO UPDATE SET
                id_nf = EXCLUDED.id_nf,
                chave_nfe = EXCLUDED.chave_nfe,
                emitida_em = EXCLUDED.emitida_em,
                cnpj_emitente = EXCLUDED.cnpj_emitente,
                razao_emitente = EXCLUDED.razao_emitente,
                cnpj_dest = EXCLUDED.cnpj_dest,
                razao_dest = EXCLUDED.razao_dest,
                valor_total = EXCLUDED.valor_total,
                xml_url = EXCLUDED.xml_url
        """, (
            id_nf,
            cab['nf_numero'], cab['nf_serie'], cab['chave_nfe'], cab['emitida_em'],
            cab['cnpj_emitente'], cab['razao_emitente'], cab['cnpj_dest'], cab['razao_dest'],
            None if cab['valor_total'] is None else float(cab['valor_total'].replace(',', '.')),
            xml_url
        ))

        # Limpa itens antigos desta NF e insere novamente
        cur.execute("DELETE FROM public.omie_nfe_itens WHERE nf_numero=%s AND nf_serie=%s",
                    (cab['nf_numero'], cab['nf_serie']))
        for it in itens:
            cur.execute("""
                INSERT INTO public.omie_nfe_itens
                (nf_numero, nf_serie, linha, sku, descricao, ncm, cfop, qtd, v_unit, v_total)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s)
            """, (
                cab['nf_numero'], cab['nf_serie'], it['linha'],
                it['sku'], it['descricao'], it['ncm'], it['cfop'],
                None if it['qtd']    is None else float(it['qtd'].replace(',', '.')),
                None if it['v_unit'] is None else float(it['v_unit'].replace(',', '.')),
                None if it['v_total']is None else float(it['v_total'].replace(',', '.'))
            ))

def main():
    conn = get_conn()
    try:
        rows = select_webhook_nf(conn)
        print(f"[INFO] Eventos com XML pendentes: {len(rows)}")

        for r in rows:
            xml_url = r['xml_url']
            id_nf   = r['id_nf']
            print(f"[INFO] Baixando XML: {xml_url}")

            resp = requests.get(xml_url, timeout=60)
            resp.raise_for_status()

            cab, itens = parse_nfe_xml(resp.content)

            if not cab['nf_numero'] or not cab['nf_serie']:
                print("[WARN] XML sem nNF/serie. Pulando.")
                continue

            upsert_nf(conn, cab, itens, id_nf, xml_url)
            conn.commit()
            print(f"[OK] Gravada NF {cab['nf_numero']}/{cab['nf_serie']} com {len(itens)} item(ns).")

        # Atualiza a MV consolidada para o orquestrador
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW public.mv_nf_pedido;")
        conn.commit()
        print("[OK] MV atualizada: public.mv_nf_pedido")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
