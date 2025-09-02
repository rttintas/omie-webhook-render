# -*- coding: utf-8 -*-
"""
load_omie_batch.py (FINAL)
"""

import os, sys, time, json, math, uuid, requests, psycopg2
from datetime import datetime
from decimal import Decimal
from psycopg2.extras import execute_batch

# ---------- impressão segura (sem emoji) ----------
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

def safe_print(*args, **kwargs):
    txt = " ".join(str(a) for a in args)
    try:
        print(txt, **kwargs)
    except UnicodeEncodeError:
        print(txt.encode("utf-8", "replace").decode("utf-8"), **kwargs)

VERSION = "2025-08-31_16h15_final"

# ---------- CONFIG ----------
PG_DSN = os.getenv("PG_DSN",
    "postgresql://neondb_owner:npg_X56oHDTNPtkz@ep-wispy-waterfall-achm1siz-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)

OMIE_APP_KEY    = os.getenv("OMIE_APP_KEY",   "6037923295404")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "f7a19a04170fe0cd16d2071d047f11a6")

PEDIDOS_URL = os.getenv("OMIE_PEDIDOS_URL", "https://app.omie.com.br/api/v1/produtos/pedido/")
NFE_URL     = os.getenv("OMIE_NFE_URL",     "https://app.omie.com.br/api/v1/contador/xml/")

OMIE_PEDIDOS_METHOD      = os.getenv("OMIE_PEDIDOS_METHOD",      "ListarPedidos")
OMIE_PEDIDO_ITENS_METHOD = os.getenv("OMIE_PEDIDO_ITENS_METHOD", "ListarItensDoPedido")
OMIE_NFE_METHOD          = os.getenv("OMIE_NFE_METHOD",          "ListarDocumentos")

BATCH_SIZE_PEDIDOS = int(os.getenv("BATCH_SIZE_PEDIDOS", "100"))
BATCH_SIZE_NFE     = int(os.getenv("BATCH_SIZE_NFE", "100"))

def _dec(v, default="0"):
    try:
        if v is None or v == "": v = default
        return Decimal(str(v))
    except Exception:
        return Decimal(default)

def _ts_now():
    return datetime.utcnow()

# ---------- HTTP Omie (com retries) ----------
class OmieHttpError(Exception): pass

def call_omie(url, method, payload, retries=2, backoff=2.0, timeout=30):
    body = {"call": method, "param":[payload], "app_key": OMIE_APP_KEY, "app_secret": OMIE_APP_SECRET}
    for attempt in range(retries+1):
        try:
            r = requests.post(url, json=body, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("faultstring"):
                raise OmieHttpError(data["faultstring"])
            return data
        except Exception as e:
            code = getattr(getattr(e, "response", None), "status_code", None)
            if attempt < retries and (code in (500,502,503,504) or isinstance(e, requests.RequestException)):
                safe_print(f"[retry] Omie {code}. aguardando {backoff}s…")
                time.sleep(backoff)
                continue
            raise OmieHttpError(f"Falha Omie ({method}): {e}") from e

# ---------- Normalização ----------
def norm_itens(id_pedido, raw_pedido):
    """
    Aceita: det/listas variadas – nunca usa .get() em lista.
    """
    det = None
    if isinstance(raw_pedido, dict):
        det = raw_pedido.get("det") or raw_pedido.get("itens") or raw_pedido.get("itens_pedido")
    if det is None:
        det = []

    # uniformiza em lista de elementos
    seq = []
    if isinstance(det, dict):
        # A nova lógica entra aqui.
        # Se for um dicionário, tente pegar a lista de itens.
        seq = det.get("item") or det.get("itens") or det.get("det") or [det]
        if isinstance(seq, dict):
            seq = [seq]
    elif isinstance(det, list):
        seq = det
    else:
        seq = []

    items = []
    linha = 0
    for elem in seq:
        if not isinstance(elem, dict):
            continue

        linha += 1
        produto = elem.get("produto") or elem.get("produtos")
        if produto is None and any(k in elem for k in ("codigo_produto","descricao","quantidade","unidade","vl_unitario","preco_unit")):
            produto = elem  # já está "achatado"

        if not isinstance(produto, dict):
            continue

        codigo = produto.get("codigo_produto") or produto.get("codigo") or produto.get("codigo_produto_omie") or ""
        descricao = produto.get("descricao") or produto.get("descricao_produto") or ""
        qtd = produto.get("quantidade") or produto.get("qtd") or produto.get("qtde") or 0
        und = produto.get("unidade") or produto.get("un") or ""
        vl_unit = produto.get("valor_unitario") or produto.get("vl_unitario") or produto.get("preco_unit") or 0

        items.append({
            "id_pedido": str(id_pedido),
            "linha": linha,
            "codigo_produto": str(codigo),
            "descricao": descricao,
            "quantidade": _dec(qtd),
            "unidade": und,
            "vl_unitario": _dec(vl_unit),
            "updated_at": _ts_now(),
        })
    return items

def norm_pedido(raw):
    cab = raw.get("cabecalho", {}) if isinstance(raw, dict) else {}
    id_pedido = cab.get("codigo_pedido_integracao") or cab.get("codigo_pedido") or cab.get("id_pedido") or cab.get("numero_pedido") or ""
    numero    = cab.get("numero_pedido") or cab.get("numero") or ""
    status    = cab.get("status_pedido") or cab.get("status") or ""
    cli_cod   = cab.get("codigo_cliente") or cab.get("cod_cliente") or ""
    cli_nome  = cab.get("nome_cliente")  or cab.get("cliente") or ""
    emissao   = (raw.get("info") or {}).get("data_emissao") or cab.get("data_previsao") or cab.get("data") or None
    vt        = (raw.get("total") or {}).get("valor_total") or cab.get("valor_total") or 0

    pedido = {
        "id_pedido": str(id_pedido),
        "numero_pedido": str(numero),
        "status": str(status),
        "cliente_codigo": str(cli_cod),
        "cliente_nome": str(cli_nome),
        "emissao": emissao,
        "valor_total": _dec(vt),
        "updated_at": _ts_now(),
    }
    pedido["itens"] = norm_itens(id_pedido, raw)
    return pedido

# ---------- SQL ----------
DDL = """
CREATE TABLE IF NOT EXISTS public.omie_pedido (
    id_pedido text PRIMARY KEY,
    numero_pedido text,
    status text,
    cliente_codigo text,
    cliente_nome text,
    emissao timestamptz,
    valor_total numeric(15,2),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_omie_pedido_updated_at ON public.omie_pedido(updated_at);
CREATE INDEX IF NOT EXISTS idx_omie_pedido_numero ON public.omie_pedido(numero_pedido);
CREATE TABLE IF NOT EXISTS public.omie_pedido_itens (
    id_pedido text NOT NULL,
    linha int NOT NULL,
    codigo_produto text,
    descricao text,
    quantidade numeric(15,3),
    unidade text,
    vl_unitario numeric(15,6),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id_pedido, linha),
    FOREIGN KEY (id_pedido) REFERENCES public.omie_pedido(id_pedido) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.omie_nfe (
    chave_nfe text PRIMARY KEY,
    numero text,
    serie text,
    emitida_em timestamptz,
    cnpj_emitente text,
    cnpj_destinatario text,
    valor_total numeric(15,2),
    status text,
    xml text,
    pdf_url text,
    last_event_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);
"""

SQL_UPSERT_PEDIDO = """
INSERT INTO public.omie_pedido
(id_pedido, numero_pedido, status, cliente_codigo, cliente_nome, emissao, valor_total, updated_at)
VALUES (%(id_pedido)s, %(numero_pedido)s, %(status)s, %(cliente_codigo)s, %(cliente_nome)s, %(emissao)s, %(valor_total)s, %(updated_at)s)
ON CONFLICT (id_pedido) DO UPDATE SET
  numero_pedido = EXCLUDED.numero_pedido,
  status = EXCLUDED.status,
  cliente_codigo = EXCLUDED.cliente_codigo,
  cliente_nome = EXCLUDED.cliente_nome,
  emissao = EXCLUDED.emissao,
  valor_total = EXCLUDED.valor_total,
  updated_at = GREATEST(omie_pedido.updated_at, EXCLUDED.updated_at);
"""

SQL_UPSERT_ITEM = """
INSERT INTO public.omie_pedido_itens
(id_pedido, linha, codigo_produto, descricao, quantidade, unidade, vl_unitario, updated_at)
VALUES (%(id_pedido)s, %(linha)s, %(codigo_produto)s, %(descricao)s, %(quantidade)s, %(unidade)s, %(vl_unitario)s, %(updated_at)s)
ON CONFLICT (id_pedido, linha) DO UPDATE SET
  codigo_produto = EXCLUDED.codigo_produto,
  descricao = EXCLUDED.descricao,
  quantidade = EXCLUDED.quantidade,
  unidade = EXCLUDED.unidade,
  vl_unitario = EXCLUDED.vl_unitario,
  updated_at = GREATEST(omie_pedido_itens.updated_at, EXCLUDED.updated_at);
"""

SQL_UPSERT_NFE = """
INSERT INTO public.omie_nfe
(chave_nfe, numero, serie, emitida_em, cnpj_emitente, cnpj_destinatario, valor_total, status, xml, pdf_url, last_event_at, updated_at)
VALUES (%(chave_nfe)s, %(numero)s, %(serie)s, %(emitida_em)s, %(cnpj_emitente)s, %(cnpj_destinatario)s, %(valor_total)s, %(status)s, %(xml)s, %(pdf_url)s, %(last_event_at)s, %(updated_at)s)
ON CONFLICT (chave_nfe) DO UPDATE SET
  numero = EXCLUDED.numero,
  serie = EXCLUDED.serie,
  emitida_em = EXCLUDED.emitida_em,
  cnpj_emitente = EXCLUDED.cnpj_emitente,
  cnpj_destinatario = EXCLUDED.cnpj_destinatario,
  valor_total = EXCLUDED.valor_total,
  status = EXCLUDED.status,
  xml = COALESCE(EXCLUDED.xml, public.omie_nfe.xml),
  pdf_url = COALESCE(EXCLUDED.pdf_url, public.omie_nfe.pdf_url),
  last_event_at = COALESCE(EXCLUDED.last_event_at, public.omie_nfe.last_event_at),
  updated_at = GREATEST(omie_nfe.updated_at, EXCLUDED.updated_at);
"""

def ensure_tables(conn):
    with conn, conn.cursor() as cur:
        cur.execute(DDL)

# ---------- Fetchers ----------
def fetch_pedidos():
    pagina = 1
    while True:
        payload = {"pagina": pagina, "registros_por_pagina": BATCH_SIZE_PEDIDOS, "apenas_importado_api": "N"}
        data = call_omie(PEDIDOS_URL, OMIE_PEDIDOS_METHOD, payload)

        pedidos = None
        if isinstance(data, dict):
            for k in ("pedido_venda_produto", "pedidos", "lista_pedidos"):
                if isinstance(data.get(k), list):
                    pedidos = data[k]; break

        if not pedidos:
            if pagina == 1: safe_print("Nenhum pedido retornado.")
            break

        for p in pedidos:
            yield p

        total_de_paginas = data.get("total_de_paginas") or 1
        if pagina >= int(total_de_paginas): break
        pagina += 1

def fetch_nfe_meta():
    pagina = 1
    while True:
        payload = {"nPagina": pagina, "nRegPorPagina": BATCH_SIZE_NFE, "cModelo": "55", "dEmiInicial": "", "dEmiFinal": ""}
        data = call_omie(NFE_URL, OMIE_NFE_METHOD, payload)

        docs = data.get("documentosEncontrados") if isinstance(data, dict) else None
        if not docs:
            if pagina == 1: safe_print("Nenhuma NF-e retornada.")
            break

        for d in docs: yield d

        n_tot = data.get("nTotRegistros") or 0
        n_reg = data.get("nRegistros") or len(docs)
        if not n_reg: break
        total_pages = int(math.ceil(n_tot / float(n_reg))) if n_tot else pagina
        if pagina >= total_pages: break
        pagina += 1

# ---------- Main ----------
def main():
    safe_print(f"load_omie_batch.py version {VERSION}")
    with psycopg2.connect(PG_DSN) as conn:
        ensure_tables(conn)

        safe_print("\nBaixando PEDIDOS…")
        pedidos_norm, itens_norm = [], []

        for raw in fetch_pedidos():
            try:
                norm = norm_pedido(raw)
                pedidos_norm.append(norm)
                itens_norm.extend(norm.get("itens", []))
            except Exception as e:
                safe_print("Falha ao normalizar pedido:", repr(e))
                continue

            if len(pedidos_norm) >= 500:
                with conn, conn.cursor() as cur:
                    execute_batch(cur, SQL_UPSERT_PEDIDO, pedidos_norm, page_size=200)
                    if itens_norm: execute_batch(cur, SQL_UPSERT_ITEM, itens_norm, page_size=500)
                pedidos_norm.clear()
                itens_norm.clear()

        if pedidos_norm:
            with conn, conn.cursor() as cur:
                execute_batch(cur, SQL_UPSERT_PEDIDO, pedidos_norm, page_size=200)
                if itens_norm: execute_batch(cur, SQL_UPSERT_ITEM, itens_norm, page_size=500)

        safe_print("\nBaixando NF-e (metadados + XML)…")
        nfe_rows = []
        for d in fetch_nfe_meta():
            try:
                numero = str(d.get("nNumero") or d.get("numero") or "")
                serie  = str(d.get("cSerie") or d.get("serie") or "")
                chave  = str(d.get("nChave") or d.get("cChave") or d.get("chave") or d.get("nChaveNFe") or "")
                dEmi   = d.get("dEmissao") or d.get("demissao")
                hEmi   = d.get("hEmissao") or d.get("hemissao")
                emissao = None
                if dEmi:
                    try: emissao = datetime.fromisoformat(str(dEmi))
                    except Exception: emissao = None
                if emissao and hEmi:
                    try:
                        hh,mm,ss = str(hEmi).split(":");
                        emissao = emissao.replace(hour=int(hh),minute=int(mm),second=int(float(ss)))
                    except Exception: pass
                valor  = _dec(d.get("nValor") or d.get("valor") or 0)
                status = str(d.get("cStatus") or d.get("status") or "")
                xml    = d.get("cXml") or d.get("xml") or None

                nfe_rows.append({
                    "chave_nfe": chave or str(uuid.uuid4()),
                    "numero": numero, "serie": serie, "emitida_em": emissao,
                    "cnpj_emitente": None, "cnpj_destinatario": None,
                    "valor_total": valor, "status": status, "xml": xml,
                    "pdf_url": None, "last_event_at": None, "updated_at": _ts_now(),
                })
            except Exception as e:
                safe_print("Falha ao normalizar NFe:", repr(e))
                continue

            if len(nfe_rows) >= 500:
                with conn, conn.cursor() as cur:
                    execute_batch(cur, SQL_UPSERT_NFE, nfe_rows, page_size=200)
                nfe_rows.clear()

        if nfe_rows:
            with conn, conn.cursor() as cur:
                execute_batch(cur, SQL_UPSERT_NFE, nfe_rows, page_size=200)

        safe_print("\nConcluído.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        safe_print("ERRO durante a carga:", repr(e))
        sys.exit(1)