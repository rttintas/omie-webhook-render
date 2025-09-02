# -*- coding: utf-8 -*-
import os
import re
import sys
import csv
import time
import logging
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from reportlab.lib.pagesizes import landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib.utils import simpleSplit
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
BASE = Path(__file__).resolve().parent

# Pastas (ajuste se quiser, mas deixei centralizado aqui)
DIR_ENTRADA_PDFS = Path(r"G:\Meu Drive\Produção Ecommerce\Entrada Pdfs")
DIR_SAIDA = BASE / "saida"
DIR_LABELS = DIR_SAIDA / "etiquetas_10x15"
DIR_CASADOS = DIR_SAIDA / "casados_10x15"

# Log
DIR_LOG = BASE / "logs"
DIR_LOG.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=DIR_LOG / f"exec_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
print(f"== Início: {datetime.now()} ==")

# Lê .env local
def load_env(p: Path):
    env = {}
    if not p.exists():
        raise RuntimeError(f"config.env não encontrado em: {p}")
    for line in p.read_text(encoding="utf-8").splitlines():
        line=line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k,v=line.split("=",1)
        env[k.strip()]=v.strip()
    return env

ENV = load_env(BASE / "config.env")
PGHOST = ENV.get("PGDB_HOST")
PGPORT = int(ENV.get("DB_PORT","5432"))
PGDB   = ENV.get("DB_NAME")
PGUSER = ENV.get("DB_USER")
PGPASS = ENV.get("DB_PASSWORD")
PGSSLM = ENV.get("DB_SSLMODE","require")

LABEL_PHONE = "(11) 91502-1984"
LABEL_INSTAGRAM = "@RTTINTAS"

# --------------------------------------------------
# DB helpers
# --------------------------------------------------
def get_conn():
    dsn = f"host={PGHOST} port={PGPORT} dbname={PGDB} user={PGUSER} password={PGPASS} sslmode={PGSSLM}"
    return psycopg2.connect(dsn)

def query_dicts(sql, params=None):
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()

# --------------------------------------------------
# Util
# --------------------------------------------------
NUM_RE = re.compile(r"(\d{6,})")  # pega sequência grande de dígitos

def listar_pdfs_de_entrada():
    if not DIR_ENTRADA_PDFS.exists():
        raise RuntimeError(f"Pasta de entrada não encontrada: {DIR_ENTRADA_PDFS}")
    arquivos = []
    for p in DIR_ENTRADA_PDFS.glob("*.pdf"):
        if p.stat().st_size >= 100_000:  # >=100KB
            arquivos.append(p)
    return sorted(arquivos)

def extrair_nf_do_nome(arq: Path):
    # tenta padrão comum: nota_fiscal_saida_00009335_001_2025-08-22_15_03_15.pdf
    m = NUM_RE.findall(arq.stem)
    return m[0] if m else None

# --------------------------------------------------
# Etiquetas 10x15
# --------------------------------------------------
def draw_multiline(c, text, x, y, w, leading, font_name="Helvetica-Bold", font_size=14):
    c.setFont(font_name, font_size)
    lines = simpleSplit(text, font_name, font_size, w)
    for ln in lines:
        c.drawString(x, y, ln)
        y -= leading
    return y

def gerar_label_10x15(saida_pdf: Path, nf: str, cliente: str, marketplace: str):
    saida_pdf.parent.mkdir(parents=True, exist_ok=True)

    # 10x15cm na orientação paisagem -> 150x100 mm
    width, height = landscape((150*mm, 100*mm))
    c = canvas.Canvas(str(saida_pdf), pagesize=(width, height))

    # fontes (opcional: tenta DejaVu para acentos bonitos)
    try:
        ttf = BASE / "DejaVuSans.ttf"
        if ttf.exists():
            pdfmetrics.registerFont(TTFont('DejaVu', str(ttf)))
            F_BOLD = "DejaVu"
        else:
            F_BOLD = "Helvetica-Bold"
    except:
        F_BOLD = "Helvetica-Bold"

    # Título “COR EM CAIXA ALTA”
    y = height - 15*mm
    c.setFont(F_BOLD, 22)
    c.drawCentredString(width/2, y, "COR EM CAIXA ALTA")
    y -= 10*mm

    # “FEITA COM CARINHO P/ [NOME DO CLIENTE]”
    y = draw_multiline(c, f"FEITA COM CARINHO P/ {cliente.upper()}", 10*mm, y, width-20*mm, 14, F_BOLD, 16) - 6

    # “A COR CHEGA …”
    y = draw_multiline(
        c,
        "A COR CHEGA NA TONALIDADE APÓS\nA SECAGEM TOTAL DA TINTA",
        10*mm, y, width-20*mm, 14, F_BOLD, 14
    ) - 8

    # rodapé com telefone e insta
    c.setFont(F_BOLD, 14)
    c.drawCentredString(width/2, 12*mm, f"📞 {LABEL_PHONE}  •  📸 {LABEL_INSTAGRAM}")

    # canto superior direito: “NFe:xxxx   {marketplace}”
    c.setFont(F_BOLD, 14)
    c.drawRightString(width - 8*mm, height - 10*mm, f"NFe:{nf}    {marketplace.upper() if marketplace else ''}")

    c.showPage()
    c.save()

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    logging.info(f"Lendo PDFs em: {DIR_ENTRADA_PDFS}")
    pdfs = listar_pdfs_de_entrada()
    print(f"NF(s) detectadas (>=100KB): {[p.stem for p in pdfs]}")
    if not pdfs:
        print("Nada para processar.")
        return

    # mapeia NF -> arquivo
    nfs = []
    mapa_pdf = {}
    for p in pdfs:
        nf = extrair_nf_do_nome(p)
        if nf:
            nfs.append(nf)
            mapa_pdf[nf] = p
    nfs = sorted(set(nfs))
    if not nfs:
        print("[ERRO] Não consegui extrair números de NF dos nomes dos PDFs.")
        return

    # consulta a view
    sql = """
    SELECT nf_numero, pedido_id, numero_pedido, cliente, marketplace, received_at
    FROM public.vw_nf_pedido_std
    WHERE nf_numero = ANY(%s)
    ORDER BY received_at DESC;
    """
    rows = query_dicts(sql, (nfs,))
    if rows is None:
        rows = []

    # salva a lista de separação (csv)
    DIR_SAIDA.mkdir(parents=True, exist_ok=True)
    csv_path = DIR_SAIDA / f"lista_separacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["nf", "pedido_id", "numero_pedido", "cliente", "marketplace", "received_at", "arquivo_pdf"])
        for r in rows:
            nf = r["nf_numero"]
            arq = mapa_pdf.get(nf)
            w.writerow([nf, r["pedido_id"], r["numero_pedido"], r["cliente"], r["marketplace"], r["received_at"], str(arq if arq else "")])

    print(f"[OK] Lista de separação: {csv_path}")

    # gera etiquetas 10x15 para as NFs encontradas
    if not rows:
        print("[AVISO] View não retornou dados para as NFs dos PDFs. Verifique webhooks/omie_nfe.")
    else:
        for r in rows:
            nf = r["nf_numero"]
            cliente = r["cliente"] or ""
            marketplace = r["marketplace"] or ""
            out_pdf = DIR_LABELS / f"etq_{nf}.pdf"
            gerar_label_10x15(out_pdf, nf, cliente, marketplace)
        print(f"[OK] Etiquetas geradas em: {DIR_LABELS}")

    print("== Fim ==")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Falha geral")
        print(f"[ERRO] {e}")
        sys.exit(1)
