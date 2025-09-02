# -*- coding: utf-8 -*-
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib import colors
import re
from datetime import datetime

LABEL_W = 78 * mm
LABEL_H = 48 * mm
PAGE_W, PAGE_H = A4

def extract_color(descricao_item):
    if not descricao_item:
        return ""
    texto = descricao_item.lower()
    # padrões de capacidade
    m = re.search(r"(800ml|3,2l|32?00ml|16l)\s+(.*)", texto)
    if m:
        cor = m.group(2)
        # limpar excesso (até primeira vírgula ou fim)
        cor = re.split(r"[,\-•|/]", cor)[0].strip()
        return cor.upper()
    # fallback: tudo maiúsculo sem números
    return re.sub(r"\d+", "", descricao_item).strip().upper()

def build_etiquetas_pdf(registros, out_path):
    """
    registros: lista de dicts [{nf_numero, cliente_nome, marketplace, descricao_item}]
    Gera etiquetas 78x48mm, 3 por página (A4).
    """
    c = canvas.Canvas(out_path, pagesize=A4)
    margin_x = 12*mm
    margin_y = 15*mm
    gap_x = 8*mm
    gap_y = 6*mm

    # 3 colunas x N linhas
    cols = 2  # em A4 cabe 2 de 78mm com margem + gap (mais seguro)
    rows = 5  # 5 linhas por página aproximadamente

    x_positions = [margin_x + col*(LABEL_W+gap_x) for col in range(cols)]
    y_positions = [PAGE_H - margin_y - row*(LABEL_H+gap_y) - LABEL_H for row in range(rows)]

    i = 0
    for r in registros:
        col = (i % (cols*rows)) % cols
        row = (i % (cols*rows)) // cols
        if i > 0 and (i % (cols*rows) == 0):
            c.showPage()
        x = x_positions[col]
        y = y_positions[row]

        # moldura
        c.setStrokeColor(colors.black)
        c.rect(x, y, LABEL_W, LABEL_H, stroke=1, fill=0)

        # textos
        cor_txt = extract_color(r.get("descricao_item","")).upper() or "COR"
        cliente = r.get("cliente_nome","")
        marketplace = r.get("marketplace","")
        nf = str(r.get("nf_numero","")).zfill(6)

        # título (cor)
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(x + LABEL_W/2, y + LABEL_H - 12*mm, cor_txt)

        c.setFont("Helvetica-Bold", 12)
        c.drawCentredString(x + LABEL_W/2, y + LABEL_H - 18*mm, f"Feita com Carinho p/ {cliente.split()[0]}")

        c.setFont("Helvetica-Oblique", 10)
        c.drawCentredString(x + LABEL_W/2, y + LABEL_H - 24*mm, "As cores só chegam na")
        c.drawCentredString(x + LABEL_W/2, y + LABEL_H - 28*mm, "tonalidade após secagem total da cor.")

        c.setFont("Helvetica-Bold", 12)
        c.drawCentredString(x + LABEL_W/2, y + LABEL_H - 34*mm, "WhatsApp 11 99259-9967")
        c.drawCentredString(x + LABEL_W/2, y + LABEL_H - 38*mm, "Siga-nos @RTTINTAS")

        c.setFont("Helvetica-Bold", 12)
        c.drawString(x + 3*mm, y + 3*mm, f"{nf}  Pedido: {marketplace}")

        i += 1

    c.save()
    return out_path
