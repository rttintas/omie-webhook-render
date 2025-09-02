# -*- coding: utf-8 -*-
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from datetime import datetime

def build_lista_pdf(registros, agregados, out_path):
    """
    registros: lista de dicts [{nf_numero, pedido_id, cliente_nome, marketplace, descricao_item, qtd_item, received_at}]
    agregados: dict com { "tot_pedidos": int, "tot_itens": int, "por_marketplace": {"ML": 10, ...} }
    """
    doc = SimpleDocTemplate(out_path, pagesize=A4, leftMargin=18*mm, rightMargin=18*mm, topMargin=18*mm, bottomMargin=18*mm)
    styles = getSampleStyleSheet()
    story = []

    # Cabeçalho
    titulo = "<para align='center'><b>LISTA DE SEPARAÇÃO</b></para>"
    story.append(Paragraph(titulo, styles["Title"]))
    meta = f"Data: {datetime.now():%d/%m/%Y} • Total de pedidos: {agregados.get('tot_pedidos',0)} • Total de itens: {agregados.get('tot_itens',0)}"
    story.append(Paragraph(f"<para align='center'>{meta}</para>", styles["Normal"]))

    if agregados.get("por_marketplace"):
        mk_txt = " • ".join([f"{k}: {v}" for k,v in agregados["por_marketplace"].items()])
        story.append(Paragraph(f"<para align='center'>{mk_txt}</para>", styles["Normal"]))

    story.append(Spacer(1, 8))

    # Corpo: linhas por item
    # Ordenar por received_at, nf_numero
    registros = sorted(registros, key=lambda r: (r.get("received_at") or "", r.get("nf_numero") or 0))

    current_nf = None
    subtotal_nf = 0
    for r in registros:
        if current_nf is None:
            current_nf = r["nf_numero"]
            subtotal_nf = 0

        # quebra de NF: imprime subtotal da anterior
        if r["nf_numero"] != current_nf:
            story.append(Paragraph(
                f"<para align='right'><b>Subtotal NF {current_nf:06d}: {subtotal_nf} item(s)</b></para>", styles["Normal"]
            ))
            story.append(Spacer(1, 6))
            current_nf = r["nf_numero"]
            subtotal_nf = 0

        linha_titulo = f"<b>{r.get('descricao_item','(sem descrição)')}</b>"
        linha_info = f"Quantidade: {r.get('qtd_item',1)} • Cliente: {r.get('cliente_nome','')}"
        linha_nf_mk = f"NF: {r.get('nf_numero','')} • {r.get('marketplace','')}"

        story.append(Paragraph(linha_titulo, styles["BodyText"]))
        story.append(Paragraph(linha_info, styles["BodyText"]))
        story.append(Paragraph(linha_nf_mk, styles["BodyText"]))
        story.append(Spacer(1, 4))

        subtotal_nf += r.get("qtd_item", 1)

    # subtotal da última NF
    if current_nf is not None:
        story.append(Paragraph(
            f"<para align='right'><b>Subtotal NF {current_nf:06d}: {subtotal_nf} item(s)</b></para>", styles["Normal"]
        ))

    doc.build(story)
    return out_path
