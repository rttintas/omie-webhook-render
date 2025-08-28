# -*- coding: utf-8 -*-
"""
Promove dados do etl_staging_raw para as tabelas de domínio OMIE.
Ajustes:
- Clientes: PK canônica usa codigo_cliente_integracao OU codigo_cliente_omie (fallback).
- Pedidos: resolve id_cliente aceitando codigo_cliente_omie; cria 'stub' de cliente se faltar.
- Itens de pedido: aceita várias chaves para quantidade/valor_unitário e calcula via total quando preciso.
- Normaliza datas (YYYY-MM-DD) e números com vírgula/ponto.
- Usa etl_staging_progress para não reprocessar o que já foi promovido.
"""

import os, sys
import psycopg2

# ------------------------- Conexão -------------------------
def pg():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )

# ------------------------- Utils ---------------------------
def _is_dict_list(x): return isinstance(x, list) and x and isinstance(x[0], dict)

def _walk_lists(obj, path="$"):
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            out += _walk_lists(v, f"{path}.{k}")
    elif isinstance(obj, list):
        if _is_dict_list(obj):
            out.append((path, obj))
        for i, v in enumerate(obj):
            out += _walk_lists(v, f"{path}[{i}]")
    return out

def _first(d, keys):
    for k in keys:
        if isinstance(d, dict):
            v = d.get(k)
            if v not in (None, "", []):
                return v
    return None

def _as_str(v): return "" if v in (None, "") else str(v)

def _to_float(v, default=0.0):
    """Converte strings '1.234,56' / '1234,56' / '1,234.56' para float."""
    if v in (None, ""): return default
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace(" ", "")
    try:
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
        return float(s)
    except Exception:
        return default

def _as_int(v, default=0):
    try: return int(float(v))
    except: return default

def _as_date_yyyy_mm_dd(s):
    if not s: return None
    s = str(s).strip()[:10]
    if not s: return None
    if "-" in s:
        parts = s.split("-")
        if len(parts) == 3 and len(parts[0]) == 4:
            yyyy, mm, dd = parts
            return f"{yyyy.zfill(4)}-{mm.zfill(2)}-{dd.zfill(2)}"
        return None
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 3:
            dd, mm, yyyy = parts
            if len(yyyy) == 4:
                return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return None

# ---------------------- Extractors -------------------------
def iter_clientes(payload):
    # procura a primeira lista que pareça ser de clientes (ex.: 'clientes_cadastro')
    for _, arr in _walk_lists(payload):
        if any(any(k in d for k in ("razao_social","nome_fantasia","cnpj_cpf","cpf_cnpj",
                                    "codigo_cliente_omie","codigo_cliente_integracao")) for d in arr[:3]):
            for c in arr:
                id_int  = _first(c, ["codigo_cliente_integracao"])
                id_omie = _first(c, ["codigo_cliente_omie","codigo_cliente","id_cliente"])
                pk = id_int or id_omie
                if not pk:
                    continue
                yield {
                    "id_cliente": _as_str(pk),                # PK canônica
                    "id_cli_int": _as_str(id_int),
                    "id_cli_omie": _as_str(id_omie),
                    "nome": _as_str(_first(c, ["razao_social","nome_fantasia","nome"])),
                    "documento": _as_str(_first(c, ["cnpj_cpf","cpf_cnpj","cnpj","cpf"])),
                    "telefone": _as_str(_first(c, ["telefone1_numero","telefone","fone1_numero","telefone1","celular"])),
                }
            return

def iter_produtos(payload):
    for _, arr in _walk_lists(payload):
        if any(any(k in d for k in ("codigo_produto","codigo","descricao","ncm","unidade")) for d in arr[:3]):
            for p in arr:
                id_prod = _first(p, ["codigo_produto_integracao","codigo_produto","id_produto","codigo"])
                if not id_prod: continue
                yield {
                    "id_produto": _as_str(id_prod),
                    "codigo_produto": _as_str(_first(p, ["codigo","codigo_produto"])),
                    "descricao": _as_str(_first(p, ["descricao","descricao_produto"])),
                    "ncm": _as_str(_first(p, ["ncm"])),
                    "unidade": _as_str(_first(p, ["unidade","unidade_produto"])),
                }
            return

def iter_pedidos(payload):
    for _, arr in _walk_lists(payload):
        if any(("cabecalho" in d) or ("det" in d or "itens" in d) or ("numero_pedido" in d) for d in arr[:3]):
            for ped in arr:
                cab = ped.get("cabecalho", ped) if isinstance(ped, dict) else {}
                numero = _first(cab, ["numero_pedido","codigo_pedido","id_pedido","codigo_pedido_integracao"])
                if not numero: continue
                dt = _first(cab, ["data_emissao","dt_faturamento"])
                rec = {
                    "numero_pedido": _as_str(numero),
                    "id_pedido_omie": _as_str(_first(cab, ["id_pedido","codigo_pedido_integracao"])),
                    # <<< ID do cliente aceita codigo_cliente_omie como fallback >>>
                    "id_cliente": _as_str(_first(cab, [
                        "codigo_cliente_integracao",
                        "codigo_cliente",
                        "codigo_cliente_omie",
                        "id_cliente"
                    ])),
                    "data_emissao": _as_date_yyyy_mm_dd(dt),
                    "valor_total": _to_float(_first(cab, ["valor_total","vl_total"]), 0.0),
                    "situacao": _as_str(_first(cab, ["situacao","status"])),
                    "itens": []
                }

                itens = ped.get("det") or ped.get("itens") or []
                for idx, i in enumerate(itens, 1):
                    prod = i.get("produto", i) if isinstance(i, dict) else {}

                    # nomes possíveis por conta
                    qty_keys  = ["quantidade", "qtde", "qtd", "qCom", "qtde_unid_trib_ipi"]
                    vunit_keys = ["valor_unitario", "vl_unit", "vlUnit", "vUnCom"]
                    vtot_keys  = ["valor_total", "valor_mercadoria"]

                    def pick(keys): return _first(i, keys) or _first(prod, keys)

                    q_raw  = pick(qty_keys)
                    vu_raw = pick(vunit_keys)
                    vt_raw = pick(vtot_keys)

                    q  = _to_float(q_raw, 0.0)
                    vu = _to_float(vu_raw, 0.0)
                    vt = _to_float(vt_raw, 0.0)

                    # fallbacks: calcula q ou vu a partir do total
                    if (q is None or q == 0.0) and vt and vu:
                        q = vt / vu
                    if (vu is None or vu == 0.0) and vt and q:
                        vu = vt / q

                    rec["itens"].append({
                        "sequencia_item": _as_int(_first(i, ["sequencia","nItemPed","nItem","sequencia_item"]), idx),
                        "id_produto": _as_str(_first(i, ["codigo_produto_integracao"])) or _as_str(_first(prod, ["codigo_produto_integracao"])),
                        "codigo_produto": _as_str(_first(i, ["codigo_produto","codigo"])) or _as_str(_first(prod, ["codigo_produto","codigo"])),
                        "descricao_produto": _as_str(_first(i, ["descricao"])) or _as_str(_first(prod, ["descricao"])),
                        "quantidade": q or 0.0,
                        "valor_unitario": vu or 0.0,
                        "eh_kit": bool(i.get("eh_kit", False)),
                        "linha_kit": _as_int(i.get("linha_kit", 0), 0),
                    })
                yield rec
            return

def iter_nfe(payload):
    for _, arr in _walk_lists(payload):
        if any(any(k in d for k in ("chave_nfe","chNFe","numero_nfe","nNF","serie")) for d in arr[:3]):
            for nf in arr:
                chave = _first(nf, ["chave_nfe","chaveAcesso","chNFe"])
                if not chave: continue
                dt = _first(nf, ["data_emissao","dhEmi"])
                rec = {
                    "chave_nfe": _as_str(chave),
                    "numero_nf": _as_int(_first(nf, ["numero_nfe","nNF","numero"]), 0),
                    "serie": _as_str(_first(nf, ["serie","nSerie"])),
                    "id_cliente": _as_str(_first(nf, ["codigo_cliente_integracao","codigo_cliente_omie","id_cliente"])),
                    "data_emissao": _as_date_yyyy_mm_dd(dt),
                    "valor_total": _to_float(_first(nf, ["valor_total","vNF"]), 0.0),
                    "modelo": _as_str(_first(nf, ["modelo","mod"])),
                    "itens": []
                }
                itens = nf.get("det") or nf.get("itens") or []
                for idx, i in enumerate(itens, 1):
                    prod = i.get("produto", i) if isinstance(i, dict) else {}
                    q  = _to_float(_first(i, ["quantidade","qCom"]) or _first(prod, ["quantidade","qCom"]), 0.0)
                    vu = _to_float(_first(i, ["valor_unitario","vUnCom"]) or _first(prod, ["valor_unitario","vUnCom"]), 0.0)
                    rec["itens"].append({
                        "sequencia_item": _as_int(_first(i, ["nItem","sequencia"]), idx),
                        "id_produto": _as_str(_first(i, ["codigo_produto_integracao"])) or _as_str(_first(prod, ["codigo_produto_integracao"])),
                        "codigo_produto": _as_str(_first(i, ["codigo_produto","codigo"])) or _as_str(_first(prod, ["codigo_produto","codigo"])),
                        "descricao_produto": _as_str(_first(i, ["descricao"])) or _as_str(_first(prod, ["descricao"])),
                        "quantidade": q,
                        "valor_unitario": vu,
                    })
                yield rec
            return

# ----------------------- Upserts ---------------------------
def upsert_clientes(cur, rec):
    cur.execute("""
        INSERT INTO omie_cliente (id_cliente, id_cliente_integracao, id_cliente_omie, nome, documento, telefone, atualizado_em)
        VALUES (%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (id_cliente)
        DO UPDATE SET id_cliente_integracao = EXCLUDED.id_cliente_integracao,
                      id_cliente_omie       = EXCLUDED.id_cliente_omie,
                      nome                  = EXCLUDED.nome,
                      documento             = EXCLUDED.documento,
                      telefone              = EXCLUDED.telefone,
                      atualizado_em         = now();
    """, (rec["id_cliente"], rec["id_cli_int"], rec["id_cli_omie"], rec["nome"], rec["documento"], rec["telefone"]))

def upsert_produtos(cur, rec):
    cur.execute("""
        INSERT INTO omie_produto (id_produto, codigo_produto, descricao, ncm, unidade, atualizado_em)
        VALUES (%s,%s,%s,%s,%s, now())
        ON CONFLICT (id_produto)
        DO UPDATE SET codigo_produto=EXCLUDED.codigo_produto,
                      descricao     =EXCLUDED.descricao,
                      ncm           =EXCLUDED.ncm,
                      unidade       =EXCLUDED.unidade,
                      atualizado_em =now();
    """, (rec["id_produto"], rec["codigo_produto"], rec["descricao"], rec["ncm"], rec["unidade"]))

def resolve_cliente_id(cur, any_id):
    if not any_id: return None
    cur.execute("""
        SELECT id_cliente
          FROM omie_cliente
         WHERE id_cliente = %s
            OR id_cliente_integracao = %s
            OR id_cliente_omie = %s
         LIMIT 1
    """, (any_id, any_id, any_id))
    row = cur.fetchone()
    return row[0] if row else None

def upsert_pedido(cur, rec):
    # garante FK de cliente (cria stub se necessário)
    raw_id = rec["id_cliente"]
    resolved = resolve_cliente_id(cur, raw_id)
    if resolved is None and raw_id:
        cur.execute("""
            INSERT INTO omie_cliente (id_cliente, atualizado_em)
            VALUES (%s, now())
            ON CONFLICT (id_cliente) DO NOTHING
        """, (raw_id,))
        resolved = raw_id

    cur.execute("""
        INSERT INTO omie_pedido (numero_pedido, id_pedido_omie, id_cliente, data_emissao, valor_total, situacao, atualizado_em)
        VALUES (%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (numero_pedido)
        DO UPDATE SET id_pedido_omie=EXCLUDED.id_pedido_omie,
                      id_cliente    =EXCLUDED.id_cliente,
                      data_emissao  =EXCLUDED.data_emissao,
                      valor_total   =EXCLUDED.valor_total,
                      situacao      =EXCLUDED.situacao,
                      atualizado_em =now();
    """, (rec["numero_pedido"], rec["id_pedido_omie"], resolved, rec["data_emissao"], rec["valor_total"], rec["situacao"]))

def upsert_pedido_item(cur, numero_pedido, it):
    cur.execute("""
        INSERT INTO omie_pedido_item (numero_pedido, sequencia_item, id_produto, codigo_produto, descricao_produto, quantidade, valor_unitario, eh_kit, linha_kit, atualizado_em)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (numero_pedido, sequencia_item)
        DO UPDATE SET id_produto=EXCLUDED.id_produto,
                      codigo_produto=EXCLUDED.codigo_produto,
                      descricao_produto=EXCLUDED.descricao_produto,
                      quantidade=EXCLUDED.quantidade,
                      valor_unitario=EXCLUDED.valor_unitario,
                      eh_kit=EXCLUDED.eh_kit,
                      linha_kit=EXCLUDED.linha_kit,
                      atualizado_em=now();
    """, (numero_pedido, it["sequencia_item"], it["id_produto"], it["codigo_produto"], it["descricao_produto"], it["quantidade"], it["valor_unitario"], it["eh_kit"], it["linha_kit"]))

def upsert_nf(cur, rec):
    cur.execute("""
        INSERT INTO omie_nf (chave_nfe, numero_nf, serie, id_cliente, data_emissao, valor_total, modelo, atualizado_em)
        VALUES (%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (chave_nfe)
        DO UPDATE SET numero_nf=EXCLUDED.numero_nf,
                      serie=EXCLUDED.serie,
                      id_cliente=EXCLUDED.id_cliente,
                      data_emissao=EXCLUDED.data_emissao,
                      valor_total=EXCLUDED.valor_total,
                      modelo=EXCLUDED.modelo,
                      atualizado_em=now();
    """, (rec["chave_nfe"], rec["numero_nf"], rec["serie"], rec["id_cliente"], rec["data_emissao"], rec["valor_total"], rec["modelo"]))

def upsert_nf_item(cur, chave_nfe, it):
    cur.execute("""
        INSERT INTO omie_nf_item (chave_nfe, sequencia_item, id_produto, codigo_produto, descricao_produto, quantidade, valor_unitario)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (chave_nfe, sequencia_item)
        DO UPDATE SET id_produto=EXCLUDED.id_produto,
                      codigo_produto=EXCLUDED.codigo_produto,
                      descricao_produto=EXCLUDED.descricao_produto,
                      quantidade=EXCLUDED.quantidade,
                      valor_unitario=EXCLUDED.valor_unitario;
    """, (chave_nfe, it["sequencia_item"], it["id_produto"], it["codigo_produto"], it["descricao_produto"], it["quantidade"], it["valor_unitario"]))

# ------------------------ Main -----------------------------
def promote(entidade):
    conn = pg(); conn.autocommit = False; cur = conn.cursor()

    # garante tabela de progresso
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_staging_progress (
            servico TEXT NOT NULL,
            metodo  TEXT NOT NULL,
            last_id BIGINT DEFAULT 0,
            atualizado_em TIMESTAMPTZ,
            PRIMARY KEY (servico,metodo)
        );
    """)
    conn.commit()

    def run(servico, metodo, it_fn, upsert_head, upsert_item=None, pk_name=None):
        cur.execute("SELECT last_id FROM etl_staging_progress WHERE servico=%s AND metodo=%s;", (servico,metodo))
        row = cur.fetchone(); last_id = row[0] if row else 0

        cur.execute("""
            SELECT id, payload_json
              FROM etl_staging_raw
             WHERE servico=%s AND metodo=%s AND id > %s
             ORDER BY id
        """, (servico,metodo,last_id))
        rows = cur.fetchall()

        max_id = last_id; pages = 0
        for _id, payload in rows:
            try:
                produced = False
                for rec in it_fn(payload) or []:
                    produced = True
                    upsert_head(cur, rec)
                    if upsert_item:
                        pk = rec.get(pk_name)
                        for it in rec.get("itens", []):
                            upsert_item(cur, pk, it)
                max_id = max(max_id, _id)
                pages += 1 if produced else 0
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"[WARN] id={_id} -> {e}")

        cur.execute("""
            INSERT INTO etl_staging_progress (servico,metodo,last_id,atualizado_em)
            VALUES (%s,%s,%s, now())
            ON CONFLICT (servico,metodo)
            DO UPDATE SET last_id=EXCLUDED.last_id, atualizado_em=EXCLUDED.atualizado_em;
        """, (servico,metodo,max_id))
        conn.commit()
        print(f"[OK] {servico}/{metodo}: {pages} páginas novas processadas (last_id={max_id}).")

    if entidade in ("clientes","tudo"):
        run("geral/clientes","ListarClientes", iter_clientes, upsert_clientes)
    if entidade in ("produtos","tudo"):
        run("geral/produtos","ListarProdutos", iter_produtos, upsert_produtos)
    if entidade in ("pedidos","tudo"):
        run("produtos/pedido","ListarPedidos", iter_pedidos, upsert_pedido, upsert_pedido_item, pk_name="numero_pedido")
    if entidade in ("nfe","tudo"):
        run("produtos/nfe","ListarNFe", iter_nfe, upsert_nf, upsert_nf_item, pk_name="chave_nfe")

    cur.close(); conn.close()

if __name__ == "__main__":
    alvo = sys.argv[1].lower() if len(sys.argv)>1 else "tudo"
    promote(alvo)
