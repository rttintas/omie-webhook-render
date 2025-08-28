import os, psycopg2, json, re
conn = psycopg2.connect(host=os.getenv("PGHOST"),port=os.getenv("PGPORT"),
                        dbname=os.getenv("PGDATABASE"),user=os.getenv("PGUSER"),
                        password=os.getenv("PGPASSWORD"))
cur = conn.cursor()
cur.execute("""
  SELECT payload_json
  FROM etl_staging_raw
  WHERE servico='produtos/pedido' AND metodo='ListarPedidos'
  ORDER BY id ASC LIMIT 1
""")
(j,) = cur.fetchone()

def walk(o, path="$"):
    if isinstance(o, dict):
        for k,v in o.items(): walk(v, f"{path}.{k}")
    elif isinstance(o, list):
        for i,v in enumerate(o): walk(v, f"{path}[{i}]")
    else:
        pass

# Mostra candidatos de itens e campos dentro de 'det' ou 'itens'
def list_item_candidates(j):
    def find_lists(node):
        out=[]
        if isinstance(node, dict):
            for v in node.values(): out += find_lists(v)
        elif isinstance(node, list):
            out.append(node)
            for v in node: out += find_lists(v)
        return out
    for lst in find_lists(j):
        if lst and isinstance(lst[0], dict) and any(k in lst[0] for k in ("det","produto","codigo_produto","descricao")):
            for d in lst[:3]:
                prod = d.get("produto", d)
                ks = list(prod.keys()) if isinstance(prod, dict) else []
                qk = [k for k in ks if re.search(r"(qtd|qtde|quant)", k, re.I)]
                vk = [k for k in ks if re.search(r"(vl[_]?unit|valor[_]?unit)", k, re.I)]
                if qk or vk:
                    print("Caminho candidato:", ks, "Q:", qk, "V:", vk)
                    return
    print("Nenhum candidato claro — me mande essa saída.")

list_item_candidates(j)
print("AMOSTRA:", json.dumps(j)[:1000])
