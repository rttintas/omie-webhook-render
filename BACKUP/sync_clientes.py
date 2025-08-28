# G:\Meu Drive\AutomacoesOmie\scripts\sync_clientes.py
import os, logging, datetime as dt
from sync_common import setup_logger, get_conn, get_checkpoint, set_checkpoint, paginar

APP_KEY    = os.getenv("OMIE_APP_KEY")
APP_SECRET = os.getenv("OMIE_APP_SECRET")

ENTIDADE   = "clientes"
URL        = "https://app.omie.com.br/api/v1/geral/clientes/"
CALL       = "ListarClientes"

# mapeie/converta aqui para suas colunas reais de omie_clientes:
def upsert_cliente(cur, item):
    # Exemplo de campos comuns (AJUSTE para seu schema!)
    codigo = item.get("codigo_cliente_omie") or item.get("codigo_cliente_integracao")
    nome   = item.get("razao_social") or item.get("nome_fantasia")
    cnpj   = (item.get("cnpj_cpf") or "").replace(".","").replace("/","").replace("-","").strip()
    criado = item.get("data_cadastro") or item.get("dataInclusao")  # dd/mm/aaaa
    if criado:
        criado_dt = dt.datetime.strptime(criado, "%d/%m/%Y")
    else:
        criado_dt = dt.datetime(1900,1,1)

    cur.execute("""
        INSERT INTO omie.clientes (codigo, nome, cnpj, criado_em)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (codigo) DO UPDATE
          SET nome = EXCLUDED.nome,
              cnpj = EXCLUDED.cnpj,
              criado_em = LEAST(omie.clientes.criado_em, EXCLUDED.criado_em)
    """, (codigo, nome, cnpj, criado_dt))
    return criado_dt

def main():
    log_path = setup_logger("sync_clientes")
    logging.info("== INÍCIO SYNC_CLIENTES ==")
    if not APP_KEY or not APP_SECRET:
        logging.error("OMIE_APP_KEY/OMIE_APP_SECRET não configurados.")
        return

    base_dt = dt.datetime(2025,8,22)  # ponto de partida mínimo de segurança
    with get_conn() as conn:
        with conn.cursor() as cur:
            desde = get_checkpoint(cur, ENTIDADE, base_dt)
            logging.info(f"checkpoint atual: {desde:%Y-%m-%d %H:%M:%S}")
            max_dt = desde

            total_upserts = 0
            for pagina, lista in paginar(URL, CALL, APP_KEY, APP_SECRET, desde_dt=desde):
                for item in lista:
                    try:
                        criado_dt = upsert_cliente(cur, item)
                        if criado_dt > max_dt:
                            max_dt = criado_dt
                        total_upserts += 1
                    except Exception as e:
                        logging.exception(f"Falha upsert cliente na página {pagina}: {e}")
                set_checkpoint(cur, ENTIDADE, max_dt)
                conn.commit()

            logging.info(f"== FIM SYNC_CLIENTES == Total upserts: {total_upserts}")
    return

if __name__ == "__main__":
    main()
