# init_schema.py
import os, psycopg2

conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS etl_staging_raw (
  id BIGSERIAL PRIMARY KEY,
  servico VARCHAR(80) NOT NULL,
  metodo VARCHAR(80) NOT NULL,
  pagina INT,
  payload_json JSONB NOT NULL,
  coletado_em TIMESTAMPTZ DEFAULT now()
);
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS etl_sync_control (
  servico VARCHAR(80) PRIMARY KEY,
  ultimo_cursor TEXT,
  atualizado_em TIMESTAMPTZ
);
""")
cur.execute("""
CREATE INDEX IF NOT EXISTS idx_staging_servico ON etl_staging_raw(servico);
""")

cur.close()
conn.close()
print("OK: schema criado/validado.")
