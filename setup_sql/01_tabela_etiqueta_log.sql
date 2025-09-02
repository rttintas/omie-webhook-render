-- tabela de log de impressões de etiquetas
CREATE TABLE IF NOT EXISTS public.etiqueta_log (
    id SERIAL PRIMARY KEY,
    nf_numero BIGINT NOT NULL,
    pedido_id BIGINT,
    cliente_nome TEXT,
    marketplace TEXT,
    printed_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (nf_numero, pedido_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_etiqueta_log_nf
  ON public.etiqueta_log (nf_numero, pedido_id);
