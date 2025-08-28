-- CLIENTE
CREATE TABLE IF NOT EXISTS omie_cliente (
  id_cliente        TEXT PRIMARY KEY,
  nome              TEXT,
  documento         TEXT,
  telefone          TEXT,
  atualizado_em     TIMESTAMPTZ
);

-- PRODUTO
CREATE TABLE IF NOT EXISTS omie_produto (
  id_produto        TEXT PRIMARY KEY,
  codigo_produto    TEXT UNIQUE,
  descricao         TEXT,
  ncm               TEXT,
  unidade           TEXT,
  atualizado_em     TIMESTAMPTZ
);

-- PEDIDO (cabeçalho)
CREATE TABLE IF NOT EXISTS omie_pedido (
  numero_pedido     TEXT PRIMARY KEY,
  id_pedido_omie    TEXT,
  id_cliente        TEXT,
  data_emissao      DATE,
  valor_total       NUMERIC(14,2),
  situacao          TEXT,
  atualizado_em     TIMESTAMPTZ,
  FOREIGN KEY (id_cliente) REFERENCES omie_cliente(id_cliente)
);

-- PEDIDO (itens)
CREATE TABLE IF NOT EXISTS omie_pedido_item (
  numero_pedido     TEXT NOT NULL,
  sequencia_item    INT  NOT NULL,
  id_produto        TEXT,
  codigo_produto    TEXT,
  descricao_produto TEXT,
  quantidade        NUMERIC(14,3),
  valor_unitario    NUMERIC(14,2),
  eh_kit            BOOLEAN DEFAULT FALSE,
  linha_kit         INT,
  atualizado_em     TIMESTAMPTZ,
  PRIMARY KEY (numero_pedido, sequencia_item),
  FOREIGN KEY (numero_pedido) REFERENCES omie_pedido(numero_pedido)
);

-- NFe (cabeçalho)
CREATE TABLE IF NOT EXISTS omie_nf (
  chave_nfe         TEXT PRIMARY KEY,
  numero_nf         INT,
  serie             TEXT,
  id_cliente        TEXT,
  data_emissao      DATE,
  valor_total       NUMERIC(14,2),
  modelo            TEXT,
  atualizado_em     TIMESTAMPTZ,
  FOREIGN KEY (id_cliente) REFERENCES omie_cliente(id_cliente)
);

-- NFe (itens)
CREATE TABLE IF NOT EXISTS omie_nf_item (
  chave_nfe         TEXT NOT NULL,
  sequencia_item    INT  NOT NULL,
  id_produto        TEXT,
  codigo_produto    TEXT,
  descricao_produto TEXT,
  quantidade        NUMERIC(14,3),
  valor_unitario    NUMERIC(14,2),
  PRIMARY KEY (chave_nfe, sequencia_item),
  FOREIGN KEY (chave_nfe) REFERENCES omie_nf(chave_nfe)
);

-- Amarração NF → Pedido (preencher quando souber)
CREATE TABLE IF NOT EXISTS rel_nf_pedido (
  chave_nfe         TEXT NOT NULL,
  numero_pedido     TEXT NOT NULL,
  origem            TEXT,
  PRIMARY KEY (chave_nfe, numero_pedido),
  FOREIGN KEY (chave_nfe) REFERENCES omie_nf(chave_nfe),
  FOREIGN KEY (numero_pedido) REFERENCES omie_pedido(numero_pedido)
);

-- Índices úteis
CREATE INDEX IF NOT EXISTS idx_pedidos_cliente_emissao ON omie_pedido(id_cliente, data_emissao);
CREATE INDEX IF NOT EXISTS idx_nf_numero_serie ON omie_nf(numero_nf, serie);
CREATE INDEX IF NOT EXISTS idx_item_pedido_codigo ON omie_pedido_item(codigo_produto);
