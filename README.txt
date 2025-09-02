# Automação Lista & Etiquetas (Omie + PDFs)

## Pastas (Windows)
- Entrada de NFs (PDFs): `G:\Meu Drive\Produção Ecommerce\Entrada Pdfs`
- Saída (listas & etiquetas): `G:\Meu Drive\Produção Ecommerce\Saida Listas Etiquetas`
- Scripts: `G:\Meu Drive\AutomacoesOmie\Arquivos necessários para Automação lista rodar`

> Os caminhos acima já estão **fixos** nos scripts.

## Requisitos
- Python 3.10+
- `pip install -r requirements.txt`
- Acesso ao seu banco Postgres (Neon) contendo:
  - `vw_nf_pedido_expandida` (que traz: nf_numero, pedido_id, numero_pedido, cliente_nome, marketplace, received_at)
  - Tabelas `omie_pedido_itens` (para extrair *descrição do pedido* e **cor**)
  - Tabela de log: `public.etiqueta_log` (script em `setup_sql\01_tabela_etiqueta_log.sql`)

## Configurar conexão ao Postgres
Edite **`util_db.py`** e preencha as variáveis `PG_*` (ou use variáveis de ambiente).

## Como usar
1) Coloque os PDFs das NFs na pasta **Entrada Pdfs**. Regras:
   - O arquivo precisa ter **≥ 100 KB**
   - O **nome do arquivo** precisa conter o número da NF (ex.: `NF_00009251.pdf` ou `00009251_qualquercoisa.pdf`).

2) Rode (no Windows, PowerShell ou CMD):
```
cd "G:\Meu Drive\AutomacoesOmie\Arquivos necessários para Automação lista rodar"
python orquestrador.py
```
Isso vai:
- Ler as NFs a partir dos PDFs válidos
- Consultar o banco
- Gerar:
  - `lista_separacao_YYYYMMDD_HHMM.pdf` (A4, cabeçalho com total de pedidos e total de itens, e subtotais por NF)
  - `etiquetas_78x48_YYYYMMDD_HHMM.pdf` (3 etiquetas por página A4, formato 78x48mm)
- Gravar **log** na tabela `public.etiqueta_log` para impedir duplicidade

3) **Reimpressão** (manual):
- Você pode reimprimir usando a *query de exemplo* no arquivo `setup_sql\02_consultas_base.sql`
  (substitua a data/NF conforme a necessidade) ou ajustando `orquestrador.py` para ignorar o log.

## Observações
- A extração da **cor** é feita a partir da **descrição do item** em `omie_pedido_itens.descricao`,
  procurando termos **depois** de “800ml”, “3,2l” ou “16l” (case-insensitive).
- Caso o padrão esteja diferente nos seus dados, ajuste a função `extract_color` em `gera_etiquetas.py`.
