import os
import json
import asyncpg
import logging
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException

# Configuração do logger
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# Variável global para o pool de conexões
connection_pool = None

async def get_connection():
    """Retorna uma conexão do pool."""
    global connection_pool
    if connection_pool is None:
        try:
            DB_URL = os.environ.get("DATABASE_URL")
            if not DB_URL:
                logging.error("Variável de ambiente DATABASE_URL não encontrada.")
                raise HTTPException(status_code=500, detail="Erro de configuração")
            connection_pool = await asyncpg.create_pool(DB_URL)
        except Exception as e:
            logging.error(f"Não foi possível criar o pool de conexões: {e}")
            raise HTTPException(status_code=500, detail="Erro de conexão com o banco de dados")
    return connection_pool

async def _insert_pedido_event(payload: dict):
    """Insere um evento de pedido na tabela omie_pedido."""
    try:
        # Pega a conexão do pool
        conn = await get_connection()
        
        # Converte o payload para JSON string para a coluna 'raw'
        payload_json = json.dumps(payload, default=str)
        
        sql = """
            INSERT INTO public.omie_pedido (
                event_type,
                origin,
                raw,
                timestamp
            ) VALUES (
                $1, $2, $3, $4
            )
            RETURNING id
        """
        
        row_id = await conn.fetchval(
            sql,
            payload.get("tópico", "desconhecido"),
            payload.get("origem", "desconhecida"),
            payload_json,
            datetime.now()
        )
        return row_id
    except asyncpg.exceptions.PostgresError as e:
        logging.error(f"Erro no banco de dados para a tabela omie_pedido: {e}")
        raise HTTPException(status_code=500, detail="Erro ao inserir evento")

async def _insert_nfe_event(payload: dict):
    """Insere um evento de NFe na tabela omie_nfe, mapeando as colunas."""
    try:
        conn = await get_connection()
        
        # O payload do webhook de NFe já é um dicionário com os dados
        evento = payload.get("evento", {})
        
        sql = """
            INSERT INTO public.omie_nfe (
                nChave,
                nIdNF,
                nIdPedido,
                nNumero,
                nValor,
                dEmissao,
                hEmissao,
                cXml
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING id
        """
        
        # Mapeia os dados do webhook para as colunas da tabela omie_nfe
        row_id = await conn.fetchval(
            sql,
            evento.get("nfe_chave"),
            evento.get("id_nf"),
            evento.get("id_pedido"),
            evento.get("numero_nf"),
            evento.get("valor_nf"), # Ajuste: o payload JSON tem valor_nf? Se não, remova. Ou use o valor da API
            evento.get("data_emis"),
            evento.get("hora_emis"),
            evento.get("nfe_xml") # Salva o link do XML, não o conteúdo do XML
        )
        return row_id
    except asyncpg.exceptions.PostgresError as e:
        logging.error(f"Erro no banco de dados para a tabela omie_nfe: {e}")
        raise HTTPException(status_code=500, detail="Erro ao inserir evento")

# ---
## Endpoints do Webhook

@app.post("/omie/webhook")
async def omie_webhook_receiver(request: Request):
    """Endpoint para receber webhooks da Omie."""
    try:
        data = await request.json()
        
        # AQUI ESTÁ O AJUSTE PRINCIPAL:
        # Verifica o 'tópico' do webhook para saber onde salvar
        topic = data.get("tópico")
        
        if topic == "NFe.NotaAutorizada":
            event_id = await _insert_nfe_event(data)
            logging.info(f"Webhook de Nota Fiscal inserido com sucesso: ID {event_id}")
            
        elif topic == "Pedidos.PedidoFaturado": # Ou outro tópico de pedido
            event_id = await _insert_pedido_event(data)
            logging.info(f"Webhook de Pedido inserido com sucesso: ID {event_id}")
            
        else:
            logging.warning(f"Tópico de webhook não reconhecido: {topic}")
            raise HTTPException(status_code=400, detail="Tópico de webhook não suportado")
            
        return {"status": "ok", "event_id": event_id}
        
    except Exception as e:
        logging.error(f"Erro geral no webhook da Omie: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

# O endpoint XML/omie/webhook parece ser para outra finalidade, talvez para receber o XML em si.
# Se for o caso, ele precisa de uma lógica diferente.
# Se a API de busca de XML for chamada pelo seu código, então o código deve estar em outra função.
# Deixe este endpoint para o caso de um webhook específico de XML, mas o payload que você enviou
# não é o que se espera de um XML puro, mas de um JSON com links.
# Com o ajuste acima, o primeiro webhook já deve salvar o link do XML na tabela omie_nfe.

@app.get("/")
async def health_check():
    """Endpoint de checagem de saúde."""
    return {"status": "ok"}