import os
import json
import asyncpg
import logging
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException

# Configuração do logger
logging.basicConfig(level=logging.INFO)

# Instância da aplicação FastAPI
app = FastAPI()

# Configuração do pool de conexões do banco de dados
DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    logging.error("Variável de ambiente DATABASE_URL não encontrada.")
    exit(1)

# Variável global para o pool de conexões
connection_pool = None

async def get_connection():
    """Retorna uma conexão do pool."""
    global connection_pool
    if connection_pool is None:
        try:
            connection_pool = await asyncpg.create_pool(DB_URL)
        except Exception as e:
            logging.error(f"Não foi possível criar o pool de conexões: {e}")
            raise HTTPException(status_code=500, detail="Erro de conexão com o banco de dados")
    return connection_pool

async def _insert_event(
    event_type: str,
    origin: str,
    payload: dict,
    timestamp: datetime
):
    """Insere um evento de webhook no banco de dados."""
    
    # Solução principal: Converte o dicionário 'payload' em uma string JSON
    try:
        # AQUI ESTÁ A CHAVE: usamos json.dumps para serializar o dicionário
        payload_json = json.dumps(payload, default=str)
    except TypeError as e:
        logging.error(f"Erro ao converter o payload para JSON: {e}")
        raise ValueError("Payload inválido para serialização JSON")

    sql = """
        INSERT INTO public.omie_nfe (
            event_type,
            origin,
            raw,
            timestamp
        ) VALUES (
            $1, $2, $3, $4
        )
        RETURNING id
    """
    
    conn = await get_connection()
    try:
        row_id = await conn.fetchval(
            sql,
            event_type,
            origin,
            payload_json,  # Passa a string JSON para a consulta
            timestamp
        )
        return row_id
    except asyncpg.exceptions.DataError as e:
        logging.error(f"Erro de dados no banco de dados: {e}")
        raise HTTPException(status_code=500, detail="Erro ao inserir evento")

# ---
## Endpoints do Webhook

@app.post("/omie/webhook")
async def pedidos_webhook(request: Request):
    """Endpoint para receber webhooks de pedidos Omie (JSON)."""
    try:
        data = await request.json()
        event_id = await _insert_event(
            event_type="pedido_atualizado",
            origin="omie_pedidos",
            payload=data,
            timestamp=datetime.now()
        )
        logging.info(f"Evento de pedido inserido com sucesso: ID {event_id}")
        return {"status": "ok", "event_id": event_id}
    except Exception as e:
        logging.error(f"Erro no webhook de pedidos: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@app.post("/xml/omie/webhook")
async def xml_webhook(request: Request):
    """Endpoint para receber webhooks de XML Omie (XML)."""
    try:
        # Lê o corpo da requisição como texto bruto (XML)
        data = await request.body()
        event_id = await _insert_event(
            event_type="xml_nota_fiscal",
            origin="omie_xml",
            payload={"xml_content": data.decode("utf-8")}, # Salva o XML como parte de um dicionário
            timestamp=datetime.now()
        )
        logging.info(f"Evento de XML inserido com sucesso: ID {event_id}")
        return {"status": "ok", "event_id": event_id}
    except Exception as e:
        logging.error(f"Erro no webhook de XML: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@app.get("/")
async def health_check():
    """Endpoint de checagem de saúde."""
    return {"status": "ok"}