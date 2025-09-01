import os
import json
import asyncpg
from fastapi import FastAPI, Request, HTTPException

# Inicializa o aplicativo FastAPI
app = FastAPI()

# Configura o pool de conexão com o banco de dados Neon
async def get_db_connection():
    return await asyncpg.create_pool(os.environ.get("DATABASE_URL"))

db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    db_pool = await get_db_connection()

@app.on_event("shutdown")
async def shutdown():
    await db_pool.close()

# Rota para receber os webhooks da Omie
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str):
    # 1. Validação do token de segurança
      if token != os.environ.get("OMIE_WEBHOOK_TOKEN"):
        raise HTTPException(status_code=401, detail="Token de autenticação inválido.")

    try:
        # 2. Recebe e desserializa o payload JSON
        payload = await request.json()
        print(f"Payload recebido: {json.dumps(payload, indent=2)}")

        # 3. Identifica o evento do webhook
        evento = payload.get('evento')
        
        # 4. Processa o evento de Nota Fiscal
        if evento == "nfe.faturada":
            numero_nf = payload['nfe']['numero']
            raw_data = json.dumps(payload)
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO public.omie_nfe (numero, raw)
                    VALUES ($1, $2)
                    ON CONFLICT (numero) DO UPDATE SET raw = EXCLUDED.raw;
                    """,
                    numero_nf, raw_data
                )
            return {"status": "success", "message": f"Nota Fiscal {numero_nf} salva com sucesso."}

        # 5. Processa o evento de Pedido de Venda
        elif evento == "venda.produto.faturado" or evento == "venda.produto.alterada":
            numero_pedido = payload['cabecalho']['numero_pedido']
            raw_data = json.dumps(payload)
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO public.omie_pedido (numero_pedido, raw)
                    VALUES ($1, $2)
                    ON CONFLICT (numero_pedido) DO UPDATE SET raw = EXCLUDED.raw;
                    """,
                    numero_pedido, raw_data
                )
            return {"status": "success", "message": f"Pedido de Venda {numero_pedido} salvo com sucesso."}

        # 6. Lida com eventos não suportados
        else:
            return {"status": "ignored", "message": f"Tipo de evento '{evento}' não suportado."}

    except Exception as e:
        print(f"Erro ao processar o webhook: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor.")
