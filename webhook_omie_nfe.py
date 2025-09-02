import os
import json
import asyncpg
from fastapi import FastAPI, Request, HTTPException
from typing import Optional

# Inicializa o aplicativo FastAPI
app = FastAPI()

# Configura o pool de conexão com o banco de dados
_pool = None

@app.on_event("startup")
async def startup():
    global _pool
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        try:
            _pool = await asyncpg.create_pool(db_url)
            print("INFO: Conexão com o banco de dados estabelecida com sucesso!")
        except Exception as e:
            print(f"ERRO CRÍTICO: Falha ao conectar ao banco de dados: {e}")
            raise e

@app.on_event("shutdown")
async def shutdown():
    if _pool:
        await _pool.close()

# Rota para receber os webhooks da Omie
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str):
    if token != os.environ.get("WEBHOOK_TOKEN"):
        raise HTTPException(status_code=401, detail="Token de autenticação inválido.")

    try:
        payload = await request.json()
        print(f"Payload recebido: {json.dumps(payload, indent=2)}")

        # Verifica o tipo de evento usando a chave 'topic'
        evento_topic = payload.get('topic', '').lower()
        evento_data = payload.get('evento', {})

        # Processa o evento de Nota Fiscal
        if evento_topic == "nfe.notaautorizada":
            numero_nf = evento_data.get('numero_nf')
            raw_data = json.dumps(payload)
            if numero_nf and _pool:
                async with _pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO public.omie_nfe (numero, raw)
                        VALUES ($1, $2)
                        ON CONFLICT (numero) DO UPDATE SET raw = EXCLUDED.raw;
                        """,
                        numero_nf, raw_data
                    )
                return {"status": "success", "message": f"Nota Fiscal {numero_nf} salva com sucesso."}

        # Processa o evento de Pedido de Venda
        elif evento_topic in ("vendaproduto.faturada", "vendaproduto.etapaalterada", "vendaproduto.incluida"):
            numero_pedido = evento_data.get('numeroPedido')
            raw_data = json.dumps(payload)
            if numero_pedido and _pool:
                async with _pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO public.omie_pedido (numero, raw)
                        VALUES ($1, $2)
                        ON CONFLICT (numero) DO UPDATE SET raw = EXCLUDED.raw;
                        """,
                        numero_pedido, raw_data
                    )
                return {"status": "success", "message": f"Pedido de Venda {numero_pedido} salvo com sucesso."}
        
        # Lida com eventos não suportados
        else:
            return {"status": "ignored", "message": f"Tipo de evento '{evento_topic}' não suportado."}
    
    except json.JSONDecodeError:
        print("INFO: Recebida requisição com payload inválido/vazio.")
        return {"status": "error", "message": "Payload não é um JSON válido. Ignorando."}
    
    except Exception as e:
        print(f"Erro ao processar o webhook: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor.")