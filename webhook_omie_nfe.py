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
    # A URL do banco de dados é lida da variável de ambiente no Render
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
    # Validação do token de segurança
    if token != os.environ.get("WEBHOOK_TOKEN"):
        raise HTTPException(status_code=401, detail="Token de autenticação inválido.")

    try:
        # Tenta receber o payload como JSON
        payload = await request.json()
        print(f"Payload recebido: {json.dumps(payload, indent=2)}")

        # Identifica o evento do webhook
        evento = payload.get('evento')
        
        # Processa o evento de Nota Fiscal
        if evento == "nfe.faturada":
            numero_nf = payload['nfe']['numero']
            raw_data = json.dumps(payload)
            if _pool:
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
        elif evento == "venda.produto.faturado" or evento == "venda.produto.alterada":
            numero_pedido = payload['cabecalho']['numero_pedido']
            raw_data = json.dumps(payload)
            if _pool:
                async with _pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO public.omie_pedido (numero_pedido, raw)
                        VALUES ($1, $2)
                        ON CONFLICT (numero_pedido) DO UPDATE SET raw = EXCLUDED.raw;
                        """,
                        numero_pedido, raw_data
                    )
            return {"status": "success", "message": f"Pedido de Venda {numero_pedido} salvo com sucesso."}

        # Lida com eventos não suportados
        else:
            return {"status": "ignored", "message": f"Tipo de evento '{evento}' não suportado."}
    
    # Adiciona esta exceção específica para erros de JSON
    except json.JSONDecodeError:
        print("INFO: Recebida requisição com payload inválido/vazio.")
        return {"status": "error", "message": "Payload não é um JSON válido. Ignorando."}

    except Exception as e:
        print(f"Erro ao processar o webhook: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor.")
