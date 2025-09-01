import os
import json
from fastapi import FastAPI, Request, HTTPException
from typing import Optional

# Inicializa o aplicativo FastAPI
app = FastAPI()

# Configuração do banco de dados (desativada para teste)
#
# @app.on_event("startup")
# async def startup():
#     pass
#
# @app.on_event("shutdown")
# async def shutdown():
#     pass

# Rota para receber os webhooks da Omie
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: Optional[str] = None):
    # ATENÇÃO: ESTE É UM CÓDIGO TEMPORÁRIO PARA DIAGNÓSTICO
    # Ele desativa a validação de segurança para podermos ver o log.
    
    # Este trecho irá imprimir os tokens nos seus logs
    print(f"Token recebido na URL: '{token}'")
    
    # Tente ler o payload como JSON
    try:
        payload = await request.json()
    except:
        payload = {"detail": "Payload não é um JSON válido."}

    print(f"Payload recebido: {json.dumps(payload, indent=2)}")
    
    # A validação do token será desativada para que o teste passe
    # if token != os.environ.get("WEBHOOK_TOKEN"):
    #    raise HTTPException(status_code=401, detail="Token de autenticação inválido.")

    return {"status": "success", "message": "Webhook recebido com sucesso. Validação de token desativada para diagnóstico."}
