import os
import json
from fastapi import FastAPI, Request, HTTPException
from typing import Optional

app = FastAPI()

@app.get("/")
async def read_root():
    return {"status": "ok", "message": "Serviço está no ar!"}

@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: Optional[str] = None):
    # ATENÇÃO: ESTE É UM CÓDIGO TEMPORÁRIO PARA DIAGNÓSTICO
    # Ele não se conecta a nada e não salva nada no banco de dados.
    
    print(f"Token recebido na URL: '{token}'")
    
    try:
        payload = await request.json()
    except:
        payload = {"detail": "Payload não é um JSON válido."}

    print(f"Payload recebido: {json.dumps(payload, indent=2)}")
    
    # A validação do token será desativada para que o teste passe
    # if token != os.environ.get("WEBHOOK_TOKEN"):
    #    raise HTTPException(status_code=401, detail="Token de autenticação inválido.")

    return {"status": "success", "message": "Webhook recebido com sucesso. Sem validação de token e sem banco de dados."}
