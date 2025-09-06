# app_combined.py — combina Pedidos + NF-e/XML
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# serviço de pedidos (existente)
from main import app as pedidos_app

# serviço NF-e/XML (dedicado) e função de tratamento (caso você use modo compat em /omie/webhook)
from main_xml import app as nfe_app

app = FastAPI(title="RTT Omie - Combined")

@app.get("/")
def root():
    return JSONResponse({
        "status": "ok",
        "services": {"pedidos": {"mounted_at": "/"}, "nfe_xml": {"mounted_at": "/xml"}},
        "compat": {"nfe_on_pedidos_webhook": False}  # agora estamos em endpoint dedicado
    })

@app.get("/healthz")
def healthz():
    return {"status": "healthy", "components": ["pedidos", "nfe_xml"], "compat": False}

# Monte primeiro /xml (para não ser engolido pelo app raiz)
app.mount("/xml", nfe_app)

# Depois o serviço de pedidos na raiz
app.mount("/", pedidos_app)
