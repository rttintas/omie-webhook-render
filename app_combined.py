# app_combined.py — combina Pedidos + NF-e/XML
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# seu serviço de pedidos (existente)
from main import app as pedidos_app

# serviço NF-e/XML e função compat
from main_xml import app as nfe_app, handle_xml_event_from_dict as handle_nfe_dict

app = FastAPI(title="RTT Omie - Combined")

@app.get("/")
def root():
    return JSONResponse({
        "status": "ok",
        "services": {"pedidos": {"mounted_at": "/"}, "nfe_xml": {"mounted_at": "/xml"}},
        "compat": {"nfe_on_pedidos_webhook": True}
    })

@app.get("/healthz")
def healthz():
    return {"status": "healthy", "components": ["pedidos", "nfe_xml"], "compat": True}

# Modo compat:
# - Se for POST em /omie/webhook e o corpo tiver cara de NF-e, despacha pro módulo XML.
@app.middleware("http")
async def nfe_compat_middleware(request: Request, call_next):
    if request.url.path == "/omie/webhook" and request.method.upper() == "POST":
        try:
            body_bytes = await request.body()  # cacheado pelo Starlette
            body_text = body_bytes.decode("utf-8", "ignore")
            if any(k in body_text for k in ["nfe_chave", "chNFe", "chaveNFe", "nfe_xml", "numero_nf", "id_nf"]):
                try:
                    parsed = json.loads(body_text) if body_text else {}
                except Exception:
                    parsed = {}
                result = await handle_nfe_dict(parsed)
                return JSONResponse(result, status_code=200)
        except Exception:
            # heurística falhou? deixa o app de pedidos tratar
            pass
        return await call_next(request)

    # demais rotas seguem o fluxo normal
    return await call_next(request)

# Monte primeiro /xml (para não ser “engolido” pelo app raiz)
app.mount("/xml", nfe_app)

# Depois monte o serviço de pedidos na raiz
app.mount("/", pedidos_app)

# Start (Render): uvicorn app_combined:app --host 0.0.0.0 --port $PORT
