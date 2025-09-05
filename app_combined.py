# app_combined.py — combinação dos dois serviços FastAPI com modo compat NF-e no mesmo endpoint
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# seu serviço de pedidos (já existente)
from main import app as pedidos_app

# serviço NF-e/XML e função compat para tratar dict diretamente
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

# Middleware COMPAT:
# - Se for POST em /omie/webhook e o corpo parecer NF-e, tratamos pelo módulo XML;
# - Caso contrário, deixamos seguir para o app de pedidos normalmente.
@app.middleware("http")
async def nfe_compat_middleware(request: Request, call_next):
    if request.url.path == "/omie/webhook" and request.method.upper() == "POST":
        try:
            body_bytes = await request.body()            # cacheia o corpo
            body_text = body_bytes.decode("utf-8", "ignore")
            # Heurística simples de detecção de NF-e
            if any(k in body_text for k in ["nfe_chave", "chNFe", "chaveNFe", "nfe_xml", "numero_nf", "id_nf"]):
                try:
                    parsed = json.loads(body_text) if body_text else {}
                except Exception:
                    parsed = {}
                result = await handle_nfe_dict(parsed)
                return JSONResponse(result, status_code=200)
        except Exception:
            # Em caso de erro na heurística, apenas deixa seguir
            pass
        return await call_next(request)

    # Demais rotas seguem o fluxo normal
    return await call_next(request)

# Monte primeiro /xml (serviço NFe com endpoints próprios ex.: /xml/admin/nfe/reprocessar-pendentes)
app.mount("/xml", nfe_app)

# Depois monte o serviço de pedidos na raiz (mantém endpoints existentes)
app.mount("/", pedidos_app)

# Start (Render): uvicorn app_combined:app --host 0.0.0.0 --port $PORT
