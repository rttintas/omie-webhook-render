# main_xml.py — serviço dedicado de NF-e/XML
import os
import json
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
APP_NAME = "RTT Omie XML"
WEBHOOK_TOKEN = os.getenv("OMIE_WEBHOOK_TOKEN_XML", "").strip()  # ex: "tiago-nati"

app = FastAPI(title=APP_NAME)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _check_xml_token(token_from_query: str):
    """
    Valida o token do webhook vindo pela querystring (?token=...).
    Se OMIE_WEBHOOK_TOKEN_XML estiver setado e não bater, retorna 401.
    """
    if WEBHOOK_TOKEN and token_from_query != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/healthz")
def healthz():
    return {"status": "healthy", "service": "nfe_xml"}


# -----------------------------------------------------------------------------
# Lógica principal (ajuste aqui se quiser manter sua regra atual)
# -----------------------------------------------------------------------------
async def handle_xml_event_from_dict(payload: dict) -> dict:
    """
    <<< PONTO ÚNICO PARA SUA LÓGICA >>>
    Se já tinha uma função com esse nome no seu antigo main_xml.py,
    pode mover sua lógica para cá (parse, gravação no banco etc).

    O que está abaixo é só um retorno OK para não quebrar o fluxo.
    """
    # Exemplo: normaliza campos comuns caso a Omie envie com nomes diferentes
    chave = (
        payload.get("nfe_chave")
        or payload.get("chave")
        or payload.get("chNFe")
        or payload.get("chaveNFe")
    )

    # Retorno padrão (adequado para o webhook da Omie entender que aceitamos o evento)
    return {
        "ok": True,
        "received": bool(payload),
        "chave": chave,
    }


# -----------------------------------------------------------------------------
# Webhook de NF-e: GET (validação da Omie) + POST (eventos reais)
# -----------------------------------------------------------------------------
@app.get("/omie/webhook")
async def omie_xml_webhook_ping(token: str = Query(default="")):
    """
    A Omie chama um GET para validar o endpoint. Precisa responder 200.
    """
    _check_xml_token(token)
    return {"ok": True, "service": "omie_xml", "mode": "ping"}


@app.post("/omie/webhook")
async def omie_xml_webhook(request: Request, token: str = Query(default="")):
    """
    Recebe os eventos reais (POST) da Omie.
    """
    _check_xml_token(token)

    try:
        body = await request.json()
        if not isinstance(body, dict):
            # a Omie pode mandar array dependendo do tópico; façamos um wrap simples
            body = {"_raw": body}
    except Exception:
        body = {}

    result = await handle_xml_event_from_dict(body)
    return JSONResponse(result, status_code=200)
