# webhook_omie_nfe.py — versão mínima com json.dumps no INSERT
# Objetivo: receber POST /omie/webhook e gravar payload/headers em public.omie_webhook_events

import os
import json
import datetime as dt
import asyncpg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

APP_NAME = "omie-webhook"

PG_DSN = os.environ["PG_DSN"]            # ex: postgresql://.../neondb?sslmode=require
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN")  # opcional; se definido, valida via query ?token=

app = FastAPI(title=APP_NAME)

# ---------- startup/shutdown ----------
@app.on_event("startup")
async def startup() -> None:
    app.state.pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)

@app.on_event("shutdown")
async def shutdown() -> None:
    pool = app.state.pool
    if pool:
        await pool.close()

# ---------- healthcheck ----------
@app.get("/health")
async def health():
    try:
        async with app.state.pool.acquire() as con:
            await con.execute("select 1;")
        return {"ok": True, "app": APP_NAME, "db": True}
    except Exception:
        return JSONResponse({"ok": False, "app": APP_NAME, "db": False}, status_code=500)

# ---------- endpoint do webhook ----------
@app.post("/omie/webhook")
async def omie_webhook(req: Request):
    # valida token por querystring, se configurado
    expected = WEBHOOK_TOKEN
    provided = req.query_params.get("token")
    if expected and provided != expected:
        raise HTTPException(status_code=401, detail="Token inválido")

    # lê payload; se falhar o parse, captura o corpo bruto
    try:
        payload = await req.json()
    except Exception:
        payload = {"_raw": (await req.body()).decode("utf-8", errors="ignore")}

    headers = dict(req.headers)

    # grava apenas colunas existentes; payload/headers vão SERIALIZADOS
    try:
        async with app.state.pool.acquire() as con:
            await con.execute(
                """
                insert into public.omie_webhook_events
                    (received_at, payload, raw_headers, http_status)
                values
                    ($1, $2, $3, 200);
                """,
                dt.datetime.utcnow(),
                json.dumps(payload),   # <— ajuste: serializa dict -> JSON texto
                json.dumps(headers),   # <— ajuste: idem
            )
        return {"ok": True}
    except Exception as e:
        # retorna erro resumido (sem stacktrace)
        return JSONResponse(
            {"ok": False, "error": "db_insert_failed", "detail": str(e)[:300]},
            status_code=500,
        )

# ---------- execução local (opcional) ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("webhook_omie_nfe:app", host="0.0.0.0", port=8000)
