# ---------- webhook ----------
@app.post("/omie/webhook")
async def omie_webhook(
    request: Request,
    token: Optional[str] = Query(None, description="Token simples via querystring")
):
    # ATENÇÃO: ESTE É UM CÓDIGO TEMPORÁRIO PARA DIAGNÓSTICO
    # Ele desativa a validação de segurança. NÃO USE EM PRODUÇÃO.
    
    # 1. Este trecho irá imprimir os tokens nos seus logs
    print(f"Token recebido na URL: '{token}'")
    print(f"Token esperado: '{os.environ.get('WEBHOOK_TOKEN')}'")

    # 2. Desativa temporariamente a validação. O status será sempre 200.
    # if os.environ.get("WEBHOOK_TOKEN") and token != os.environ.get("WEBHOOK_TOKEN"):
    #     raise HTTPException(status_code=401, detail="Token inválido")

    # O resto do código continua igual.
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {"_raw": body}
    except Exception:
        raw = (await request.body()).decode("utf-8", errors="ignore")
        body = {"_raw": raw}
    
    event_type = _extract_event_type(body)
    event_id = body.get("event_id") or body.get("id") or body.get("guid") or _hash_event(body)
    headers = dict(request.headers)

    if not _pool:
        return {"ok": True, "warning": "PG_DSN ausente; evento não foi persistido", "event_type": event_type}

    try:
        async with _pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO public.omie_webhook_events
                    (received_at, event_type, event_id, payload, raw_headers, http_status)
                VALUES
                    (now(), $1, $2, $3::jsonb, $4::jsonb, 200)
                """,
                event_type,
                event_id,
                json.dumps(body, ensure_ascii=False),
                json.dumps(headers, ensure_ascii=False),
            )
        return {"ok": True, "event_type": event_type, "stored": True}
    except Exception as e:
        return JSONResponse(
            {"ok": True, "event_type": event_type, "stored": False, "error": str(e)[:300]},
            status_code=200,
        )
