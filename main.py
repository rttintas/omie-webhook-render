# main.py (Trecho corrigido do endpoint omie_webhook)

# ------------------------------------------------------------
# Webhook da Omie (pedidos / NFe.NotaAutorizada)
# ------------------------------------------------------------
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str = Query(default="")):
    if OMIE_WEBHOOK_TOKEN and token != OMIE_WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")

    body = await request.json()
    pool = await get_pool()

    async with pool.acquire() as conn:
        try:
            # Identificar o tipo de evento pelo 'tópico'
            topico = body.get("tópico") or body.get("topic")
            
            if topico and str(topico).lower().startswith("nfe."):
                logger.info({"tag": "omie_nf_event", "msg": "Evento de NF recebido"})
                evento_nf = body.get("evento", {})
                await upsert_nf_xml_row(conn, evento_nf)
            
            else:
                # Se não for NF, processe como um evento de pedido
                numero_any = deep_find_first(body, WANTED_NUM_KEYS)
                id_any = deep_find_first(body, WANTED_ID_KEYS)
                integ_any = deep_find_first(body, WANTED_INTEGR_KEYS)

                numero = str(numero_any) if numero_any is not None else None
                id_pedido = _as_int(id_any)
                codigo_pedido_integracao = None

                if isinstance(integ_any, str) and integ_any:
                    codigo_pedido_integracao = integ_any
                elif looks_like_integr(numero):
                    codigo_pedido_integracao = numero
                
                await insert_inbox_safe(conn, numero, id_pedido, body, codigo_pedido_integracao)
            
            logger.info(
                {"tag": "omie_webhook_processed", 
                 "numero": numero,
                 "id_pedido": id_pedido,
                 "topico": topico}
            )

        except Exception as e:
            logger.error({
                "tag": "webhook_error", 
                "err": traceback.format_exc(), 
                "body": body
            })
            raise HTTPException(status_code=500, detail=f"Erro interno: {e}")

    return {"ok": True}