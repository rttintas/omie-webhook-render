import os
import json
import asyncio
from typing import Any, Dict, Optional

import asyncpg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

APP_NAME = "omie-webhook-render"

DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")

app = FastAPI(title=APP_NAME)
pool: Optional[asyncpg.Pool] = None


# ------------- util ------------- #
async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        # Render/Neon normalmente exigem SSL.
        pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=60,
            ssl="require"
        )
        # valida conexão e esquema
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1;")
    return pool


async def detect_xml_column(conn: asyncpg.Connection) -> str:
    """
    Descobre se a coluna na tabela public.omie_nfe_xml chama 'xml' ou 'nfe_xml'.
    Default: 'xml'.
    """
    rows = await conn.fetch("""
        select column_name
        from information_schema.columns
        where table_schema='public'
          and table_name='omie_nfe_xml'
          and column_name in ('xml','nfe_xml')
        order by case when column_name='xml' then 0 else 1 end
        limit 1;
    """)
    return rows[0]["column_name"] if rows else "xml"


def get_json_field(data: Dict[str, Any], *caminhos, default=None):
    """
    Busca um campo em diferentes caminhos/chaves.
    Ex.: get_json_field(payload, ("evento","numero_nf"), ("evento","numeroNf"))
    """
    for path in caminhos:
        cur = data
        try:
            for k in path:
                cur = cur[k]
            if cur is not None:
                return cur
        except Exception:
            pass
    return default


# ------------- health ------------- #
@app.get("/health/app")
async def health_app():
    return {"ok": True, "name": APP_NAME}

@app.get("/health/db")
async def health_db():
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("select 1 as select1;")
        return {"db": "ok", "select1": row["select1"]}


# ------------- webhook ------------- #
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: Optional[str] = None):
    # se você quiser, valide o token aqui
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    # Alguns provedores mandam string JSON dentro de string
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            pass

    topic = get_json_field(payload, ("topic",), ("assunto",), default=None)
    evento: Dict[str, Any] = get_json_field(payload, ("evento",), default={})
    message_id = get_json_field(payload, ("messageId",), ("idMensagem",), default="-")

    p = await get_pool()
    async with p.acquire() as conn:
        # importante: cada chamada com sua transação
        async with conn.transaction():
            if topic == "VendaProduto.Incluida":
                numero_pedido = get_json_field(
                    payload, ("evento", "numeroPedido"), ("evento", "numero_pedido")
                )
                if not numero_pedido:
                    raise HTTPException(status_code=400, detail="numeroPedido ausente")

                # upsert de pedido (NENHUM XML AQUI)
                await conn.execute(
                    """
                    insert into public.omie_pedido (numero, recebido_em)
                    values ($1, timezone('UTC', now()))
                    on conflict (numero) do update
                       set recebido_em = excluded.recebido_em,
                           updated_at  = timezone('UTC', now());
                    """,
                    str(numero_pedido),
                )
                return JSONResponse(
                    {"status": "success", "message": f"Pedido {numero_pedido} salvo/atualizado."}
                )

            elif topic == "NFe.NotaAutorizada":
                # dados que podem vir
                numero_nf = get_json_field(evento, ("numero_nf",), ("numeroNf",))
                chave_nfe = get_json_field(evento, ("chave_nfe",), ("chaveNfe",), ("chave",))
                emitida_em = get_json_field(evento, ("emitida_em",), ("emitidaEm",))
                # XML pode vir com nomes diferentes
                xml_body = get_json_field(evento, ("xml",), ("nfe_xml",), ("nfeXml",))

                # 1) Salva/atualiza metadados de NF na omie_nfe (NADA de XML aqui)
                if numero_nf:
                    await conn.execute(
                        """
                        insert into public.omie_nfe (numero, recebido_em, updated_at)
                        values ($1, timezone('UTC', now()), timezone('UTC', now()))
                        on conflict (numero) do update
                           set recebido_em = excluded.recebido_em,
                               updated_at  = excluded.updated_at;
                        """,
                        str(numero_nf),
                    )

                if chave_nfe:
                    # Se já houver chave e a linha existir, completa/atualiza outros campos
                    await conn.execute(
                        """
                        insert into public.omie_nfe (chave_nfe, emitida_em, recebido_em, updated_at, numero)
                        values ($1, $2, timezone('UTC', now()), timezone('UTC', now()), $3)
                        on conflict (chave_nfe) do update
                           set emitida_em = coalesce(excluded.emitida_em, public.omie_nfe.emitida_em),
                               numero     = coalesce(excluded.numero, public.omie_nfe.numero),
                               recebido_em = excluded.recebido_em,
                               updated_at  = excluded.updated_at;
                        """,
                        str(chave_nfe),
                        emitida_em,
                        str(numero_nf) if numero_nf else None,
                    )

                # 2) Se houver XML e chave, salva na omie_nfe_xml
                if xml_body and chave_nfe:
                    xml_col = await detect_xml_column(conn)  # 'xml' ou 'nfe_xml'
                    # monta SQL dinamicamente APENAS para a tabela de XML
                    sql = f"""
                        insert into public.omie_nfe_xml (chave_nfe, {xml_col}, recebido_em)
                        values ($1, $2, timezone('UTC', now()))
                        on conflict (chave_nfe) do update
                           set {xml_col}  = excluded.{xml_col},
                               recebido_em = excluded.recebido_em;
                    """
                    await conn.execute(sql, str(chave_nfe), str(xml_body))

                return JSONResponse(
                    {"status": "success", "message": f"NF {numero_nf or chave_nfe or ''} salva/atualizada."}
                )

            else:
                # tópico que não mapeamos
                return JSONResponse({"status": "ignored", "topic": topic or "desconhecido"})

    # fallback
    return {"ok": True, "messageId": message_id}
