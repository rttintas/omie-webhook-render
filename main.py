# main.py
import os
import json
import logging
from datetime import datetime
from typing import Optional

import asyncpg
from fastapi import FastAPI, Request, HTTPException

# tenta carregar variáveis de um .env quando rodando local
try:  # não falha no Render caso python-dotenv não esteja instalado
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

app = FastAPI(title="Omie Webhook API")

_pool: Optional[asyncpg.pool.Pool] = None  # conexão global (pool)


# ------------------------------- utils/helpers ------------------------------- #
def get_env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise RuntimeError(f"Variável de ambiente ausente: {name}")
    return v


async def _parse_payload(request: Request):
    """
    Aceita:
    - application/json
    - application/x-www-form-urlencoded ou multipart/form-data
      * JSON string em 'payload' / 'data' / 'json' / 'mensagem' / 'message'
      * ou campos soltos (numeroPedido, numero_nf, chave_nfe, etc.)
    """
    ct = (request.headers.get("content-type") or "").lower()
    data = None

    # 1) JSON puro
    if "application/json" in ct:
        try:
            data = await request.json()
        except Exception:
            data = None

    # 2) Form-encoded / multipart
    if not data and ("application/x-www-form-urlencoded" in ct or "multipart/form-data" in ct):
        form = await request.form()

        # 2a) JSON dentro de um campo
        for key in ("payload", "data", "json", "mensagem", "message"):
            if key in form and form[key]:
                try:
                    data = json.loads(form[key])
                    break
                except Exception:
                    pass

        # 2b) Campos avulsos -> construir dicionário compatível
        if not data:
            d = dict(form)
            evento = {}
            for k in (
                "numeroPedido",
                "numero_nf",
                "nfe_num",
                "chave_nfe",
                "nfe_xml",
                "nfe_danfe",
                "operacao",
                "serie",
            ):
                if k in d and d[k] not in (None, "", "null"):
                    evento[k] = d[k]

            if evento:
                topic = d.get("topic")
                if not topic:
                    # inferir tópico
                    if "numeroPedido" in evento:
                        topic = "VendaProduto.Incluida"
                    else:
                        topic = "NFe.NotaAutorizada"
                data = {
                    "messageId": d.get("messageId")
                    or d.get("id_pedido")
                    or d.get("id_nf")
                    or f"auto-{int(datetime.now().timestamp())}",
                    "topic": topic,
                    "evento": evento,
                }

    # 3) fallback: tentar interpretar corpo como json
    if not data:
        raw = (await request.body()).decode("utf-8", "ignore").strip()
        if raw:
            try:
                data = json.loads(raw)
            except Exception:
                data = None

    return data


async def ensure_schema(conn: asyncpg.Connection) -> None:
    """
    Garante que as tabelas/índices usados pelo código existam.
    Não mexe em PKs/constraints existentes (idempotente).
    """
    # omie_pedido
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.omie_pedido (
            id_pedido BIGSERIAL PRIMARY KEY,
            numero TEXT UNIQUE,
            raw JSONB,
            recebido_em TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_pedido_numero
          ON public.omie_pedido (numero) WHERE numero IS NOT NULL;

        -- colunas novas (idempotente)
        ALTER TABLE public.omie_pedido
          ADD COLUMN IF NOT EXISTS raw JSONB,
          ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now(),
          ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
        """
    )

    # omie_nfe
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.omie_nfe (
            id_nfe BIGSERIAL PRIMARY KEY,
            numero TEXT,
            chave_nfe TEXT,
            nfe_xml TEXT,
            nfe_danfe TEXT,
            emitida_em TIMESTAMPTZ,
            raw JSONB,
            recebido_em TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        );

        -- índices únicos parciais (permitem chave_nfe = NULL)
        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_nfe_numero
          ON public.omie_nfe (numero) WHERE numero IS NOT NULL;

        CREATE UNIQUE INDEX IF NOT EXISTS ux_omie_nfe_chave
          ON public.omie_nfe (chave_nfe) WHERE chave_nfe IS NOT NULL;

        -- colunas novas (idempotente)
        ALTER TABLE public.omie_nfe
          ADD COLUMN IF NOT EXISTS raw JSONB,
          ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now(),
          ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
        """
    )


# ----------------------------- lifecycle events ------------------------------ #
@app.on_event("startup")
async def on_startup():
    global _pool
    db_url = get_env("DATABASE_URL")
    _pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5)
    async with _pool.acquire() as conn:
        await ensure_schema(conn)
    logging.info("Pool OK & schema verificado.")


@app.on_event("shutdown")
async def on_shutdown():
    global _pool
    if _pool:
        await _pool.close()


# -------------------------------- healthchecks -------------------------------- #
@app.get("/health/app")
async def health_app():
    return {"ok": True, "name": "omie-webhook-render"}


@app.get("/health/db")
async def health_db():
    global _pool
    if not _pool:
        raise HTTPException(status_code=503, detail="pool indisponível")
    async with _pool.acquire() as conn:
        row = await conn.fetchval("SELECT 1;")
    return {"db": "ok", "select1": row}


# --------------------------------- webhook ----------------------------------- #
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: Optional[str] = None):
    # valida token
    expected = get_env("WEBHOOK_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=401, detail="token inválido")

    payload = await _parse_payload(request)
    if not payload:
        logging.info("Payload inválido/vazio. ct=%s", request.headers.get("content-type"))
        return {"status": "error", "message": "payload inválido/vazio"}

    logging.info("Payload recebido: %s", json.dumps(payload, ensure_ascii=False))

    topic = (payload.get("topic") or "").lower()
    evento = payload.get("evento") or {}

    global _pool
    if not _pool:
        raise HTTPException(status_code=503, detail="pool indisponível")

    # ------------------------------ Pedido (venda) ------------------------------ #
    if "venda" in topic or "vendaproduto" in topic or "pedido" in topic or "incluida" in topic:
        numero = (
            evento.get("numeroPedido")
            or payload.get("numeroPedido")
            or payload.get("numero")
        )
        if not numero:
            raise HTTPException(status_code=400, detail="numeroPedido ausente no evento")

        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO public.omie_pedido (numero, raw, recebido_em, updated_at)
                VALUES ($1, $2::jsonb, now(), now())
                ON CONFLICT (numero)
                DO UPDATE SET
                  raw = EXCLUDED.raw,
                  recebido_em = now(),
                  updated_at = now();
                """,
                str(numero),
                json.dumps(payload, ensure_ascii=False),
            )

        return {"status": "success", "message": f"Pedido {numero} salvo/atualizado."}

    # ------------------------------ NF-e autorizada ----------------------------- #
    # cobre tópicos como 'NFe.NotaAutorizada' e variações
    if "nfe" in topic or "notaautorizada" in topic or "nota_autorizada" in topic:
        numero_nf = (
            evento.get("numero_nf")
            or evento.get("nfe_num")
            or payload.get("numero_nf")
            or payload.get("numero")
        )
        chave_nfe = evento.get("chave_nfe") or payload.get("chave_nfe")
        nfe_xml = evento.get("nfe_xml") or payload.get("nfe_xml")
        nfe_danfe = evento.get("nfe_danfe") or payload.get("nfe_danfe")

        # tenta mapear uma data de emissão, se vier
        emitida_em: Optional[datetime] = None
        for key in ("emitida_em", "emissao", "emitido_em"):
            val = evento.get(key) or payload.get(key)
            if isinstance(val, str) and val.strip():
                try:
                    emitida_em = datetime.fromisoformat(val.replace("Z", "+00:00"))
                    break
                except Exception:
                    pass

        if not numero_nf and not chave_nfe:
            raise HTTPException(
                status_code=400,
                detail="numero_nf ou chave_nfe ausentes no evento",
            )

        async with _pool.acquire() as conn:
            # preferimos conflitar por numero (índice parcial assegura unicidade quando numero_nf não é nulo)
            await conn.execute(
                """
                INSERT INTO public.omie_nfe (numero, chave_nfe, nfe_xml, nfe_danfe, emitida_em, raw, recebido_em, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, now(), now())
                ON CONFLICT (numero)
                DO UPDATE SET
                  chave_nfe = COALESCE(EXCLUDED.chave_nfe, public.omie_nfe.chave_nfe),
                  nfe_xml   = COALESCE(EXCLUDED.nfe_xml,   public.omie_nfe.nfe_xml),
                  nfe_danfe = COALESCE(EXCLUDED.nfe_danfe, public.omie_nfe.nfe_danfe),
                  emitida_em= COALESCE(EXCLUDED.emitida_em,public.omie_nfe.emitida_em),
                  raw       = EXCLUDED.raw,
                  recebido_em = now(),
                  updated_at  = now();
                """,
                str(numero_nf) if numero_nf else None,
                str(chave_nfe) if chave_nfe else None,
                nfe_xml,
                nfe_danfe,
                emitida_em,
                json.dumps(payload, ensure_ascii=False),
            )

        ref = numero_nf or chave_nfe
        return {"status": "success", "message": f"NF {ref} salva/atualizada."}

    # --------------------------------- default --------------------------------- #
    return {"status": "ignored", "message": f"topic não reconhecido: {payload.get('topic')}"}
