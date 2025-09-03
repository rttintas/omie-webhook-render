# main.py
import os
import json
import logging
from typing import Optional, Tuple
import urllib.parse

import asyncpg
from fastapi import FastAPI, Request, HTTPException
import httpx # Importa a biblioteca para fazer requisições HTTP

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

app = FastAPI(title="Omie Webhook API")

_pool: Optional[asyncpg.pool.Pool] = None
_http_client: Optional[httpx.AsyncClient] = None # Cliente HTTP assíncrono

# ------------------------- util/helpers ------------------------- #
def get_env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise RuntimeError(f"Variável de ambiente ausente: {name}")
    return v

async def ensure_schema(conn: asyncpg.Connection) -> None:
    # Garante que as tabelas mínimas existam e que as colunas usadas no código estejam presentes.
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.omie_pedido (
            id_pedido BIGSERIAL PRIMARY KEY,
            numero TEXT UNIQUE,
            raw JSONB,
            recebido_em TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS numero TEXT;")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS raw JSONB;")
    await conn.execute("ALTER TABLE public.omie_pedido ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now();")

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.omie_nfe (
            id BIGSERIAL PRIMARY KEY,
            numero TEXT UNIQUE,
            raw JSONB,
            recebido_em TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS numero TEXT;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS raw JSONB;")
    await conn.execute("ALTER TABLE public.omie_nfe ADD COLUMN IF NOT EXISTS recebido_em TIMESTAMPTZ DEFAULT now();")


# ------------------------- lifecycle ------------------------- #
@app.on_event("startup")
async def startup() -> None:
    global _pool, _http_client
    db_url = get_env("DATABASE_URL")
    _pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
    
    _http_client = httpx.AsyncClient(base_url="https://api.omie.com.br", timeout=30.0)
    
    async with _pool.acquire() as conn:
        await conn.execute("SELECT 1;")
        await ensure_schema(conn)
    logging.info("Pool OK & schema verificado.")


@app.on_event("shutdown")
async def shutdown() -> None:
    if _pool:
        await _pool.close()
    if _http_client:
        await _http_client.aclose()


# ------------------------- health ------------------------- #
@app.get("/health/db")
async def health_db():
    if not _pool:
        raise HTTPException(500, "Pool indisponível")
    async with _pool.acquire() as conn:
        one = await conn.fetchval("SELECT 1;")
    return {"db": "ok", "select1": one}

def _normaliza_topic(value: str) -> str:
    return (value or "").strip().lower()

def _extrai_numeros(payload: dict) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    evento = payload.get("evento", {}) or payload.get("event", {}) # Aceita 'evento' ou 'event'
    numero_pedido = evento.get("numeroPedido") or evento.get("numero_pedido") or evento.get("pedido")
    numero_nf = evento.get("numero_nf") or evento.get("numeroNf") or evento.get("numero")
    
    id_pedido = evento.get("idPedido") or evento.get("id_pedido")

    if numero_pedido is not None:
        numero_pedido = str(numero_pedido)
    if numero_nf is not None:
        numero_nf = str(numero_nf)
    return numero_pedido, numero_nf, id_pedido

async def _parse_payload(request: Request) -> Optional[dict]:
    content_type = request.headers.get("Content-Type", "")
    try:
        if "application/json" in content_type:
            return await request.json()
        elif "application/x-www-form-urlencoded" in content_type:
            body = await request.body()
            form_data = urllib.parse.parse_qs(body.decode("utf-8"))
            return {k: v[0] for k, v in form_data.items()}
        else:
            body = await request.body()
            try:
                # Tenta decodificar como JSON mesmo sem o Content-Type
                return json.loads(body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logging.warning(f"Não foi possível decodificar o corpo como JSON. Content-Type: {content_type}, Corpo: {body[:100]}...")
                return None
    except Exception as e:
        logging.error(f"Erro ao analisar o payload: {e}")
        return None

# ------------------------- webhook ------------------------- #
@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str):
    expected = get_env("WEBHOOK_TOKEN")
    if token != expected:
        raise HTTPException(status_code=401, detail="Token de autenticação inválido.")

    payload = await _parse_payload(request)
    if not payload:
        raise HTTPException(status_code=400, detail="Payload inválido ou vazio.")

    logging.info("Payload recebido: %s", json.dumps(payload, ensure_ascii=False))

    topic = _normaliza_topic(payload.get("topic", ""))
    numero_pedido, numero_nf, id_pedido = _extrai_numeros(payload)
    raw_data = json.dumps(payload, ensure_ascii=False)

    if not _pool:
        raise HTTPException(500, "Pool indisponível")
    if not _http_client:
        raise HTTPException(500, "Cliente HTTP indisponível")

    # ------------- Pedido de Venda ------------- #
    if topic in {
        "vendaproduto.incluida", "vendaproduto.alterada", "vendaproduto.etapaalterada",
        "vendaproduto.faturada", "vendaproduto.pedidodevenda",
    }:
        if not id_pedido:
            return {"status": "ignored", "message": "Evento de Pedido sem 'idPedido'."}

        api_payload = {
            "call": "ConsultarPedido",
            "app_key": get_env("OMIE_APP_KEY"),
            "app_secret": get_env("OMIE_APP_SECRET"),
            "param": [{"codigo_pedido": id_pedido}]
        }
        
        logging.info(f"Chamando Omie API para consultar o pedido: {id_pedido}")
        try:
            response = await _http_client.post("/api/v1/produtos/pedido/", json=api_payload)
            response.raise_for_status()
            detailed_data = response.json()
            raw_data = json.dumps(detailed_data, ensure_ascii=False)
            logging.info(f"Dados detalhados do pedido {id_pedido} obtidos com sucesso.")
        except httpx.HTTPStatusError as e:
            logging.error(f"Erro na chamada à Omie API: {e.response.status_code} - {e.response.text}")
            return {"status": "error", "message": f"Falha ao consultar detalhes do pedido {id_pedido} na Omie."}
        except Exception as e:
            logging.error(f"Erro inesperado ao consultar Omie API: {e}")
            return {"status": "error", "message": f"Falha inesperada ao consultar detalhes do pedido {id_pedido} na Omie."}

        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO public.omie_pedido (numero, raw, recebido_em)
                VALUES ($1, $2, now())
                ON CONFLICT (numero) DO UPDATE SET raw = EXCLUDED.raw, recebido_em = now();
                """,
                numero_pedido,
                raw_data,
            )
        return {"status": "success", "message": f"Pedido {numero_pedido} salvo/atualizado com dados detalhados."}

    # ------------- NF-e ------------- #
    elif topic in {
        "nfe.notaautorizada", "nfe.nota_autorizada", "nfe.autorizada",
    }:
        if not numero_nf:
            return {"status": "ignored", "message": "Evento de NF sem 'numero_nf'."}

        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO public.omie_nfe (numero, raw, recebido_em)
                VALUES ($1, $2, now())
                ON CONFLICT (numero) DO UPDATE SET raw = EXCLUDED.raw, recebido_em = now();
                """,
                numero_nf,
                raw_data,
            )
        return {"status": "success", "message": f"NF {numero_nf} salva/atualizada."}

    # ------------- Desconhecido ------------- #
    else:
        return {"status": "ignored", "message": f"Tipo de evento '{topic}' não suportado."}