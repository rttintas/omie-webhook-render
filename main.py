# main.py
import os
import json
import asyncio
from datetime import datetime, timezone

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException, Query

APP_NAME = "omie-webhook-render"

# ====== CONFIG ======
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "um-segredo-forte")
POOL_MIN = int(os.getenv("POOL_MIN", "1"))
POOL_MAX = int(os.getenv("POOL_MAX", "5"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))

if not DATABASE_URL:
    raise RuntimeError("Faltou DATABASE_URL no ambiente.")

app = FastAPI(title=APP_NAME)
pool: asyncpg.Pool | None = None


# ====== UTIL ======
def now_tz() -> datetime:
    return datetime.now(timezone.utc)


async def fetch_text(url: str) -> str | None:
    """Baixa texto (XML) se a URL estiver acessível; senão, retorna None."""
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.text
    except Exception:
        return None


# ====== STARTUP / SHUTDOWN ======
@app.on_event("startup")
async def _startup() -> None:
    global pool
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=POOL_MIN,
        max_size=POOL_MAX,
        command_timeout=60,
    )
    # ping simples
    async with pool.acquire() as con:
        val = await con.fetchval("SELECT 1;")
        if val != 1:
            raise RuntimeError("Falha no ping do banco.")
    print("INFO: Pool OK & schema verificado.")


@app.on_event("shutdown")
async def _shutdown() -> None:
    global pool
    if pool:
        await pool.close()
        pool = None


# ====== HEALTH ======
@app.get("/health/app")
async def health_app():
    return {"ok": True, "name": APP_NAME}


@app.get("/health/db")
async def health_db():
    async with pool.acquire() as con:
        v = await con.fetchval("SELECT 1;")
    return {"db": "ok", "select1": v}


# ====== PARSERS DE PAYLOAD ======
def _get(d: dict, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def parse_payload(body: dict) -> dict:
    """
    Normaliza o payload em um dicionário com:
      kind: "pedido" | "nfe"
      numero_pedido: str | None
      numero_nf: str | None
      chave_nfe: str | None
      xml_url: str | None
    Suporta:
      - Formato Omie (topic + evento)
      - Formato "cru" com campos no topo
    """
    topic = body.get("topic") or body.get("tipo") or ""

    # Tentativas diretas do 'evento'
    ev = body.get("evento") if isinstance(body.get("evento"), dict) else {}
    numero_pedido = (
        _get(body, "numeroPedido")
        or _get(ev, "numeroPedido")
        or _get(body, "pedido", "numero")
    )
    numero_nf = (
        _get(body, "numero_nf")
        or _get(ev, "numero_nf")
        or _get(body, "nfe", "numero")
    )
    chave_nfe = (
        _get(body, "nfe_chave")
        or _get(ev, "nfe_chave")
        or _get(body, "chave_nfe")
        or _get(ev, "chave_nfe")
    )
    xml_url = (
        _get(body, "nfe_xml")
        or _get(ev, "nfe_xml")
        or _get(body, "xml_url")
        or _get(ev, "xml_url")
    )

    # Determinar kind
    kind = None
    t = str(topic).lower()
    if "vendaproduto" in t:
        kind = "pedido"
    elif "nfe.notaautorizada" in t or "nfe" in t:
        kind = "nfe"
    # fallback por presença de campos
    if kind is None:
        if numero_pedido:
            kind = "pedido"
        elif numero_nf or chave_nfe or xml_url:
            kind = "nfe"

    return {
        "kind": kind,
        "numero_pedido": str(numero_pedido) if numero_pedido else None,
        "numero_nf": str(numero_nf) if numero_nf else None,
        "chave_nfe": str(chave_nfe) if chave_nfe else None,
        "xml_url": str(xml_url) if xml_url else None,
    }


# ====== UPSERTS ======
async def upsert_pedido(numero_pedido: str) -> None:
    """
    Grava/atualiza pedido em public.omie_pedido.
    Requer UNIQUE(numero) já criado (uQ_omie_pedido_numero).
    """
    sql = """
        INSERT INTO public.omie_pedido (numero, recebido_em)
        VALUES ($1, now())
        ON CONFLICT (numero) DO UPDATE
           SET recebido_em = EXCLUDED.recebido_em;
    """
    async with pool.acquire() as con:
        await con.execute(sql, numero_pedido)


async def upsert_nfe(
    *,
    numero_nf: str | None,
    chave_nfe: str | None,
    xml_text: str | None,
) -> None:
    """
    Grava/atualiza capa da NFe em public.omie_nfe usando a coluna **xml**.
    Usa ON CONFLICT por (chave_nfe) se existir; senão, por (numero).
    Também grava em public.omie_nfe_xml (chave_nfe, xml) se houver chave.
    """
    async with pool.acquire() as con:
        async with con.transaction():
            if chave_nfe:
                # upsert por chave_nfe
                sql_chave = """
                    INSERT INTO public.omie_nfe
                        (chave_nfe, numero, xml, updated_at, recebido_em)
                    VALUES ($1, $2, $3, now(), now())
                    ON CONFLICT (chave_nfe) DO UPDATE SET
                        numero = COALESCE(EXCLUDED.numero, public.omie_nfe.numero),
                        xml    = COALESCE(EXCLUDED.xml, public.omie_nfe.xml),
                        updated_at = now();
                """
                await con.execute(sql_chave, chave_nfe, numero_nf, xml_text)
            elif numero_nf:
                # upsert por numero (precisa de UNIQUE(numero))
                sql_numero = """
                    INSERT INTO public.omie_nfe
                        (numero, xml, updated_at, recebido_em)
                    VALUES ($1, $2, now(), now())
                    ON CONFLICT (numero) DO UPDATE SET
                        xml = COALESCE(EXCLUDED.xml, public.omie_nfe.xml),
                        updated_at = now();
                """
                await con.execute(sql_numero, numero_nf, xml_text)

            # Guarda XML completo em tabela dedicada (se souber a chave)
            if chave_nfe and xml_text:
                sql_xml = """
                    INSERT INTO public.omie_nfe_xml (chave_nfe, xml, recebido_em)
                    VALUES ($1, $2, now())
                    ON CONFLICT (chave_nfe) DO UPDATE SET
                        xml = EXCLUDED.xml,
                        recebido_em = now();
                """
                await con.execute(sql_xml, chave_nfe, xml_text)


# ====== WEBHOOK ======
@app.post("/omie/webhook")
async def omie_webhook(
    request: Request,
    token: str = Query(None, alias="token"),
):
    # Autorização simples por token de querystring
    if token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=403, detail="Token inválido.")

    try:
        body = await request.json()
    except Exception:
        body = {}

    if not isinstance(body, dict) or not body:
        # Log leve para diagnóstico
        print("INFO: Recebida chamada com payload inválido/vazio.")
        raise HTTPException(status_code=400, detail="Payload inválido")

    print(f"INFO: Payload recebido: {json.dumps(body, ensure_ascii=False)}")

    event = parse_payload(body)
    kind = event.get("kind")

    if kind == "pedido":
        numero_pedido = event.get("numero_pedido")
        if not numero_pedido:
            raise HTTPException(status_code=400, detail="numeroPedido ausente")
        await upsert_pedido(numero_pedido)
        return {"status": "success", "message": f"Pedido {numero_pedido} salvo/atualizado."}

    if kind == "nfe":
        numero_nf = event.get("numero_nf")
        chave_nfe = event.get("chave_nfe")
        xml_url = event.get("xml_url")

        xml_text = None
        if xml_url:
            xml_text = await fetch_text(xml_url)

        await upsert_nfe(numero_nf=numero_nf, chave_nfe=chave_nfe, xml_text=xml_text)
        msg_id = chave_nfe or numero_nf or "-"
        return {"status": "success", "message": f"NF {msg_id} salva/atualizada."}

    # Sem match
    return {"status": "ignored", "message": "Evento não reconhecido."}


# ====== RAIZ ======
@app.get("/")
async def root():
    return {"ok": True, "app": APP_NAME}
