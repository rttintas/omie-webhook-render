# main.py
# FastAPI + asyncpg + httpx
# Omie Webhook + Consulta de Pedido + Reprocess + Backfill
from __future__ import annotations

import os
import json
import logging
from typing import Optional, Dict, Any, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse

# -------------------------
# Bootstrap do FastAPI (deixe isso ANTES de qualquer @app.*)
# -------------------------
app: FastAPI
try:
    app  # type: ignore # se já existir (outro arquivo), mantém
except NameError:
    app = FastAPI(title="Omie Webhook")

# -------------------------
# Logger
# -------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("omie")

# -------------------------
# Env
# -------------------------
OMIE_URL = os.environ.get("OMIE_URL", "https://app.omie.com.br/api/v1/produtos/pedido/")
OMIE_APP_KEY = os.environ.get("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.environ.get("OMIE_APP_SECRET", "")
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not DATABASE_URL:
    log.warning("DATABASE_URL não definido – o app não conseguirá iniciar a pool.")

# -------------------------
# Conexão DB
# -------------------------
db_pool: Optional[asyncpg.Pool] = None


async def ensure_schema(conn: asyncpg.Connection) -> None:
    # Cria tabela e colunas necessárias
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS omie_pedido (
            pk BIGSERIAL PRIMARY KEY,
            numero TEXT,
            id_pedido_omie BIGINT,
            codigo_pedido_integracao TEXT,
            raw_basico JSONB,
            raw_detalhe JSONB,
            status TEXT,
            recebido_em TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    # UNIQUE em id_pedido_omie (para permitir ON CONFLICT)
    await conn.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                 WHERE conname = 'omie_pedido_uid'
            ) THEN
                ALTER TABLE omie_pedido
                ADD CONSTRAINT omie_pedido_uid UNIQUE (id_pedido_omie);
            END IF;
        END$$;
        """
    )
    # Índices úteis
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_pedido_status ON omie_pedido(status);")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_pedido_numero ON omie_pedido(numero);")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_omie_pedido_codint ON omie_pedido(codigo_pedido_integracao);"
    )


@app.on_event("startup")
async def on_start() -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=60,
    )
    async with db_pool.acquire() as conn:
        await ensure_schema(conn)
    log.info("Pool OK e schema selecionado.")


@app.on_event("shutdown")
async def on_stop() -> None:
    global db_pool
    if db_pool:
        await db_pool.close()
        log.info("Pool encerrada.")


# -------------------------
# Helpers Omie
# -------------------------
def _parse_detail_fields(detalhe: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Extrai numero_pedido, codigo_pedido (int) e codigo_pedido_integracao do JSON de detalhe.
    """
    try:
        cab = (detalhe.get("pedido_venda_produto") or {}).get("cabecalho") or {}
    except Exception:
        cab = {}

    numero = cab.get("numero_pedido") or None

    id_pedido = cab.get("codigo_pedido")
    try:
        if id_pedido is not None:
            id_pedido = int(str(id_pedido))
    except Exception:
        id_pedido = None

    cod_int = cab.get("codigo_pedido_integracao") or None
    return numero, id_pedido, cod_int


async def omie_request(method: str, param: Dict[str, Any]) -> Dict[str, Any]:
    """
    Chamada genérica à Omie (padrão APIs Omie).
    """
    payload = {
        "call": method,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [param],
    }
    log.info({"tag": "omie_request", "method": method, "param": param})
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(OMIE_URL, json=payload)
        r.raise_for_status()
        data = r.json()
        log.info({"tag": "omie_response", "method": method, "response": data})
        # Alguns erros de negócio vêm como 200; normalize
        if isinstance(data, dict) and "faultstring" in data:
            raise httpx.HTTPStatusError(
                f"Omie fault: {data.get('faultstring')}",
                request=r.request,
                response=r,
            )
        return data


async def consultar_pedido(
    *,
    id_pedido: Optional[int] = None,
    codigo_pedido_integracao: Optional[str] = None,
) -> Dict[str, Any]:
    if id_pedido:
        return await omie_request("ConsultarPedido", {"codigo_pedido": id_pedido})
    if codigo_pedido_integracao:
        return await omie_request(
            "ConsultarPedido", {"codigo_pedido_integracao": codigo_pedido_integracao}
        )
    raise ValueError("Informe id_pedido OU codigo_pedido_integracao.")


# -------------------------
# Helpers DB
# -------------------------
async def upsert_basico(
    conn: asyncpg.Connection,
    *,
    numero: Optional[str],
    id_pedido: Optional[int],
    codigo_integracao: Optional[str],
    raw: Dict[str, Any],
) -> int:
    """
    Insere/atualiza o registro 'básico' com status inicial.
    - Se houver id_pedido_omie, faz UPSERT via UNIQUE (id_pedido_omie).
    - Senão, insere linha 'sem_id' ou 'pendente_consulta' se tiver código integração.
    Retorna pk.
    """
    status = "pendente_consulta" if (id_pedido or (codigo_integracao or "").strip()) else "sem_id"

    if id_pedido:
        await conn.execute(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (id_pedido_omie) DO UPDATE SET
                numero = EXCLUDED.numero,
                codigo_pedido_integracao = COALESCE(EXCLUDED.codigo_pedido_integracao, omie_pedido.codigo_pedido_integracao),
                raw_basico = EXCLUDED.raw_basico,
                status = CASE WHEN omie_pedido.raw_detalhe IS NULL THEN 'pendente_consulta' ELSE omie_pedido.status END,
                recebido_em = now();
            """,
            numero,
            id_pedido,
            codigo_integracao,
            json.dumps(raw),
            status,
        )
        row = await conn.fetchrow("SELECT pk FROM omie_pedido WHERE id_pedido_omie=$1", id_pedido)
        return int(row["pk"])
    else:
        row = await conn.fetchrow(
            """
            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
            VALUES ($1, NULL, $2, $3, $4)
            RETURNING pk;
            """,
            numero,
            codigo_integracao,
            json.dumps(raw),
            status,
        )
        return int(row["pk"])


async def salvar_consulta(conn: asyncpg.Connection, *, pk: int, detalhe: Dict[str, Any]) -> None:
    numero_d, id_d, cod_d = _parse_detail_fields(detalhe)
    await conn.execute(
        """
        UPDATE omie_pedido
           SET raw_detalhe = $2,
               status = 'consultado',
               recebido_em = now(),
               id_pedido_omie = COALESCE(id_pedido_omie, $3),
               numero = COALESCE(numero, $4),
               codigo_pedido_integracao = COALESCE(codigo_pedido_integracao, $5)
         WHERE pk = $1;
        """,
        pk,
        json.dumps(detalhe),
        id_d,
        numero_d,
        cod_d,
    )


def _first_nonempty(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def extract_incoming_keys(body: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Tenta achar numero, id e código de integração no payload do webhook (flexível).
    """
    numero = _first_nonempty(
        body.get("numero"),
        body.get("numero_pedido"),
        body.get("numeroPedido"),
        body.get("pedido"),
    )

    id_pedido = body.get("id_pedido") or body.get("idPedido") or body.get("codigo_pedido")
    try:
        if id_pedido is not None and str(id_pedido).isdigit():
            id_pedido = int(str(id_pedido))
        else:
            id_pedido = None
    except Exception:
        id_pedido = None

    cod_int = _first_nonempty(
        body.get("codigo_pedido_integracao"),
        body.get("codigoPedidoIntegracao"),
        body.get("numero_pedido_cliente"),
        body.get("numeroPedidoCliente"),
    )

    return numero, id_pedido, cod_int


# -------------------------
# Endpoints
# -------------------------
@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str = Query(...)) -> Dict[str, Any]:
    if token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=403, detail="Token inválido")
    body = await request.json()
    numero, id_pedido, cod_int = extract_incoming_keys(body)
    log.info({"tag": "omie_webhook_received", "numero_pedido": numero, "id_pedido": id_pedido})

    if not db_pool:
        raise HTTPException(500, "DB pool indisponível")

    async with db_pool.acquire() as conn:
        pk = await upsert_basico(
            conn,
            numero=numero,
            id_pedido=id_pedido,
            codigo_integracao=cod_int,
            raw=body,
        )

    # Se já temos uma "chave de consulta", tenta consultar imediatamente
    consultado_agora = False
    if id_pedido or cod_int:
        try:
            detalhe = await consultar_pedido(id_pedido=id_pedido, codigo_pedido_integracao=cod_int)
            async with db_pool.acquire() as conn:
                await salvar_consulta(conn, pk=pk, detalhe=detalhe)
            consultado_agora = True
        except httpx.HTTPStatusError as e:
            log.error(
                {"tag": "omie_response_error", "id_pedido": id_pedido, "numero": numero, "err": str(e)}
            )
            # Mantém como pendente_consulta; nada a fazer aqui.

    return {"ok": True, "pk": pk, "consultado_agora": consultado_agora}


# --- Admin: reprocessar pendentes (fila) ---
@app.post("/admin/reprocessar-pendentes")
async def reprocessar_pendentes(secret: str, limit: int = 50) -> Dict[str, Any]:
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "forbidden")

    if not db_pool:
        raise HTTPException(500, "DB pool indisponível")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pk, id_pedido_omie, codigo_pedido_integracao, numero
              FROM omie_pedido
             WHERE status = 'pendente_consulta'
               AND (id_pedido_omie IS NOT NULL OR COALESCE(codigo_pedido_integracao,'') <> '')
             ORDER BY recebido_em ASC
             LIMIT $1;
            """,
            limit,
        )

    ok, err = 0, 0
    ids_ok = []
    for r in rows:
        try:
            detalhe = await consultar_pedido(
                id_pedido=r["id_pedido_omie"], codigo_pedido_integracao=r["codigo_pedido_integracao"]
            )
            async with db_pool.acquire() as conn:
                await salvar_consulta(conn, pk=int(r["pk"]), detalhe=detalhe)
            ok += 1
            ids_ok.append(int(r["pk"]))
        except Exception as e:
            err += 1
            log.error(
                {
                    "tag": "reprocess_error",
                    "id_pedido": r["id_pedido_omie"],
                    "numero": r["numero"],
                    "err": str(e),
                }
            )

    return {
        "ok": True,
        "pendentes_encontrados": len(rows),
        "processados_ok": ok,
        "processados_erro": err,
        "ids_ok": ids_ok,
    }


# --- Admin: reprocessar específico ---
@app.post("/admin/reprocessar-pedido")
async def reprocessar_pedido(
    secret: str,
    numero: Optional[str] = None,
    id_pedido: Optional[int] = None,
    codigo_integracao: Optional[str] = None,
) -> JSONResponse | Dict[str, Any]:
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "forbidden")

    if not any([numero, id_pedido, codigo_integracao]):
        raise HTTPException(400, "Informe numero ou id_pedido ou codigo_integracao")

    if not db_pool:
        raise HTTPException(500, "DB pool indisponível")

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT pk, id_pedido_omie, codigo_pedido_integracao, numero
              FROM omie_pedido
             WHERE ($1::text IS NULL OR numero = $1)
               AND ($2::bigint IS NULL OR id_pedido_omie = $2)
               AND ($3::text IS NULL OR codigo_pedido_integracao = $3)
             ORDER BY recebido_em DESC
             LIMIT 1;
            """,
            numero,
            id_pedido,
            codigo_integracao,
        )
        if not row:
            return JSONResponse(
                {"detail": "Pedido não encontrado ou sem chave de consulta"}, status_code=404
            )

    detalhe = await consultar_pedido(
        id_pedido=row["id_pedido_omie"] or id_pedido,
        codigo_pedido_integracao=row["codigo_pedido_integracao"] or codigo_integracao,
    )
    async with db_pool.acquire() as conn:
        await salvar_consulta(conn, pk=int(row["pk"]), detalhe=detalhe)

    return {"ok": True, "pk": int(row["pk"])}


# --- Admin: backfill por janela de datas (data de inclusão na Omie) ---
@app.post("/admin/backfill")
async def backfill(
    secret: str,
    inicio: str = Query(..., description="ex.: 2025-08-25"),
    fim: str = Query(..., description="ex.: 2025-09-04T16:57:00-03:00"),
    pagina_inicial: int = 1,
    por_pagina: int = 200,
) -> Dict[str, Any]:
    """
    Busca pedidos pela Omie em janela de datas (data_inc) e 'semeia' o banco
    com linhas básicas para posterior consulta de detalhe.
    """
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "forbidden")
    if not db_pool:
        raise HTTPException(500, "DB pool indisponível")

    total_inseridos = 0
    pagina = max(1, pagina_inicial)
    por_pagina = max(1, min(por_pagina, 500))

    while True:
        try:
            resp = await omie_request(
                "ListarPedidos",
                {
                    "pagina": pagina,
                    "registros_por_pagina": por_pagina,
                    "filtrar_por_data_inc": "S",
                    "data_inc_de": inicio,
                    "data_inc_ate": fim,
                },
            )
        except httpx.HTTPStatusError as e:
            log.error({"tag": "backfill_http_error", "err": str(e)})
            break

        itens = (resp or {}).get("pedido_venda_produto") or []
        if not itens:
            break

        async with db_pool.acquire() as conn:
            for it in itens:
                cab = (it or {}).get("cabecalho") or {}
                numero = cab.get("numero_pedido") or None
                cod = cab.get("codigo_pedido_integracao") or None

                id_pedido = cab.get("codigo_pedido")
                try:
                    id_pedido = int(str(id_pedido)) if id_pedido is not None else None
                except Exception:
                    id_pedido = None

                try:
                    await upsert_basico(
                        conn,
                        numero=numero,
                        id_pedido=id_pedido,
                        codigo_integracao=cod,
                        raw={"cabecalho": cab},
                    )
                    total_inseridos += 1
                except Exception as e:
                    log.error(
                        {
                            "tag": "upsert_backfill_error",
                            "numero": numero,
                            "id_pedido": id_pedido,
                            "err": str(e),
                        }
                    )

        ultima = (resp or {}).get("ultima_pagina")
        pagina += 1
        if ultima in ("S", "s", True):
            break

    return {"ok": True, "coletados": total_inseridos}


# --- Admin: atalho para rodar a fila de pendentes ---
@app.post("/admin/run-jobs")
async def run_jobs(secret: str, limit: int = 200) -> Dict[str, Any]:
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "forbidden")
    # chama a própria função de reprocessar (síncrono)
    return await reprocessar_pendentes(secret=secret, limit=limit)
