# -*- coding: utf-8 -*-
import os
import json
import asyncio
import logging
from typing import Optional, Tuple, Dict, Any

import asyncpg
import httpx
from fastapi import FastAPI, Request, HTTPException
from starlette.responses import JSONResponse
from datetime import datetime, date, time, timezone

# -----------------------------------------------------------------------------
# Configuração básica
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("omie-webhook")

OMIE_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "julia-matheus")

app = FastAPI(title="omie-webhook-render")

pool: asyncpg.Pool = None  # será criado no startup


# -----------------------------------------------------------------------------
# Helpers HTTP
# -----------------------------------------------------------------------------
def ok(payload: Dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(content=payload, status_code=status_code)


def fail(payload: Dict[str, Any], status_code: int = 400) -> JSONResponse:
    return JSONResponse(content=payload, status_code=status_code)


def check_secret(secret: str) -> None:
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")


# -----------------------------------------------------------------------------
# Conexão Postgres
# -----------------------------------------------------------------------------
def _database_url_from_env() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    database = os.getenv("PGDATABASE", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


async def ensure_schema(conn: asyncpg.Connection) -> None:
    # Tabela
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
    # Único por id da Omie (chave de consulta)
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
    # Índices auxiliares
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_pedido_status ON omie_pedido(status);")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_pedido_codigo_integ ON omie_pedido(codigo_pedido_integracao);")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_omie_pedido_numero ON omie_pedido(numero);")


@app.on_event("startup")
async def on_startup():
    global pool
    pool = await asyncpg.create_pool(_database_url_from_env(), min_size=1, max_size=10)
    async with pool.acquire() as conn:
        await ensure_schema(conn)
    logger.info("Pool OK e schema selecionado.")


# -----------------------------------------------------------------------------
# Omie API
# -----------------------------------------------------------------------------
def _omie_body(call: str, param: dict) -> dict:
    return {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [param],
    }


async def omie_consultar_pedido(
    id_pedido: Optional[int] = None,
    codigo_integracao: Optional[str] = None,
) -> dict:
    if not OMIE_APP_KEY or not OMIE_APP_SECRET:
        raise RuntimeError("Credenciais OMIE não configuradas (OMIE_APP_KEY/OMIE_APP_SECRET).")

    if not id_pedido and not codigo_integracao:
        raise ValueError("Informe id_pedido OU codigo_integracao para ConsultarPedido.")

    param: dict = {}
    if id_pedido:
        param["idPedido"] = int(id_pedido)
    if codigo_integracao:
        param["codigo_pedido_integracao"] = str(codigo_integracao)

    body = _omie_body("ConsultarPedido", param)

    async with httpx.AsyncClient(timeout=40) as client:
        r = await client.post(OMIE_URL, json=body)
        r.raise_for_status()
        data = r.json()

    # Falhas SOAP da Omie vêm com 'faultstring'
    if isinstance(data, dict) and data.get("faultstring"):
        raise RuntimeError(f"OMIE ConsultarPedido falhou: {data.get('faultstring')}")

    return data


def _br_date(d: date) -> str:
    # retorna "dd/mm/aaaa"
    return d.strftime("%d/%m/%Y")


def _parse_iso(s: str) -> datetime:
    # aceita "YYYY-MM-DD" ou "YYYY-MM-DDTHH:MM:SS±ZZ:ZZ"
    try:
        if "T" in s:
            return datetime.fromisoformat(s)
        # só data -> meia-noite local
        return datetime.fromisoformat(s + "T00:00:00")
    except Exception:
        raise ValueError(f"Data/hora inválida: '{s}'")


async def backfill_omie_por_periodo(inicio: str, fim: str, por_pagina: int = 200) -> dict:
    """
    Busca pedidos na Omie (ListarPedidos) por período [inicio..fim] (datas),
    insere/atualiza 'raw_basico' e marca como 'pendente_consulta'.
    Se 'fim' tiver hora, filtra pedidos do dia final pela hora (via dInc/hInc quando disponível).
    """
    if not OMIE_APP_KEY or not OMIE_APP_SECRET:
        raise RuntimeError("Credenciais OMIE não configuradas (OMIE_APP_KEY/OMIE_APP_SECRET).")

    dt_inicio = _parse_iso(inicio)
    dt_fim = _parse_iso(fim)

    data_de = _br_date(dt_inicio.date())
    data_ate = _br_date(dt_fim.date())

    total_importados = 0
    total_paginas = 1
    pagina = 1

    async with httpx.AsyncClient(timeout=50) as client, pool.acquire() as conn:
        await ensure_schema(conn)

        while pagina <= total_paginas:
            body = _omie_body(
                "ListarPedidos",
                {
                    "pagina": pagina,
                    "registros_por_pagina": por_pagina,
                    "apenas_importado_api": "N",
                    "filtrar_por_data_de": data_de,
                    "filtrar_por_data_ate": data_ate,
                    # "ordem_descrescente": "S"  # se quiser garantir ordem
                },
            )

            r = await client.post(OMIE_URL, json=body)
            r.raise_for_status()
            data = r.json()

            if isinstance(data, dict) and data.get("faultstring"):
                raise RuntimeError(f"OMIE ListarPedidos falhou: {data.get('faultstring')}")

            total_paginas = int(data.get("total_de_paginas", 1))
            lista = data.get("pedido_venda_produto", []) or []

            for ped in lista:
                try:
                    cab = (ped or {}).get("cabecalho", {}) or {}
                    info = (ped or {}).get("infoCadastro", {}) or {}

                    id_pedido_omie = cab.get("codigo_pedido")
                    codigo_integracao = cab.get("codigo_pedido_integracao")
                    numero = str(cab.get("numero_pedido") or "").strip() or None

                    # se fim tiver hora, filtrar itens do último dia
                    if dt_inicio.date() <= dt_fim.date() and dt_fim.hour + dt_fim.minute + dt_fim.second > 0:
                        dInc = info.get("dInc")  # "dd/mm/aaaa"
                        hInc = info.get("hInc")  # "HH:MM:SS"
                        if dInc and hInc:
                            try:
                                dd, mm, aa = map(int, dInc.split("/"))
                                hh, mi, ss = map(int, hInc.split(":"))
                                ts = datetime(aa, mm, dd, hh, mi, ss, tzinfo=dt_fim.tzinfo)
                                if ts.date() == dt_fim.date() and ts > dt_fim:
                                    # pula porque é posterior ao corte
                                    continue
                            except Exception:
                                pass  # se não der pra interpretar, não filtramos

                    await conn.execute(
                        """
                        INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
                        VALUES ($1, $2, $3, $4::jsonb, 'pendente_consulta')
                        ON CONFLICT (id_pedido_omie) DO UPDATE
                           SET raw_basico = EXCLUDED.raw_basico,
                               status = CASE
                                           WHEN omie_pedido.raw_detalhe IS NULL THEN 'pendente_consulta'
                                           ELSE 'consultado'
                                        END,
                               recebido_em = now();
                        """,
                        numero,
                        id_pedido_omie,
                        codigo_integracao,
                        json.dumps(ped),
                    )
                    total_importados += 1
                except Exception as e:
                    logger.exception("backfill_insert_error: %s", e)

            pagina += 1

    return {"importados": total_importados, "paginas": total_paginas}


# -----------------------------------------------------------------------------
# Extração dos campos do webhook
# -----------------------------------------------------------------------------
def extrair_chaves_webhook(payload: dict) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Retorna: (numero, id_pedido_omie, codigo_integracao)
    Tenta cobrir variações de nomes de campos que vimos nos exemplos/logs.
    """
    numero = (
        payload.get("numero")
        or payload.get("numero_pedido")
        or payload.get("numeroPedido")
        or None
    )
    # Pode vir string -> tenta converter
    id_pedido = payload.get("id_pedido") or payload.get("idPedido") or payload.get("codigo_pedido")
    try:
        id_pedido = int(id_pedido) if id_pedido is not None else None
    except Exception:
        id_pedido = None

    codigo_integracao = (
        payload.get("codigo_pedido_integracao")
        or payload.get("codigoPedidoIntegracao")
        or None
    )
    if numero is not None:
        numero = str(numero)
    if codigo_integracao is not None:
        codigo_integracao = str(codigo_integracao)
    return numero, id_pedido, codigo_integracao


# -----------------------------------------------------------------------------
# Rotas
# -----------------------------------------------------------------------------
@app.post("/omie/webhook")
async def omie_webhook(req: Request):
    """
    Recebe o webhook da Omie, salva raw_basico, e marca 'pendente_consulta'
    quando houver alguma chave consultável (id_pedido / codigo_integracao).
    """
    payload = await req.json()
    numero, id_pedido, codigo_integracao = extrair_chaves_webhook(payload)
    logger.info({"tag": "omie_webhook_received", "numero_pedido": str(numero), "id_pedido": id_pedido})

    async with pool.acquire() as conn:
        await ensure_schema(conn)
        status_inicial = "pendente_consulta" if (id_pedido or codigo_integracao) else "sem_id"

        # Quando existe id_pedido_omie, usamos upsert com a UNIQUE
        if id_pedido:
            await conn.execute(
                """
                INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
                VALUES ($1, $2, $3, $4::jsonb, $5)
                ON CONFLICT (id_pedido_omie) DO UPDATE
                   SET raw_basico = EXCLUDED.raw_basico,
                       status = CASE
                                  WHEN omie_pedido.raw_detalhe IS NULL THEN 'pendente_consulta'
                                  ELSE 'consultado'
                               END,
                       recebido_em = now();
                """,
                numero,
                id_pedido,
                codigo_integracao,
                json.dumps(payload),
                status_inicial,
            )
        else:
            # Sem id, insere um registro (pode haver mais de um enquanto não consultado)
            await conn.execute(
                """
                INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, raw_basico, status)
                VALUES ($1, NULL, $2, $3::jsonb, $4);
                """,
                numero,
                codigo_integracao,
                json.dumps(payload),
                status_inicial,
            )

    return ok({"ok": True})


@app.post("/admin/reprocessar-pendentes")
async def admin_reprocessar_pendentes(secret: str, limit: int = 200):
    check_secret(secret)
    processados_ok, processados_err, ids_ok = 0, 0, []

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pk, id_pedido_omie, codigo_pedido_integracao
              FROM omie_pedido
             WHERE status = 'pendente_consulta'
               AND (id_pedido_omie IS NOT NULL OR codigo_pedido_integracao IS NOT NULL)
               AND raw_detalhe IS NULL
             ORDER BY recebido_em ASC
             LIMIT $1
            """,
            limit,
        )

        for r in rows:
            try:
                detalhe = await omie_consultar_pedido(
                    id_pedido=r["id_pedido_omie"],
                    codigo_integracao=r["codigo_pedido_integracao"],
                )
                await conn.execute(
                    """
                    UPDATE omie_pedido
                       SET raw_detalhe = $1::jsonb,
                           status = 'consultado',
                           recebido_em = now()
                     WHERE pk = $2
                    """,
                    json.dumps(detalhe),
                    r["pk"],
                )
                processados_ok += 1
                ids_ok.append(r["id_pedido_omie"])
            except Exception as e:
                logger.exception("reprocess_error: %s", e)
                processados_err += 1

    return ok({
        "ok": True,
        "pendentes_encontrados": len(rows),
        "processados_ok": processados_ok,
        "processados_erro": processados_err,
        "ids_ok": ids_ok,
    })


@app.post("/admin/reprocessar-pedido")
async def admin_reprocessar_pedido(
    secret: str,
    numero: Optional[str] = None,
    id_pedido: Optional[int] = None,
    codigo_integracao: Optional[str] = None,
):
    check_secret(secret)
    async with pool.acquire() as conn:
        row = None
        if id_pedido:
            row = await conn.fetchrow("SELECT pk, id_pedido_omie, codigo_pedido_integracao FROM omie_pedido WHERE id_pedido_omie=$1 LIMIT 1", id_pedido)
        elif codigo_integracao:
            row = await conn.fetchrow(
                "SELECT pk, id_pedido_omie, codigo_pedido_integracao FROM omie_pedido WHERE codigo_pedido_integracao=$1 ORDER BY recebido_em DESC LIMIT 1",
                codigo_integracao,
            )
        elif numero:
            row = await conn.fetchrow(
                "SELECT pk, id_pedido_omie, codigo_pedido_integracao FROM omie_pedido WHERE numero=$1 ORDER BY recebido_em DESC LIMIT 1",
                numero,
            )

        if not row:
            return fail({"detail": "Pedido não encontrado na base"}, 404)

        if not (row["id_pedido_omie"] or row["codigo_pedido_integracao"]):
            return fail({"detail": "Pedido sem chave de consulta (id/codigo_integracao)"}, 400)

        try:
            detalhe = await omie_consultar_pedido(
                id_pedido=row["id_pedido_omie"],
                codigo_integracao=row["codigo_pedido_integracao"],
            )
            await conn.execute(
                """
                UPDATE omie_pedido
                   SET raw_detalhe = $1::jsonb,
                       status = 'consultado',
                       recebido_em = now()
                 WHERE pk = $2
                """,
                json.dumps(detalhe),
                row["pk"],
            )
            return ok({"ok": True, "id": row["id_pedido_omie"], "numero": numero})
        except Exception as e:
            logger.exception("reprocess_error: %s", e)
            return fail({"ok": False, "detail": str(e)}, 500)


@app.post("/admin/backfill")
async def admin_backfill(secret: str, inicio: str, fim: str, por_pagina: int = 200):
    """
    Ex.: /admin/backfill?secret=julia-matheus&inicio=2025-08-25&fim=2025-09-04T16:57:00-03:00
    """
    check_secret(secret)
    try:
        resultado = await backfill_omie_por_periodo(inicio, fim, por_pagina)
        return ok({"ok": True, **resultado})
    except Exception as e:
        logger.exception("backfill_error: %s", e)
        return fail({"ok": False, "error": "backfill_error", "detail": str(e)}, 500)


@app.post("/admin/run-jobs")
async def admin_run_jobs(secret: str):
    check_secret(secret)
    # Espaço para tarefas rápidas se precisar (atualmente só um ping)
    return ok({"ok": True, "msg": "jobs disparados"})


# -----------------------------------------------------------------------------
# Uvicorn local (opcional)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
