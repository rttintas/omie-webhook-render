# main.py
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Query, Request

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")  # Ex.: postgres://user:pass@host/db
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "julia-matheus")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "um-segredo-forte")

OMIE_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"
OMIE_APP_KEY = os.getenv("OMIE_APP_KEY", "")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", "")

# -----------------------------------------------------------------------------
# App / Clients / Logger
# -----------------------------------------------------------------------------
app = FastAPI(title="omie-webhook-render")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("omie")

http = httpx.AsyncClient(timeout=40)


# -----------------------------------------------------------------------------
# DB
# -----------------------------------------------------------------------------
async def get_pool() -> asyncpg.Pool:
    if not hasattr(app.state, "pool"):
        app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return app.state.pool


SCHEMA_SQL = """
-- Tabela base
CREATE TABLE IF NOT EXISTS omie_pedido (
  pk BIGSERIAL PRIMARY KEY,
  numero TEXT,
  id_pedido_omie BIGINT,
  codigo_pedido_integracao TEXT,
  status TEXT,
  recebido_em TIMESTAMPTZ DEFAULT now(),
  raw_basico JSONB,
  raw_detalhe JSONB
);

-- Índices auxiliares
CREATE INDEX IF NOT EXISTS omie_pedido_status_idx ON omie_pedido (status);
CREATE INDEX IF NOT EXISTS omie_pedido_recebido_idx ON omie_pedido (recebido_em DESC);

-- Único pelo id da Omie (quando existir)
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


@app.on_event("startup")
async def on_startup():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    log.info("INFO: { 'tag': 'startup', 'msg': 'Pool OK e esquema selecionado.'}")


@app.on_event("shutdown")
async def on_shutdown():
    try:
        await http.aclose()
    finally:
        if hasattr(app.state, "pool"):
            pool: asyncpg.Pool = app.state.pool
            await pool.close()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def _parse_dt(s: str) -> datetime:
    """Aceita 'YYYY-MM-DD' ou ISO. Assume UTC se não vier TZ."""
    try:
        if len(s) == 10:
            dt = datetime.fromisoformat(s + "T00:00:00")
        else:
            dt = datetime.fromisoformat(s)
    except Exception:
        raise HTTPException(400, detail="Formato de data inválido.")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def _extract_id_omie(raw: Dict[str, Any]) -> Optional[int]:
    """
    Tenta achar um ID numérico do pedido (id da Omie) dentro do JSON recebido.
    """
    keys = [
        ("idPedido",),
        ("evento", "idPedido"),
        ("pedido", "idPedido"),
    ]
    for path in keys:
        cur = raw
        ok = True
        for p in path:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                ok = False
                break
        if ok and cur is not None:
            try:
                # Alguns vêm com pontuação
                return int(str(cur).replace(".", "").replace(",", ""))
            except Exception:
                pass
    return None


def _extract_codigo_integracao(raw: Dict[str, Any]) -> Optional[str]:
    for k in ("codigo_pedido_integracao", "codigoPedidoIntegracao", "numero", "numPedidoIntegracao"):
        v = raw.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _numero_from_rawdet(raw_det: Dict[str, Any]) -> Optional[str]:
    try:
        return str(
            raw_det["pedido_venda_produto"]["cabecalho"]["numero_pedido"]
        )
    except Exception:
        return None


def _build_omie_consulta_body(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    **AQUI ESTÁ O AJUSTE IMPORTANTE**
    - Se tiver id numérico da Omie, enviar como `codigo_pedido`
    - Senão, usar `codigo_pedido_integracao`
    """
    if row.get("id_pedido_omie") is not None:
        try:
            return {"codigo_pedido": int(row["id_pedido_omie"])}
        except Exception:
            return {"codigo_pedido": int(str(row["id_pedido_omie"]).replace(".", "").replace(",", ""))}

    if row.get("codigo_pedido_integracao"):
        return {"codigo_pedido_integracao": row["codigo_pedido_integracao"]}

    return None


async def omie_call(call: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Envelopa a chamada no formato da Omie.
    """
    body = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [payload],
    }
    r = await http.post(OMIE_URL, json=body)
    r.raise_for_status()
    data = r.json()
    return data


async def salvar_basico(conn: asyncpg.Connection, raw: Dict[str, Any]) -> int:
    numero = raw.get("numero")
    id_omie = _extract_id_omie(raw)
    cod_int = _extract_codigo_integracao(raw)

    if id_omie is None and not cod_int:
        status = "sem_chave"
    else:
        status = "pendente_consulta"

    sql = """
    INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, status, raw_basico)
    VALUES ($1,$2,$3,$4,$5::jsonb)
    ON CONFLICT (id_pedido_omie) DO UPDATE
       SET numero = EXCLUDED.numero,
           raw_basico = EXCLUDED.raw_basico,
           recebido_em = now(),
           status = CASE
                     WHEN omie_pedido.raw_detalhe IS NOT NULL THEN 'consultado'
                     ELSE 'pendente_consulta'
                    END
    RETURNING pk;
    """
    pk = await conn.fetchval(sql, numero, id_omie, cod_int, status, _json(raw))
    return int(pk)


async def salvar_detalhe_ok(conn: asyncpg.Connection, pk: int, detalhe: Dict[str, Any]) -> None:
    numero = _numero_from_rawdet(detalhe)
    await conn.execute(
        """
        UPDATE omie_pedido
           SET raw_detalhe = $2::jsonb,
               status = 'consultado',
               numero = COALESCE($3, numero)
         WHERE pk = $1
        """,
        pk, _json(detalhe), numero
    )


async def marcar_sem_chave(conn: asyncpg.Connection, pk: int) -> None:
    await conn.execute("UPDATE omie_pedido SET status='erro_sem_chave' WHERE pk=$1", pk)


# -----------------------------------------------------------------------------
# Rotas
# -----------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"ok": True, "service": "omie-webhook-render"}


@app.post("/omie/webhook")
async def omie_webhook(request: Request, token: str = Query(...)):
    if token != WEBHOOK_TOKEN:
        raise HTTPException(403, "token inválido")

    raw = await request.json()
    log.info(_json({"tag": "omie_webhook_received", "numero": raw.get("numero"), "id_pedido": _extract_id_omie(raw)}))

    pool = await get_pool()
    async with pool.acquire() as conn:
        pk = await salvar_basico(conn, raw)
    return {"ok": True, "pk": pk}


# --------------------------- ADM: reprocessar pendentes ----------------------
@app.post("/admin/reprocessar-pendentes")
async def reprocessar_pendentes(secret: str = Query(...), limit: int = Query(200)):
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "nope")

    pool = await get_pool()
    ok = err = 0
    ids_ok: list[int] = []

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pk, id_pedido_omie, codigo_pedido_integracao
              FROM omie_pedido
             WHERE status = 'pendente_consulta'
             ORDER BY recebido_em ASC
             LIMIT $1
            """,
            limit,
        )

        for row in rows:
            pk = int(row["pk"])
            body = _build_omie_consulta_body(dict(row))
            if not body:
                await marcar_sem_chave(conn, pk)
                err += 1
                continue

            try:
                data = await omie_call("ConsultarPedido", body)
                if "faultstring" in data:
                    # Erro de negócio da Omie
                    log.error(_json({"tag": "omie_response", "pk": pk, "err": data["faultstring"]}))
                    err += 1
                    continue

                await salvar_detalhe_ok(conn, pk, data)
                ids_ok.append(pk)
                ok += 1

            except httpx.HTTPStatusError as e:
                log.error(_json({"tag": "http_error", "status": e.response.status_code, "pk": pk}))
                err += 1
            except Exception as e:
                log.exception(e)
                err += 1

    return {
        "ok": True,
        "pendentes_encontrados": len(rows),
        "processados_ok": ok,
        "processados_erro": err,
        "ids_ok": ids_ok,
    }


# --------------------------- ADM: reprocessar um pedido ----------------------
@app.post("/admin/reprocessar-pedido")
async def reprocessar_pedido(
    secret: str = Query(...),
    numero: Optional[str] = Query(None, description="Pode ser o numero (string) que você usa como integração"),
    id_pedido_omie: Optional[int] = Query(None),
    codigo_integracao: Optional[str] = Query(None),
):
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "nope")

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = None
        if id_pedido_omie is not None:
            row = await conn.fetchrow(
                "SELECT pk, id_pedido_omie, codigo_pedido_integracao FROM omie_pedido WHERE id_pedido_omie=$1",
                id_pedido_omie,
            )
            if not row:
                # cria stub para processar
                pk = await conn.fetchval(
                    """
                    INSERT INTO omie_pedido (id_pedido_omie, status)
                    VALUES ($1,'pendente_consulta')
                    ON CONFLICT (id_pedido_omie) DO NOTHING
                    RETURNING pk
                    """,
                    id_pedido_omie,
                )
                if pk is None:
                    row = await conn.fetchrow(
                        "SELECT pk, id_pedido_omie, codigo_pedido_integracao FROM omie_pedido WHERE id_pedido_omie=$1",
                        id_pedido_omie,
                    )
                else:
                    row = await conn.fetchrow("SELECT pk, id_pedido_omie, codigo_pedido_integracao FROM omie_pedido WHERE pk=$1", pk)

        elif codigo_integracao:
            row = await conn.fetchrow(
                """
                SELECT pk, id_pedido_omie, codigo_pedido_integracao
                  FROM omie_pedido
                 WHERE codigo_pedido_integracao = $1
                 ORDER BY recebido_em DESC
                 LIMIT 1
                """,
                codigo_integracao,
            )
            if not row:
                pk = await conn.fetchval(
                    "INSERT INTO omie_pedido (codigo_pedido_integracao, status) VALUES ($1,'pendente_consulta') RETURNING pk",
                    codigo_integracao,
                )
                row = await conn.fetchrow("SELECT pk, id_pedido_omie, codigo_pedido_integracao FROM omie_pedido WHERE pk=$1", pk)

        elif numero:
            row = await conn.fetchrow(
                "SELECT pk, id_pedido_omie, codigo_pedido_integracao FROM omie_pedido WHERE numero=$1 ORDER BY recebido_em DESC LIMIT 1",
                numero,
            )

        if not row:
            raise HTTPException(404, detail="Pedido não encontrado ou sem chave de consulta")

        body = _build_omie_consulta_body(dict(row))
        if not body:
            await marcar_sem_chave(conn, int(row["pk"]))
            raise HTTPException(422, detail="Sem chave (id ou codigo_pedido_integracao)")

        data = await omie_call("ConsultarPedido", body)
        if "faultstring" in data:
            raise HTTPException(502, detail=data["faultstring"])

        await salvar_detalhe_ok(conn, int(row["pk"]), data)
        return {"ok": True, "pk": int(row["pk"])}


# --------------------------- ADM: backfill (importa lista) -------------------
@app.post("/admin/backfill")
async def backfill(
    secret: str = Query(...),
    inicio: str = Query(..., description="YYYY-MM-DD ou ISO"),
    fim: str = Query(..., description="YYYY-MM-DD ou ISO"),
    por_pagina: int = Query(200, ge=50, le=500),
):
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "nope")

    dt_ini = _parse_dt(inicio)
    dt_fim = _parse_dt(fim)

    # Omie espera dd/mm/yyyy
    d_ini = dt_ini.astimezone(timezone.utc).strftime("%d/%m/%Y")
    d_fim = dt_fim.astimezone(timezone.utc).strftime("%d/%m/%Y")

    importados = 0
    paginas = 0

    pool = await get_pool()
    async with pool.acquire() as conn:
        pagina = 1
        while True:
            payload = {
                "pagina": pagina,
                "registros_por_pagina": por_pagina,
                "apenas_importado_api": "N",
                "filtrar_por_data_de": d_ini,
                "filtrar_por_data_ate": d_fim,
                # a API não tem filtro de hora; filtramos depois usando infoCadastro (quando disponível)
            }
            data = await omie_call("ListarPedidos", payload)
            paginas += 1

            lista = data.get("pedido_venda_produto", []) or data.get("lista_pedidos", [])
            if not isinstance(lista, list):
                lista = []

            for item in lista:
                # cada item costuma vir com cabecalho + infoCadastro (depende do plano)
                # guardamos como "basico" e marcamos para detalhar
                raw = {
                    "numero": str(
                        item.get("cabecalho", {}).get("numero_pedido")
                        or item.get("numero_pedido")
                        or ""
                    ),
                    "idPedido": item.get("cabecalho", {}).get("codigo_pedido")
                    or item.get("codigo_pedido"),
                    "codigo_pedido_integracao": item.get("cabecalho", {}).get("codigo_pedido_integracao")
                    or item.get("codigo_pedido_integracao"),
                    "infoCadastro": item.get("infoCadastro", {}),
                }

                # filtro por hora, se vier infoCadastro
                try:
                    d_inc = raw.get("infoCadastro", {}).get("dInc")
                    h_inc = raw.get("infoCadastro", {}).get("hInc")
                    if d_inc and h_inc:
                        dt_inc = datetime.strptime(f"{d_inc} {h_inc}", "%d/%m/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
                        if not (dt_ini <= dt_inc <= dt_fim):
                            continue
                except Exception:
                    pass

                pk = await salvar_basico(conn, raw)
                importados += 1

            # paginação
            tot_paginas = data.get("total_de_paginas") or data.get("nPaginas") or 1
            if pagina >= int(tot_paginas):
                break
            pagina += 1

    return {"ok": True, "importados": importados, "paginas": paginas}


# --------------------------- ADM: run-jobs (gatilho simples) -----------------
@app.post("/admin/run-jobs")
async def run_jobs(secret: str = Query(...), lotes: int = Query(3), limit: int = Query(200)):
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "nope")

    total_ok = total_err = total_encontrados = 0
    for _ in range(max(1, lotes)):
        r = await reprocessar_pendentes(ADMIN_SECRET, limit)  # type: ignore
        total_ok += r["processados_ok"]
        total_err += r["processados_erro"]
        total_encontrados += r["pendentes_encontrados"]
        await asyncio.sleep(0.2)

    return {
        "ok": True,
        "lotes": lotes,
        "pendentes_encontrados": total_encontrados,
        "processados_ok": total_ok,
        "processados_erro": total_err,
    }
