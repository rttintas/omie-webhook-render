# === BACKFILL DE PEDIDOS POR PERÍODO (cole no seu main.py) ===================
import asyncio
from datetime import datetime, date, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import Query, HTTPException

# --- Ajuda: formatos de data/hora usados pela Omie (dd/MM/yyyy) --------------
def _br(d: date) -> str:
    return d.strftime("%d/%m/%Y")

def _dt_br(d: datetime) -> Tuple[str, str]:
    # dInc (data) e hInc (hora) que vêm no payload detalhado
    return d.strftime("%d/%m/%Y"), d.strftime("%H:%M:%S")

# --- Monta variações de filtros (a Omie pode ter chaves diferentes por conta) -
# Se a Omie responder "Tag [XYZ] não faz parte...", trocamos para a próxima variante.
_PARAM_VARIANTS = [
    # mais comum
    lambda de, ate: {"dIncDe": _br(de), "dIncAte": _br(ate)},
    # outras variações que já vi em contas
    lambda de, ate: {"dataCadastroDe": _br(de), "dataCadastroAte": _br(ate)},
    lambda de, ate: {"dataInicial": _br(de), "dataFinal": _br(ate)},
]

# --- Chama Omie "ListarPedidos" paginado -------------------------------------
async def omie_listar_pedidos_periodo(
    http: httpx.AsyncClient,
    app_key: str,
    app_secret: str,
    de: date,
    ate: date,
    pagina: int,
    por_pagina: int = 500,
) -> Dict[str, Any]:
    base = {
        "pagina": pagina,
        "registros_por_pagina": por_pagina,
        # filtros adicionais comuns (opcional):
        # "apenas_importado_api": "N",
        # "ordenar_por": "DINC",
    }
    last_err = None
    for build in _PARAM_VARIANTS:
        payload = {
            "call": "ListarPedidos",
            "app_key": app_key,
            "app_secret": app_secret,
            "param": [{**base, **build(de, ate)}],
        }
        try:
            r = await http.post("https://app.omie.com.br/api/v1/produtos/pedido/", json=payload, timeout=60)
            r.raise_for_status()
            j = r.json()
            # Falhas padronizadas da Omie vêm com 'faultstring'
            if isinstance(j, dict) and j.get("faultstring"):
                # Se for erro de TAG inválida, tenta a próxima variante
                msg = str(j.get("faultstring"))
                if "Tag [" in msg and "não faz parte" in msg:
                    last_err = msg
                    continue
                raise HTTPException(502, f"Omie falhou: {msg}")
            return j
        except httpx.HTTPStatusError as e:
            # 500 também pode aparecer quando a TAG está errada
            last_err = f"HTTP {e.response.status_code}"
            continue
    raise HTTPException(502, f"Não consegui listar na Omie com nenhum formato de filtro. Último erro: {last_err}")

# --- Extrai itens da resposta de listagem (chaves variam por conta/método) ----
def _coletar_itens_lista(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Alguns retornos vêm como {"lista": [...]} ou {"pedido_venda_produto": [...]} ou {"pedidos": [...]}
    for key in ("lista", "pedido_venda_produto", "pedidos", "pedido_venda_produto_resumido"):
        v = j.get(key)
        if isinstance(v, list):
            return v
    # Algumas versões retornam dentro de "pagina" / "registros"
    if "pagina" in j and isinstance(j["pagina"], dict):
        for key in ("registros", "lista", "itens"):
            v = j["pagina"].get(key)
            if isinstance(v, list):
                return v
    # fallback vazio
    return []

# --- Pega campos chave de cada item da lista ----------------------------------
def _extrair_minimos(item: Dict[str, Any]) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Retorna: (id_pedido_omie, codigo_pedido_integracao, numero_pedido)
    """
    # os nomes mudam bastante entre "resumido" e "completo"
    cab = item.get("cabecalho", {}) if isinstance(item.get("cabecalho"), dict) else item
    id_pedido = cab.get("codigo_pedido") or cab.get("idPedido") or cab.get("id_pedido")
    if isinstance(id_pedido, str) and id_pedido.isdigit():
        id_pedido = int(id_pedido)
    codigo_int = cab.get("codigo_pedido_integracao") or cab.get("codigoPedidoIntegracao")
    numero = cab.get("numero_pedido") or cab.get("numeroPedido")
    return (id_pedido if isinstance(id_pedido, int) else None,
            str(codigo_int) if codigo_int else None,
            str(numero) if numero else None)

# --- Consulta detalhe (reaproveita seu fluxo atual) ---------------------------
async def _consultar_e_atualizar_detalhe(conn, http: httpx.AsyncClient, app_key: str, app_secret: str,
                                         id_pedido: Optional[int], codigo_integracao: Optional[str]) -> bool:
    # monta params de consulta conforme o que temos
    if id_pedido:
        param = [{"idPedido": id_pedido}]
    elif codigo_integracao:
        param = [{"codigo_pedido_integracao": codigo_integracao}]
    else:
        return False

    payload = {
        "call": "ConsultarPedido",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": param,
    }
    r = await http.post("https://app.omie.com.br/api/v1/produtos/pedido/", json=payload, timeout=60)
    r.raise_for_status()
    j = r.json()

    # guarda a resposta completa em raw_detalhe e marca 'consultado'
    await conn.execute(
        """
        UPDATE omie_pedido
           SET raw_detalhe = $1::jsonb,
               status = 'consultado'
         WHERE (id_pedido_omie = $2) OR ($3 IS NOT NULL AND codigo_pedido_integracao = $3)
        """,
        j, id_pedido, codigo_integracao
    )
    return True

# --- Endpoint: /admin/backfill ------------------------------------------------
@app.post("/admin/backfill")
async def admin_backfill(
    secret: str = Query(...),
    inicio: str = Query(..., description="Data ou datetime início (ex.: 2025-08-25 ou 2025-08-25T00:00:00-03:00)"),
    fim: str = Query(..., description="Data ou datetime fim (ex.: 2025-09-04T16:57:00-03:00)"),
):
    if secret != os.environ.get("ADMIN_SECRET", "julia-matheus"):
        raise HTTPException(401, "secret inválido")

    # parse datas
    def parse_iso(s: str) -> datetime:
        try:
            return datetime.fromisoformat(s)
        except Exception:
            # se vier só data (YYYY-MM-DD)
            return datetime.combine(datetime.strptime(s, "%Y-%m-%d").date(), time(0, 0, 0))

    dt_ini = parse_iso(inicio)
    dt_fim = parse_iso(fim)

    # normaliza para datas (filtro da Omie é por data de inclusão)
    dia_ini = dt_ini.date()
    dia_fim = dt_fim.date()

    # cliente http e conexão db
    async with httpx.AsyncClient() as http:
        async with pool.acquire() as conn:
            app_key = os.environ["OMIE_APP_KEY"]
            app_secret = os.environ["OMIE_APP_SECRET"]

            inseridos = 0
            consultados_ok = 0
            paginas = 0

            # percorre dias; em cada dia pagina até acabar
            d = dia_ini
            while d <= dia_fim:
                pagina = 1
                while True:
                    j = await omie_listar_pedidos_periodo(http, app_key, app_secret, d, d, pagina)
                    itens = _coletar_itens_lista(j)
                    if not itens:
                        break

                    for it in itens:
                        id_pedido, cod_int, numero = _extrair_minimos(it)

                        # Guarda data/hora de inclusão se disponível e filtra pela "borda" do fim (16:57).
                        dInc = (it.get("infoCadastro", {}) or {}).get("dInc")
                        hInc = (it.get("infoCadastro", {}) or {}).get("hInc")
                        ts_ok = True
                        if dInc and hInc:
                            try:
                                ts = datetime.strptime(f"{dInc} {hInc}", "%d/%m/%Y %H:%M:%S")
                                # aplica recorte: até dt_fim exato
                                if datetime.combine(dInc and ts.date(), ts.time()) > dt_fim and d == dia_fim:
                                    ts_ok = False
                            except Exception:
                                pass
                        if not ts_ok:
                            continue

                        # UPSERT "mínimo" e marca pendente_consulta
                        await conn.execute(
                            """
                            INSERT INTO omie_pedido (numero, id_pedido_omie, codigo_pedido_integracao, status, raw_basico)
                            VALUES ($1, $2, $3, 'pendente_consulta', $4::jsonb)
                            ON CONFLICT (id_pedido_omie) DO UPDATE
                               SET numero = COALESCE(EXCLUDED.numero, omie_pedido.numero),
                                   codigo_pedido_integracao = COALESCE(EXCLUDED.codigo_pedido_integracao, omie_pedido.codigo_pedido_integracao)
                            """,
                            numero, id_pedido, cod_int, it
                        )
                        inseridos += 1

                        # consulta detalhe já na sequência
                        ok = await _consultar_e_atualizar_detalhe(conn, http, app_key, app_secret, id_pedido, cod_int)
                        if ok:
                            consultados_ok += 1

                        await asyncio.sleep(0.1)  # suaviza rate-limit

                    paginas += 1
                    # heurística de término de paginação: se veio menos que 500, acabou o dia
                    if len(itens) < 500:
                        break
                    pagina += 1

                d += timedelta(days=1)

    return {
        "ok": True,
        "inicio": dt_ini.isoformat(),
        "fim": dt_fim.isoformat(),
        "paginas_lidas": paginas,
        "inseridos_ou_atualizados": inseridos,
        "detalhes_consultados_ok": consultados_ok,
    }
# === FIM DO BLOCO =============================================================
