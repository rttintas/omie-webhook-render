# -*- coding: utf-8 -*-
"""
SYNC NFE (saídas) - OMIE
- Busca NFe via /api/v1/produtos/nfe/ (CALL=ListarNFe)
- Filtra localmente: tpNF == 'S' e dEmissao >= checkpoint (omie.sync_control)
- Pagina com nPagina / nRegPorPagina
- Upsert em omie_nfe (se existir) OU em omie_nfe_raw (cria se não existir)
- Atualiza checkpoint ao final
"""

import os
import json
import time
import logging
import datetime as dt
from typing import Any, Dict, List, Tuple

# utilidades compartilhadas do seu projeto
from sync_common import (
    setup_logger, get_conn, get_checkpoint, set_checkpoint,
    omie_call
)

# ======================================================================
# PATCH DO LOGGER (impede logger virar string)
# ======================================================================
_tmp_logger = setup_logger("sync_nfe")
if isinstance(_tmp_logger, (tuple, list)):
    logger = _tmp_logger[0]
    log_path = _tmp_logger[1] if len(_tmp_logger) > 1 else None
else:
    logger = _tmp_logger
    log_path = None
if not hasattr(logger, "info"):
    raise TypeError(f"setup_logger retornou tipo inválido: {type(logger)}")

# ======================================================================
# CONFIG OMIE
# ======================================================================
APP_KEY    = os.getenv("OMIE_APP_KEY")
APP_SECRET = os.getenv("OMIE_APP_SECRET")

URL_NFE = "https://app.omie.com.br/api/v1/produtos/nfe/"
CALL    = "ListarNFe"

ENTIDADE = "nfe"   # usada no controle de checkpoint

# paginação (ajuste se quiser)
NREG_POR_PAGINA = 200

# ======================================================================
# HELPERS
# ======================================================================
def parse_data_omie(valor: Any) -> dt.datetime | None:
    """Aceita 'dd/mm/aaaa', 'aaaa-mm-dd', datetime/date/None"""
    if valor is None:
        return None
    if isinstance(valor, dt.datetime):
        return valor
    if isinstance(valor, dt.date):
        return dt.datetime.combine(valor, dt.time())
    s = str(valor).strip()
    if not s:
        return None

    # dd/mm/aaaa
    try:
        return dt.datetime.strptime(s, "%d/%m/%Y")
    except Exception:
        pass

    # aaaa-mm-dd
    try:
        return dt.datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        pass

    # aaaa-mm-ddTHH:MM:SS (ou variações)
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def existe_tabela(cur, schema: str, nome: str) -> bool:
    cur.execute("""
        select 1
        from information_schema.tables
        where table_schema = %s and table_name = %s
        """, (schema, nome))
    return cur.fetchone() is not None


def colunas_tabela(cur, schema: str, nome: str) -> List[str]:
    cur.execute("""
        select column_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
        order by ordinal_position
        """, (schema, nome))
    return [r[0] for r in cur.fetchall()]


def upsert_nfe_generico(cur, item: Dict[str, Any]) -> None:
    """
    Tenta gravar em omie_nfe (se existir) usando colunas comuns.
    Se não existir, cria/stage em omie_nfe_raw (id_nfe, emissao, raw jsonb).
    """
    # campos comuns que costumam vir na listagem
    # (ajuste se quiser acrescentar mais)
    cab = item.get("cabecalho", {}) if isinstance(item.get("cabecalho"), dict) else item
    numero   = cab.get("numeroNFe") or cab.get("numero_nf") or cab.get("numero") or cab.get("nNF")
    serie    = cab.get("serie") or cab.get("serieNFe") or cab.get("nSerie")
    chave    = cab.get("chaveNFe") or cab.get("chave") or cab.get("chNFe")
    tpNF     = cab.get("tpNF") or cab.get("tipo")  # S = Saída
    emissao  = cab.get("dEmissao") or cab.get("dEmi") or cab.get("data_emissao") or cab.get("emissao")
    emissao  = parse_data_omie(emissao)
    cod_cli  = cab.get("codigo_cliente") or cab.get("codigo_cliente_omie") or cab.get("cCliente")
    valor    = (
        cab.get("valor_total") or cab.get("valor_total_nfe")
        or cab.get("vNF") or cab.get("valor") or 0
    )

    # existe omie_nfe?
    target_schema = "omie"
    target_table  = "omie_nfe"
    if existe_tabela(cur, target_schema, target_table):
        cols = colunas_tabela(cur, target_schema, target_table)
        # Monta um upsert flexível com o que existir
        campos = {}
        if "numero" in cols:            campos["numero"] = numero
        if "serie" in cols:             campos["serie"] = serie
        if "tp_nf" in cols:             campos["tp_nf"] = tpNF
        if "chave" in cols:             campos["chave"] = chave
        if "emissao" in cols:           campos["emissao"] = emissao
        if "cliente_codigo" in cols:    campos["cliente_codigo"] = cod_cli
        if "valor_total" in cols:       campos["valor_total"] = valor
        if "raw" in cols:               campos["raw"] = json.dumps(item, ensure_ascii=False)

        # precisa de uma PK/unique pra dar upsert:
        # preferimos 'chave' (única por NFe); se não houver, usa (serie,numero)
        if "chave" in cols:
            # garante chave como natural key
            cur.execute(f"""
                insert into {target_schema}.{target_table} ({", ".join(campos.keys())})
                values ({", ".join(["%s"]*len(campos))})
                on conflict (chave) do update set
                    {", ".join([f'{k}=excluded.{k}' for k in campos.keys() if k != "chave"])}
            """, list(campos.values()))
        elif "serie" in cols and "numero" in cols:
            # tenta unique por (serie, numero)
            cur.execute(f"""
                insert into {target_schema}.{target_table} ({", ".join(campos.keys())})
                values ({", ".join(["%s"]*len(campos))})
                on conflict (serie, numero) do update set
                    {", ".join([f'{k}=excluded.{k}' for k in campos.keys() if k not in ("serie","numero")])}
            """, list(campos.values()))
        else:
            # não temos como fazer upsert consistente -> cai para RAW
            _upsert_raw(cur, item, emissao)
    else:
        # cria RAW se não existir e grava
        _ensure_raw_table(cur)
        _upsert_raw(cur, item, emissao)


def _ensure_raw_table(cur) -> None:
    cur.execute("""
        create table if not exists omie.omie_nfe_raw (
            id_nfe text primary key,
            emissao timestamp null,
            raw jsonb not null,
            created_at timestamp not null default now(),
            updated_at timestamp not null default now()
        )
    """)
    # ajuda na busca por período
    cur.execute("""create index if not exists ix_omie_nfe_raw_emissao on omie.omie_nfe_raw(emissao)""")


def _upsert_raw(cur, item: Dict[str, Any], emissao: dt.datetime | None) -> None:
    cab = item.get("cabecalho", {}) if isinstance(item.get("cabecalho"), dict) else item
    chave  = cab.get("chaveNFe") or cab.get("chave") or cab.get("chNFe")
    numero = cab.get("numeroNFe") or cab.get("numero_nf") or cab.get("numero") or cab.get("nNF")
    serie  = cab.get("serie") or cab.get("serieNFe") or cab.get("nSerie")
    # id_nfe coeso mesmo se faltarem campos
    id_nfe = chave or f"{serie or ''}-{numero or ''}"

    cur.execute("""
        insert into omie.omie_nfe_raw (id_nfe, emissao, raw)
        values (%s, %s, %s)
        on conflict (id_nfe) do update set
            emissao = excluded.emissao,
            raw     = excluded.raw,
            updated_at = now()
    """, (id_nfe, emissao, json.dumps(item, ensure_ascii=False)))


def listar_nfe() -> Tuple[int, List[Dict[str, Any]]]:
    """
    Lê todas as páginas de ListarNFe (sem filtro de data) e retorna (total_paginas, itens).
    """
    itens: List[Dict[str, Any]] = []
    n_pag = 1
    total_paginas = None

    while True:
        params = {"nPagina": n_pag, "nRegPorPagina": NREG_POR_PAGINA}
        resp = omie_call(URL_NFE, CALL, APP_KEY, APP_SECRET, params)
        # estrutura padrão:
        # {'nPagina': 1, 'nTotPaginas': 0, 'nRegPorPagina': 1, 'nTotRegistros': 0, 'listagemNfe': []}
        nPagina       = resp.get("nPagina", n_pag)
        nTotPaginas   = resp.get("nTotPaginas", 0)
        nRegPorPag    = resp.get("nRegPorPagina", NREG_POR_PAGINA)
        listagem      = resp.get("listagemNfe") or resp.get("listagem_nfe") or []
        if isinstance(listagem, list):
            itens.extend(listagem)

        if total_paginas is None:
            total_paginas = int(nTotPaginas or 0)

        logger.debug(f"Página {nPagina}/{total_paginas or '?'} | recebidos {len(listagem)} itens | nRegPorPag {nRegPorPag}")
        # controla saída
        if total_paginas is None or total_paginas <= 1:
            break
        if n_pag >= total_paginas:
            break

        n_pag += 1
        time.sleep(0.2)  # pequeno respiro p/ API

    return total_paginas or 0, itens


def main():
    logger.info("== INÍCIO SYNC_NFE ==")

    if not APP_KEY or not APP_SECRET:
        raise RuntimeError("Variáveis OMIE_APP_KEY / OMIE_APP_SECRET não definidas.")

    # checkpoint
    desde: dt.datetime = get_checkpoint(ENTIDADE)  # já retorna datetime
    logger.info(f"checkpoint atual: {desde}")

    # leitura OMIE
    total_pag, itens = listar_nfe()
    logger.info(f"OMIE retornou total_paginas={total_pag} | itens_recebidos={len(itens)} (antes do filtro)")

    # filtro local (somente saída + desde)
    selecionados: List[Dict[str, Any]] = []
    for it in itens:
        cab = it.get("cabecalho", {}) if isinstance(it.get("cabecalho"), dict) else it
        tpNF = cab.get("tpNF") or cab.get("tipo")
        dEmissao = cab.get("dEmissao") or cab.get("dEmi") or cab.get("data_emissao") or cab.get("emissao")
        dEmissao = parse_data_omie(dEmissao)

        if tpNF == "S" and dEmissao is not None and dEmissao >= desde:
            selecionados.append(it)

    logger.info(f"Após filtro local (saida + >= checkpoint): {len(selecionados)} itens")

    # gravação
    if not selecionados:
        logger.info("Nenhum item para gravar. Mantendo checkpoint.")
        logger.info("== FIM SYNC_NFE ==")
        return

    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            upserts = 0
            for it in selecionados:
                upsert_nfe_generico(cur, it)
                upserts += 1

            conn.commit()
            logger.info(f"Gravados/atualizados: {upserts}")

            # novo checkpoint = maior dEmissao gravado
            maior = None
            for it in selecionados:
                cab = it.get("cabecalho", {}) if isinstance(it.get("cabecalho"), dict) else it
                dEmissao = cab.get("dEmissao") or cab.get("dEmi") or cab.get("data_emissao") or cab.get("emissao")
                dEmissao = parse_data_omie(dEmissao)
                if dEmissao and (maior is None or dEmissao > maior):
                    maior = dEmissao

            if maior and maior > desde:
                set_checkpoint(ENTIDADE, maior)
                logger.info(f"Checkpoint atualizado para: {maior}")
            else:
                logger.info("Checkpoint mantido (nenhuma emissão maior que a atual).")

    except Exception as e:
        conn.rollback()
        logger.exception(f"Erro ao gravar NFe: {e}")
        raise
    finally:
        conn.close()

    logger.info("== FIM SYNC_NFE ==")


if __name__ == "__main__":
    main()
