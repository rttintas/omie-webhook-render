# sync_nfe.py — NF-e emitidas: autodiscovery + overrides por CLI/ENV
from __future__ import annotations
import os, sys, json, time, argparse, logging, datetime as dt, pathlib
from typing import Any, Dict, List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values
import requests

log = logging.getLogger("SYNC_NFE")
ROOT = pathlib.Path(__file__).resolve().parent
CACHE = ROOT / ".omie_nfe_endpoint.json"

# Candidatos default (caso não haja override)
CANDIDATES = [
    ("https://app.omie.com.br/api/v1/produtos/nfemitidas/", "ListarNFeEmitidas"),
    ("https://app.omie.com.br/api/v1/produtos/nfemitidas/", "ListarNfEmitidas"),
    ("https://app.omie.com.br/api/v1/produtos/nfe/",        "ListarNFeEmitidas"),
    ("https://app.omie.com.br/api/v1/produtos/nfe/",        "ListarNFe"),
    ("https://app.omie.com.br/api/v1/produtos/nf/",         "ListarNFe"),
    ("https://app.omie.com.br/api/v1/vendas/nfe/",          "ListarNFe"),
]

def build_params(profile: int, pagina: int, desde: Optional[dt.datetime], ate: Optional[dt.datetime]) -> Dict[str, Any]:
    base = {"pagina": pagina, "registros_por_pagina": 200}
    if profile == 0:
        return base
    if desde is None:
        return base
    d = desde.strftime("%Y-%m-%d")
    a = (ate or desde).strftime("%Y-%m-%d")
    if profile == 1: base.update({"data_emissao_de": d, "data_emissao_ate": a})
    elif profile == 2: base.update({"DATA_EMISSAO_DE": d, "DATA_EMISSAO_ATE": a})
    elif profile == 3: base.update({"dataEmissaoDe": d, "dataEmissaoAte": a})
    elif profile == 4: base.update({"filtrar_por_data_de": d, "filtrar_por_data_ate": a})
    elif profile == 5: base.update({"data_de": d, "data_ate": a})
    elif profile == 6: base.update({"apenas_emitidas": True})
    elif profile == 7: base.update({"emitidas": True})
    return base

PARAM_PROFILES = [0,1,2,3,4,5,6,7]

def setup_logging(debug: bool):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")

def env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return val

def parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sincroniza NF-e (emitidas) da Omie para Postgres.")
    p.add_argument("--desde", help="YYYY-MM-DD (se ausente, usa checkpoint).")
    p.add_argument("--ate", help="YYYY-MM-DD (opcional).")
    p.add_argument("--debug", action="store_true", help="Ativa logs DEBUG.")
    # overrides:
    p.add_argument("--url", help="Override: URL do serviço Omie (ex.: https://app.omie.com.br/api/v1/xxx/).")
    p.add_argument("--call", help="Override: método exato do serviço (ex.: ListarNFe).")
    p.add_argument("--profile", type=int, choices=PARAM_PROFILES, help="Override: perfil de parâmetros (0..7).")
    p.add_argument("--dry-run", action="store_true", help="Não grava no banco; só testa 1 página e imprime o retorno.")
    return p.parse_args()

def try_parse_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s: return None
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d","%d/%m/%Y %H:%M:%S","%d/%m/%Y"):
        try: return dt.datetime.strptime(s, fmt)
        except: pass
    return None

def omie_call(url: str, call: str, app_key: str, app_secret: str, params: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"call": call, "app_key": app_key, "app_secret": app_secret, "param": [params]}
    r = requests.post(url, json=payload, timeout=60)
    log.debug("POST %s -> %s", url, r.status_code)
    if r.status_code != 200:
        log.debug("OMIE BODY: %s", r.text[:2000])
        raise RuntimeError(f"HTTP {r.status_code} em {url}")
    data = r.json()
    if isinstance(data, dict) and (data.get("status") == "error" or "faultstring" in data or "faultcode" in data):
        raise RuntimeError((r.text or "").strip()[:800])
    return data

def discover(app_key: str, app_secret: str, desde: Optional[dt.datetime], ate: Optional[dt.datetime]) -> Tuple[str,str,int]:
    # 1) overrides via ENV
    url_env  = os.getenv("OMIE_NFE_URL")
    call_env = os.getenv("OMIE_NFE_CALL")
    prof_env = os.getenv("OMIE_NFE_PROFILE")
    if url_env and call_env:
        profile = int(prof_env) if prof_env and prof_env.isdigit() else 0
        log.info("Override via ENV: %s | %s | profile=%s", url_env, call_env, profile)
        return url_env, call_env, profile

    # 2) cache
    if CACHE.exists():
        try:
            c = json.loads(CACHE.read_text(encoding="utf-8"))
            if all(k in c for k in ("url","call","profile")):
                log.info("Usando endpoint previamente descoberto: %s | %s | profile=%s", c["url"], c["call"], c["profile"])
                return c["url"], c["call"], int(c["profile"])
        except: pass

    # 3) brute force
    for url, call in CANDIDATES:
        for profile in PARAM_PROFILES:
            try:
                _ = omie_call(url, call, app_key, app_secret, build_params(profile, 1, desde, ate))
                CACHE.write_text(json.dumps({"url": url, "call": call, "profile": profile}), encoding="utf-8")
                log.info("Descoberto: %s | %s | profile=%s", url, call, profile)
                return url, call, profile
            except Exception as e:
                log.debug("Falhou %s | %s | profile=%s -> %s", url, call, profile, str(e)[:200])
                time.sleep(0.2)
    raise RuntimeError("Não consegui descobrir endpoint/método/param válido p/ NF-e na sua conta.")

def paginar(url: str, call: str, profile: int, app_key: str, app_secret: str,
            desde: Optional[dt.datetime], ate: Optional[dt.datetime]):
    pagina = 1
    while True:
        req = build_params(profile, pagina, desde, ate)
        tent = 0
        while True:
            try:
                data = omie_call(url, call, app_key, app_secret, req)
                break
            except Exception as e:
                tent += 1
                if tent >= 5:
                    raise RuntimeError("Falha OMIE após 5 tentativas") from e
                time.sleep(1.0 * tent)

        lista = (data.get("lista") or data.get("nfes") or data.get("nfe")
                 or data.get("nf") or data.get("listaNFe") or [])
        if not isinstance(lista, list) and isinstance(lista, dict):
            lista = lista.get("lista", [])
        yield pagina, lista, data

        total_paginas = data.get("total_de_paginas") or data.get("total_paginas") or data.get("nr_total_de_paginas")
        pag_atual = data.get("pagina") or data.get("nr_pagina") or pagina
        if total_paginas:
            if int(pag_atual) >= int(total_paginas): break
        else:
            if len(lista) < 200: break
        pagina += 1

def get_pg():
    return psycopg2.connect(
        host=env("PGHOST"), port=int(env("PGPORT","5432")),
        dbname=env("PGDATABASE"), user=env("PGUSER"), password=env("PGPASSWORD"),
    )

def get_checkpoint(cur) -> Optional[dt.datetime]:
    cur.execute("SELECT last_sync_at FROM omie.sync_control WHERE entidade='nfe'")
    row = cur.fetchone()
    return row[0] if row else None

def set_checkpoint(cur, ts: dt.datetime):
    cur.execute("""
        INSERT INTO omie.sync_control (entidade, last_sync_at)
        VALUES ('nfe', %s)
        ON CONFLICT (entidade) DO UPDATE SET last_sync_at = EXCLUDED.last_sync_at
    """, (ts,))

def try_parse_any_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s: return None
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d","%d/%m/%Y %H:%M:%S","%d/%m/%Y"):
        try: return dt.datetime.strptime(s, fmt)
        except: pass
    return None

def norm_item(raw: Dict[str, Any]) -> Tuple[str,int,str,dt.datetime,float]:
    chave = raw.get("chave_nfe") or raw.get("chaveNFe") or raw.get("chNFe") or raw.get("chave")
    num   = raw.get("numero_nfe") or raw.get("numeroNFe") or raw.get("nNF") or raw.get("numero")
    serie = raw.get("serie_nfe")  or raw.get("serieNFe")  or raw.get("serie")
    data  = raw.get("data_emissao") or raw.get("dtEmissao") or raw.get("dataEmissao") or raw.get("emissao")
    valor = raw.get("valor_total") or raw.get("vlTotal") or raw.get("valorTotal") or raw.get("valor")
    try: num = int(num) if num is not None else 0
    except: num = 0
    serie = str(serie or "")
    dte = try_parse_any_dt(data) or dt.datetime(1970,1,1)
    try: valor = float(str(valor).replace(",", ".")) if valor is not None else 0.0
    except: valor = 0.0
    if not chave: chave = f"NO-CHAVE-{num}-{serie}-{data}"
    return (chave, num, serie, dte, valor)

def upsert(cur, rows: List[Tuple[str,int,str,dt.datetime,float]]):
    if not rows: return
    sql = """
        INSERT INTO omie.omie_nfe (chave_nfe, numero_nfe, serie_nfe, data_emissao, valor_total)
        VALUES %s
        ON CONFLICT (chave_nfe) DO UPDATE SET
            numero_nfe=EXCLUDED.numero_nfe,
            serie_nfe=EXCLUDED.serie_nfe,
            data_emissao=EXCLUDED.data_emissao,
            valor_total=EXCLUDED.valor_total
    """
    execute_values(cur, sql, rows, page_size=200)

def main():
    args = parse_cli()
    setup_logging(args.debug)
    log.info("== INÍCIO SYNC_NFE ==")

    APP_KEY    = env("OMIE_APP_KEY")
    APP_SECRET = env("OMIE_APP_SECRET")

    # overrides por CLI
    url_override  = args.url or os.getenv("OMIE_NFE_URL")
    call_override = args.call or os.getenv("OMIE_NFE_CALL")
    prof_override = args.profile if args.profile is not None else (int(os.getenv("OMIE_NFE_PROFILE", "0")) if os.getenv("OMIE_NFE_PROFILE") else None)

    ate_dt = dt.datetime.strptime(args.ate, "%Y-%m-%d") if args.ate else None

    with get_pg() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            if args.desde:
                desde_dt = dt.datetime.strptime(args.desde, "%Y-%m-%d")
            else:
                desde_dt = get_checkpoint(cur) or dt.datetime(1970,1,1)
            log.info("Checkpoint atual: %s", desde_dt)

            if url_override and call_override:
                url, call = url_override, call_override
                profile = prof_override if prof_override is not None else 0
                log.info("Override em uso: %s | %s | profile=%s", url, call, profile)
            else:
                url, call, profile = discover(APP_KEY, APP_SECRET, desde_dt, ate_dt)
                log.info("Usando: %s | %s | profile=%s", url, call, profile)

            total = 0
            maior_emissao: Optional[dt.datetime] = None

            for pagina, lista, bruto in paginar(url, call, profile, APP_KEY, APP_SECRET, desde_dt, ate_dt):
                log.debug("Página %s recebida: %d itens", pagina, len(lista))

                if args.dry_run:
                    print("\n=== DRY-RUN: payload bruto da página 1 ===")
                    print(json.dumps(bruto, ensure_ascii=False)[:8000])
                    break

                batch = [norm_item(r) for r in lista]
                upsert(cur, batch)
                total += len(batch)
                for _,_,_,dte,_ in batch:
                    if dte and (maior_emissao is None or dte > maior_emissao):
                        maior_emissao = dte
                if maior_emissao:
                    set_checkpoint(cur, maior_emissao); conn.commit()
                    log.debug("Página %s aplicada. Parcial checkpoint=%s", pagina, maior_emissao)

            if not args.dry_run:
                if not maior_emissao: maior_emissao = desde_dt
                set_checkpoint(cur, maior_emissao); conn.commit()
                log.info("== FIM SYNC_NFE == total upserts: %s  novo checkpoint: %s", total, maior_emissao)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrompido pelo usuário."); sys.exit(130)
    except Exception as e:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
        log.exception("Erro fatal no sync_nfe.py: %s", e); sys.exit(1)
