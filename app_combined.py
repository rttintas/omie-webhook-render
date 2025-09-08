import base64
import json
import logging
from datetime import datetime, timedelta  # CORREÇÃO: Adicionada a importação de timedelta
from typing import Any, Dict, Optional, Tuple

import asyncpg
import httpx

# --- 1. Configurações e Constantes (ajuste com seus valores) ---

# Configure o logger conforme seu ambiente
logger = logging.getLogger("app_combined")

# Suas constantes da Omie (substitua pelos valores reais)
OMIE_XML_URL = "https://app.omie.com.br/api/v1/contador/xml/"
OMIE_XML_LIST_CALL = "ListarDocumentos"
OMIE_TIMEOUT_SECONDS = 30.0


# --- 2. Funções Auxiliares (substitua com suas implementações reais) ---
# Estas são funções que seu código original utiliza.
# Coloquei implementações de exemplo para o código ser coeso.

async def _omie_post(client: httpx.AsyncClient, url: str, call: str, payload: Dict) -> Dict:
    # Esta função deve encapsular sua lógica de chamada à API Omie,
    # incluindo app_key e app_secret.
    # Exemplo:
    # app_key = "SEU_APP_KEY"
    # app_secret = "SEU_APP_SECRET"
    # request_data = {"call": call, "param": [payload], "app_key": app_key, "app_secret": app_secret}
    # response = await client.post(url, json=request_data, timeout=OMIE_TIMEOUT_SECONDS)
    # response.raise_for_status()
    # return response.json()
    logger.info(f"Simulando _omie_post para call: {call} com payload: {payload}")
    # Retorno simulado para fins de exemplo
    return {"nTotPáginas": 0, "documentos": []}

def _pick(data: Dict, *keys: str) -> Any:
    """Retorna o primeiro valor não nulo para uma lista de chaves."""
    for key in keys:
        if key in data and data[key]:
            return data[key]
    return None

def _safe_text(value: Any) -> Optional[str]:
    """Converte um valor para string de forma segura."""
    return str(value).strip() if value is not None else None

def _parse_dt(value: Any) -> Optional[datetime]:
    """Converte uma string de data para um objeto datetime."""
    if not value:
        return None
    try:
        # Tenta formatos comuns
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None

def _date_range_for_omie(dt: Optional[datetime]) -> Tuple[str, str]:
    """Cria um intervalo de datas no formato D/M/AAAA para a API Omie."""
    if not dt:
        dt = datetime.now()
    # Exemplo simples: pega o primeiro e último dia do mês
    first_day = dt.replace(day=1)
    last_day = (first_day.replace(month=first_day.month % 12 + 1, day=1) - timedelta(days=1))
    return first_day.strftime("%d/%m/%Y"), last_day.strftime("%d/%m/%Y")


# --- 3. Lógica Principal Corrigida ---

async def _buscar_conteudo_xml_via_api(client: httpx.AsyncClient, chave: str, data_emis: Optional[datetime]) -> Optional[str]:
    """
    Consulta a API Contador/XML -> ListarDocumentos e retorna o CONTEÚDO do XML da NF-e específica.
    """
    if not data_emis and chave and len(chave) >= 4:
        try:
            ano = int(chave[2:4])
            mes = int(chave[4:6])
            data_emis = datetime(2000 + ano, mes, 1)
        except (ValueError, IndexError):
            pass
    
    dEmiInicial, dEmiFinal = _date_range_for_omie(data_emis)
    
    pagina = 1
    registros_por_pagina = 50
    
    while pagina <= 10:  # Limite de 10 páginas para evitar loop infinito
        payload = {
            "nPagina": pagina,
            "nRegPorPagina": registros_por_pagina,
            "cModelo": "55",
            "dEmiInicial": dEmiInicial,
            "dEmiFinal": dEmiFinal
        }
        
        try:
            logger.info(f"🔎 Buscando XML para chave {chave} na API - Página {pagina}...")
            resp = await _omie_post(client, OMIE_XML_URL, OMIE_XML_LIST_CALL, payload)
            
            # A resposta pode ter a lista de documentos em uma chave variável
            documentos = next((v for v in resp.values() if isinstance(v, list)), [])

            def _get_api_value(doc, *keys):
                if not isinstance(doc, dict): return None
                for key in keys:
                    if key in doc and doc[key]: return str(doc[key]).strip()
                return None
            
            for documento in documentos:
                # Na API, a chave da NFe vem no campo "nChave"
                chave_doc = _get_api_value(documento, "nChave") 
                
                if chave_doc and chave_doc == chave:
                    # A API retorna o CONTEÚDO do XML no campo "cXml"
                    xml_content = _get_api_value(documento, "cXml")
                    
                    if xml_content:
                        logger.info(f"✅ XML da NF-e {chave} encontrado diretamente na resposta da API (Página {pagina}).")
                        return xml_content  # Retorna o CONTEÚDO do XML

            # Lógica de paginação baseada na documentação
            total_paginas = resp.get("nTotPáginas")
            if total_paginas and pagina >= int(total_paginas):
                logger.info(f"Fim da busca na API, total de páginas ({total_paginas}) atingido.")
                break
                
            pagina += 1
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar na página {pagina} da API: {e}")
            break
    
    logger.warning(f"⚠️ NF-e {chave} não encontrada na busca via API após {pagina-1} páginas.")
    return None

async def processar_nfe(conn: asyncpg.Connection, payload: Dict[str, Any], client: httpx.AsyncClient) -> bool:
    """
    Processa uma NF-e específica com tratamento robusto para obter o XML,
    seja por URL (webhook) ou por conteúdo direto (API).
    """
    try:
        event = payload.get("event", {}) or {}
        
        chave = _safe_text(_pick(event, "nfe_chave", "chave", "chaveNFe"))
        numero_nf = _safe_text(_pick(event, "id_nf", "numero_nf", "nNumero"))
        serie = _safe_text(_pick(event, "serie", "cSerie"))
        data_emis = _parse_dt(_pick(event, "data_emis", "dhEmissao", "dEmissao"))
        xml_url = _safe_text(_pick(event, "nfe_xml", "xml_url"))
        danfe_url = _safe_text(_pick(event, "nfe_danfe", "danfe_url"))
        cnpj_emit = _safe_text(_pick(event, "empresa_cnpj", "cnpj_emitente"))
        status = _safe_text(_pick(event, "acao", "status"))

        logger.info(f"🔍 Processando NF-e: Chave={chave}, Número={numero_nf}")

        if not chave:
            logger.warning("❌ NF-e sem chave no payload, impossível continuar.")
            return False

        xml_text = None
        xml_content_bytes = None

        # --- LÓGICA ROBUSTA PARA OBTER O XML ---
        
        # 1. Tenta baixar o XML pela URL (se fornecida pelo webhook)
        if xml_url:
            try:
                logger.info(f"Baixando XML da URL fornecida para NF-e {chave}...")
                r = await client.get(xml_url, timeout=OMIE_TIMEOUT_SECONDS)
                r.raise_for_status()
                xml_text = r.text
                xml_content_bytes = r.content
            except Exception as e:
                logger.error(f"❌ Falha ao baixar XML da URL: {e}. Tentando busca alternativa via API.")
                xml_url = None # Limpa a URL para forçar a busca na API

        # 2. Se não conseguiu pela URL, busca o CONTEÚDO na API
        if not xml_text:
            logger.warning("⚠️ NF-e sem XML — tentando buscar conteúdo via API Contador/XML…")
            xml_text = await _buscar_conteudo_xml_via_api(client, chave, data_emis)
            if xml_text:
                xml_content_bytes = xml_text.encode('utf-8')

        # 3. Se ainda não tem o XML, falha o processamento
        if not xml_text:
            logger.warning("❌ NF-e segue sem conteúdo XML após todas as tentativas — não será possível salvar.")
            return False

        # --- PROCESSAMENTO E SALVAMENTO NO BANCO ---

        try:
            logger.info(f"✅ Conteúdo XML para a NF-e {chave} obtido com sucesso. Processando...")
            
            # Limpeza do XML para armazenamento
            cleaned_xml = ' '.join(xml_text.replace('\r', '').replace('\n', ' ').split())
            
            # Codificação em Base64 para armazenamento
            xml_base64 = base64.b64encode(xml_content_bytes).decode("utf-8")
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar o conteúdo do XML: {e}")
            return False

        # Salva na tabela principal (omie_nfe)
        await conn.execute("""
            INSERT INTO public.omie_nfe
                (chave_nfe, numero, serie, emitida_em, cnpj_emitente, status, xml, xml_url, danfe_url, last_event_at, updated_at, recebido_em, raw)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),now(),now(),$10)
            ON CONFLICT (chave_nfe) DO UPDATE SET
                numero        = EXCLUDED.numero,
                serie         = COALESCE(EXCLUDED.serie, omie_nfe.serie),
                emitida_em    = COALESCE(EXCLUDED.emitida_em, omie_nfe.emitida_em),
                cnpj_emitente = COALESCE(EXCLUDED.cnpj_emitente, omie_nfe.cnpj_emitente),
                status        = COALESCE(EXCLUDED.status, omie_nfe.status),
                xml           = EXCLUDED.xml,
                xml_url       = EXCLUDED.xml_url,
                danfe_url     = COALESCE(EXCLUDED.danfe_url, omie_nfe.danfe_url),
                last_event_at = now(),
                updated_at    = now(),
                raw           = EXCLUDED.raw;
        """, chave, numero_nf, serie, data_emis, cnpj_emit, status, cleaned_xml, xml_url, danfe_url,
        json.dumps(payload, ensure_ascii=False, default=str))

        # Salva na tabela de XMLs (omie_nfe_xml)
        await conn.execute("""
            INSERT INTO public.omie_nfe_xml (chave_nfe, numero, serie, emitida_em, xml_base64, recebido_em, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,now(),now(),now())
            ON CONFLICT (chave_nfe) DO UPDATE SET
                numero     = EXCLUDED.numero,
                serie      = COALESCE(EXCLUDED.serie, omie_nfe_xml.serie),
                emitida_em = COALESCE(EXCLUDED.emitida_em, omie_nfe_xml.emitida_em),
                xml_base64 = EXCLUDED.xml_base64,
                updated_at = now();
        """, chave, numero_nf, serie, data_emis, xml_base64)

        logger.info(f"✅ NF-e {chave} salva no banco de dados.")
        return True

    except Exception as e:
        logger.error(f"❌ Erro inesperado ao processar NF-e: {e}", exc_info=True)
        return False

# CORREÇÃO: Removido o "}" extra que estava aqui causando um erro de sintaxe.