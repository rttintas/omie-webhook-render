# ------------------------------------------------------------------------------
# Utils (ADICIONE ESTAS FUNÇÕES)
# ------------------------------------------------------------------------------
def _get_value(doc: Dict[str, Any], *possible_keys: str) -> Optional[str]:
    """Extrai valor de um documento usando múltiplas chaves possíveis"""
    if not isinstance(doc, dict):
        return None
    for key in possible_keys:
        if key in doc and doc[key] not in (None, "", 0):
            return str(doc[key]).strip()
    return None

def _parse_dt(v: Any) -> Optional[datetime]:
    if not v: return None
    if isinstance(v, datetime): return v
    try:
        # Tenta parsear datas no formato "DD/MM/YYYY"
        if isinstance(v, str) and "/" in v and len(v) == 10:
            return datetime.strptime(v, "%d/%m/%Y")
        # Tenta parsear datas no formato ISO (padrão da API)
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None

def _date_range_for_omie(data_emis: Optional[datetime]) -> Tuple[str, str]:
    """
    Retorna datas no formato DD/MM/YYYY para a API Omie.
    Usa um intervalo de 3 dias para garantir a captura da nota.
    """
    if data_emis:
        start = (data_emis - timedelta(days=3)).strftime("%d/%m/%Y")
        end = (data_emis + timedelta(days=3)).strftime("%d/%m/%Y")
    else:
        end = (_now_utc() + timedelta(days=NFE_LOOKAHEAD_DAYS)).strftime("%d/%m/%Y")
        start = (_now_utc() - timedelta(days=NFE_LOOKBACK_DAYS)).strftime("%d/%m/%Y")
    return start, end

# ------------------------------------------------------------------------------
# NF-e helpers (VERSÃO CORRIGIDA)
# ------------------------------------------------------------------------------
async def processar_nfe(conn: asyncpg.Connection, payload: Dict[str, Any], client: httpx.AsyncClient) -> bool:
    """
    Processa um evento de NF-e, busca os dados na API Omie e salva cada
    documento na tabela 'omie_nfe' com as colunas corretas.
    """
    try:
        event_payload = payload.get("event", {}) or {}
        logger.info("🔍 Processando gatilho de NF-e...")

        # Tenta pegar data do webhook, senão, extrai da chave como plano B
        data_evento = _parse_dt(_pick(event_payload, "data_emis", "dh_emis", "dEmissao"))
        chave_webhook = _safe_text(_pick(event_payload, "nfe_chave", "chave", "chaveNFe"))

        if not data_evento and chave_webhook and len(chave_webhook) >= 6:
            try:
                ano = int(chave_webhook[2:4])
                mes = int(chave_webhook[4:6])
                data_evento = datetime(2000 + ano, mes, 15)
                logger.info(f"📅 Usando data extraída da chave: {data_evento.strftime('%d/%m/%Y')}")
            except Exception:
                pass

        if not data_evento:
            logger.warning("❌ Não foi possível determinar a data para busca")
            return False

        # Monta a chamada para buscar o bloco de dados na API
        dEmiInicial, dEmiFinal = _date_range_for_omie(data_evento)

        api_payload = {
            "nPagina": 1, 
            "nRegPorPagina": 100, 
            "cModelo": "55",
            "dEmiInicial": dEmiInicial, 
            "dEmiFinal": dEmiFinal
        }

        logger.info(f"📄 Consultando API Omie para NF-e no período de {dEmiInicial} a {dEmiFinal}...")
        
        try:
            response_data = await _omie_post_with_retry(client, OMIE_XML_URL, "ListarDocumentos", api_payload)
        except Exception as e:
            logger.error(f"❌ Erro ao consultar API Omie: {e}")
            return False

        # Procura por documentos em várias chaves possíveis da resposta
        documentos = []
        if isinstance(response_data, dict):
            for key, value in response_data.items():
                if isinstance(value, list):
                    documentos.extend(value)
        
        if not documentos:
            logger.warning("⚠️ Nenhum documento NF-e encontrado na API para o período.")
            return True  # Sucesso, pois não há o que processar

        logger.info(f"✅ Encontrados {len(documentos)} documentos. Salvando no banco...")

        # Loop para salvar CADA documento encontrado
        for doc in documentos:
            if not isinstance(doc, dict):
                continue
                
            chave_nfe = _get_value(doc, "nChave", "chaveNFe", "chave_nfe", "chave")
            if not chave_nfe:
                continue  # Pula item sem chave

            # Mapeamento para as colunas da tabela
            numero_nf = _get_value(doc, "nNumero", "numero", "numero_nfe")
            serie_nf = _get_value(doc, "cSerie", "serie")
            status_nf = _get_value(doc, "cStatus", "status", "acao")
            
            # Converte valor para numérico
            valor_nf = None
            try:
                valor_nf = float(str(doc.get("nValor", 0)).replace(",", "."))
            except (ValueError, TypeError):
                pass
            
            # Parse da data de emissão
            data_emissao_nf = _parse_dt(_get_value(doc, "dEmissao", "data_emis", "dhEmissao"))
            
            # XML e dados brutos
            xml_str = doc.get("cXml")
            json_completo_str = json.dumps(doc, ensure_ascii=False, default=str)

            # URLs (a API ListarDocumentos não retorna URLs, então salvamos como Nulo)
            danfe_url_nf = _get_value(doc, "danfe_url", "url_danfe", "nfe_danfe")
            xml_url_nf = _get_value(doc, "xml_url", "url_xml", "nfe_xml")

            # CNPJ do emitente
            cnpj_emitente = _get_value(doc, "cnpj_emitente", "empresa_cnpj", "CNPJEmit")

            try:
                await conn.execute("""
                    INSERT INTO public.omie_nfe (
                        chave_nfe, numero, serie, status, valor_total, emitida_em,
                        xml, danfe_url, xml_url, raw, cnpj_emitente, last_event_at, updated_at, recebido_em
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), now(), now())
                    ON CONFLICT (chave_nfe) DO UPDATE SET
                        numero = EXCLUDED.numero,
                        serie = EXCLUDED.serie,
                        status = EXCLUDED.status,
                        valor_total = EXCLUDED.valor_total,
                        emitida_em = EXCLUDED.emitida_em,
                        xml = EXCLUDED.xml,
                        danfe_url = COALESCE(EXCLUDED.danfe_url, omie_nfe.danfe_url),
                        xml_url = COALESCE(EXCLUDED.xml_url, omie_nfe.xml_url),
                        raw = EXCLUDED.raw,
                        cnpj_emitente = COALESCE(EXCLUDED.cnpj_emitente, omie_nfe.cnpj_emitente),
                        updated_at = now();
                """, chave_nfe, numero_nf, serie_nf, status_nf, valor_nf, data_emissao_nf,
                     xml_str, danfe_url_nf, xml_url_nf, json_completo_str, cnpj_emitente)

                logger.info(f"✔️ NF-e {chave_nfe} (Número: {numero_nf}) salva/atualizada.")

            except Exception as e:
                logger.error(f"❌ Erro ao salvar NF-e {chave_nfe} no banco: {e}")
                continue

        return True

    except Exception as e:
        logger.error(f"❌ Erro fatal ao processar NF-e: {e}", exc_info=True)
        return False

# ------------------------------------------------------------------------------
# Rate Limiting Corrigido
# ------------------------------------------------------------------------------
async def _omie_post_with_retry(client: httpx.AsyncClient, url: str, call: str, payload: Dict[str, Any], max_retries: int = 2) -> Dict[str, Any]:
    """
    Faz requisições para API Omie com rate limiting inteligente
    """
    global LAST_API_CALL_TIME
    
    for attempt in range(max_retries):
        try:
            # Rate limiting - espera entre requisições
            current_time = time.time()
            elapsed = current_time - LAST_API_CALL_TIME
            if elapsed < OMIE_RATE_LIMIT_DELAY:
                wait_time = OMIE_RATE_LIMIT_DELAY - elapsed
                await asyncio.sleep(wait_time)
            
            LAST_API_CALL_TIME = time.time()
            
            body = _build_omie_body(call, payload)
            logger.info(f"📤 Request to {url} (attempt {attempt + 1}/{max_retries})")
            
            res = await client.post(url, json=body, timeout=OMIE_TIMEOUT_SECONDS)
            
            logger.info(f"📥 Response status: {res.status_code}")
            
            # Trata erro 425 (consumo indevido)
            if res.status_code == 425:
                error_data = res.json()
                retry_after = _extract_retry_time(error_data.get("faultstring", ""))
                
                logger.warning(f"⏳ API bloqueada. Retentando em {retry_after} segundos...")
                await asyncio.sleep(retry_after)
                continue
                
            res.raise_for_status()
            return res.json()
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 425 and attempt < max_retries - 1:
                error_data = e.response.json()
                retry_after = _extract_retry_time(error_data.get("faultstring", ""))
                
                logger.warning(f"⏳ API bloqueada (425). Tentativa {attempt + 1}/{max_retries}")
                await asyncio.sleep(retry_after)
                continue
            else:
                error_detail = f"HTTP error {e.response.status_code}: {e.response.text}"
                logger.error(f"❌ HTTP error details: {error_detail}")
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Erro na tentativa {attempt + 1}: {e}")  # ✅ CORRIGIDO
                await asyncio.sleep(10 * (attempt + 1))  # Backoff
                continue
            else:
                logger.error(f"❌ Unexpected error in _omie_post: {e}")
                raise
    
    raise Exception("Todas as tentativas falharam")