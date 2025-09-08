async def _buscar_links_xml_via_api(client: httpx.AsyncClient, chave: str, data_emis: Optional[datetime]) -> Tuple[Optional[str], Optional[str]]:
    """
    Consulta a API Contador/XML → ListarDocumentos com paginação até encontrar a NF-e específica
    """
    # Extrai mês/ano da chave NFe (formato: ANO+MES+...)
    if not data_emis and chave and len(chave) >= 4:
        try:
            ano = int(chave[2:4])  # 25 de 352509...
            mes = int(chave[4:6])  # 09 de 352509...
            data_emis = datetime(2000 + ano, mes, 1)
        except (ValueError, IndexError):
            pass
    
    dEmiInicial, dEmiFinal = _date_range_for_omie(data_emis)
    
    # Busca com paginação até encontrar a NF-e
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
            logger.info(f"🔎 Buscando XML para chave {chave} - Página {pagina}...")
            resp = await _omie_post(client, OMIE_XML_URL, OMIE_XML_LIST_CALL, payload)
            
            # Procura na lista de documentos (chave pode variar)
            documentos = []
            if isinstance(resp, dict):
                for key, value in resp.items():
                    if isinstance(value, list) and any('chave' in str(item) for item in value):
                        documentos.extend(value)
                        break
            
            # Função auxiliar para extrair valores
            def _get_value(doc, *possible_keys):
                if not isinstance(doc, dict):
                    return None
                for key in possible_keys:
                    if key in doc and doc[key] not in (None, "", 0):
                        return str(doc[key]).strip()
                return None
            
            # Busca específica pela chave exata
            for documento in documentos:
                chave_doc = _get_value(documento, "chave", "chaveNFe", "nfe_chave", "cChaveNFe", "nChave")
                
                if chave_doc and chave_doc == chave:
                    xml_url = _get_value(documento, "xml_url", "url_xml", "nfe_xml", "link_xml")
                    danfe_url = _get_value(documento, "danfe_url", "url_danfe", "nfe_danfe", "link_danfe")
                    
                    if xml_url:
                        logger.info(f"✅ NF-e {chave} encontrada na página {pagina}")
                        return xml_url, danfe_url
            
            # Verifica se há mais páginas
            total_registros = _get_value(resp, "nTotRegistros", "total_registros", "nTotalRegistros")
            if total_registros and pagina * registros_por_pagina >= int(total_registros):
                break
                
            pagina += 1
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar na página {pagina}: {e}")
            break
    
    logger.warning(f"⚠️ NF-e {chave} não encontrada nas {pagina-1} páginas")
    return None, None

async def processar_nfe(conn: asyncpg.Connection, payload: Dict[str, Any], client: httpx.AsyncClient) -> bool:
    """
    Processa uma NF-e específica com tratamento robusto do XML
    """
    try:
        event = payload.get("event", {}) or {}
        
        chave = _safe_text(_pick(event, "nfe_chave", "chave", "chaveNFe", "chave_nfe"))
        numero_nf = _safe_text(_pick(event, "id_nf", "numero", "numero_nfe", "nNumero"))
        serie = _safe_text(_pick(event, "serie", "cSerie"))
        data_emis = _parse_dt(_pick(event, "data_emis", "dh_emis", "dhEmissao", "dEmissao"))
        xml_url = _safe_text(_pick(event, "nfe_xml", "xml_url", "url_xml", "link_xml"))
        danfe_url = _safe_text(_pick(event, "nfe_danfe", "danfe_url", "url_danfe", "link_danfe"))
        cnpj_emit = _safe_text(_pick(event, "empresa_cnpj", "cnpj_emitente", "CNPJEmit"))
        status = _safe_text(_pick(event, "acao", "status", "cStatus"))

        logger.info(f"🔍 Processando NF-e: Chave={chave}, Número={numero_nf}")

        if not chave:
            logger.warning("❌ NF-e sem chave")
            return False

        if not xml_url:
            logger.warning("⚠️  NF-e sem URL do XML — tentando buscar via Contador/XML …")
            xml_url_api, danfe_url_api = await _buscar_links_xml_via_api(client, chave, data_emis)
            xml_url = xml_url or xml_url_api
            danfe_url = danfe_url or danfe_url_api

        if not xml_url:
            logger.warning("❌ NF-e segue sem URL do XML — não será possível salvar.")
            return False

        try:
            # Baixa e limpa o XML
            r = await client.get(xml_url, timeout=OMIE_TIMEOUT_SECONDS)
            r.raise_for_status()
            
            # Limpeza básica do XML (remove caracteres problemáticos)
            xml_text = r.text
            xml_text = xml_text.replace('\r', '').replace('\n', ' ').strip()
            xml_text = ' '.join(xml_text.split())  # Remove espaços múltiplos
            
            xml_base64 = base64.b64encode(r.content).decode("utf-8")
            logger.info(f"✅ XML baixado e limpo para NF-e {chave}")
            
        except Exception as e:
            logger.error(f"❌ Erro ao baixar/limpar XML: {e}")
            return False

        # Salva no banco (mesmo código anterior)
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
        """, chave, numero_nf, serie, data_emis, cnpj_emit, status, xml_text, xml_url, danfe_url,
        json.dumps(payload, ensure_ascii=False, default=str))

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

        logger.info(f"✅ NF-e {chave} salva no banco")
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao processar NF-e: {e}")
        return False