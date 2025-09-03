# pull_orders.py

import os
import json
import logging
import httpx
from datetime import datetime, timedelta, timezone

OMIE_BASE = "https://api.omie.com.br"
OMIE_PEDIDO_PATH = "/api/v1/produtos/pedido/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def pull_new_orders(last_run_time: datetime):
    """Puxa pedidos da Omie criados desde a última execução."""
    app_key = os.environ.get("OMIE_APP_KEY")
    app_secret = os.environ.get("OMIE_APP_SECRET")
    
    if not app_key or not app_secret:
        logging.error("Credenciais da Omie não configuradas.")
        return

    # Formata a data para o filtro da API
    data_inicio_str = last_run_time.strftime("%d/%m/%Y")
    data_fim_str = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    payload = {
        "call": "ListarPedidos",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [
            {
                "pagina": 1,
                "registros_por_pagina": 50,
                "filtro_data_inclusao_de": data_inicio_str,
                "filtro_data_inclusao_ate": data_fim_str
            }
        ]
    }
    
    async with httpx.AsyncClient(base_url=OMIE_BASE, timeout=60.0) as client:
        try:
            r = await client.post(OMIE_PEDIDO_PATH, json=payload)
            r.raise_for_status()
            
            response_data = r.json()
            # Esta parte do script só loga a resposta por enquanto.
            # A lógica para salvar no banco de dados será adicionada no Passo 2.
            logging.info(f"Sucesso ao puxar {len(response_data.get('cadastros', []))} pedidos.")
            
        except httpx.HTTPError as exc:
            logging.error(f"Erro HTTP na chamada para Omie: {exc}")

if __name__ == "__main__":
    # Define o tempo da última execução. Exemplo: 1 hora atrás.
    last_run_utc = datetime.now(timezone.utc) - timedelta(hours=1)
    
    import asyncio
    asyncio.run(pull_new_orders(last_run_utc))