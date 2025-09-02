import os
import requests
import json

# URL base da API da Omie
OMIE_API_URL = "https://app.omie.com.br/api/v1"

# Suas credenciais da Omie (lidas das variáveis de ambiente)
omie_app_key = os.environ.get("OMIE_APP_KEY")
omie_app_secret = os.environ.get("OMIE_APP_SECRET")

# Funções para importar dados
def importar_dados(call, endpoint, params):
    print(f"Executando a chamada: {call}...")
    
    payload = {
        "call": call,
        "app_key": omie_app_key,
        "app_secret": omie_app_secret,
        "param": [params]
    }

    try:
        response = requests.post(endpoint, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"Erro HTTP: {err}")
        return None
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return None

if __name__ == "__main__":
    if not all([omie_app_key, omie_app_secret]):
        print("ERRO: OMIE_APP_KEY ou OMIE_APP_SECRET não encontradas nas variáveis de ambiente.")
    else:
        # Puxa os pedidos de venda (ListarPedidos)
        pedidos_endpoint = f"{OMIE_API_URL}/produtos/pedidovenda/"
        pedidos_params = {
            "filtrar_por_data_de": "01/08/2025",
            "filtrar_por_data_ate": "31/08/2025",
        }
        pedidos_retorno = importar_dados("ListarPedidos", pedidos_endpoint, pedidos_params)
        print("--- Retorno de ListarPedidos ---")
        print(pedidos_retorno)
        
        print("\n" + "="*50 + "\n")

        # Puxa as notas fiscais (ListarDocumentos)
        notas_endpoint = f"{OMIE_API_URL}/contador/xml/"
        notas_params = {
            "cModelo": "55",
            "dEmiInicial": "01/08/2025",
            "dEmiFinal": "31/08/2025"
        }
        notas_retorno = importar_dados("ListarDocumentos", notas_endpoint, notas_params)
        print("--- Retorno de ListarDocumentos ---")
        print(notas_retorno)