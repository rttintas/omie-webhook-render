import os, json, requests
from dotenv import load_dotenv

# carrega .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

APP_KEY    = os.getenv('OMIE_APP_KEY')
APP_SECRET = os.getenv('OMIE_APP_SECRET')

def call_omie(call, endpoint, params):
    url = f'https://app.omie.com.br/api/v1/{endpoint}'
    payload = {"call": call, "app_key": APP_KEY, "app_secret": APP_SECRET, "param": [params]}
    r = requests.post(url, json=payload, timeout=30)
    print("STATUS:", r.status_code)
    try:
        print(json.dumps(r.json(), ensure_ascii=False, indent=2))
    except Exception:
        print(r.text)

# Exemplo seguro: clientes (lista a 1ª página, 1 registro)
call_omie(
    call="ListarClientes",
    endpoint="geral/clientes/",
    params={"pagina": 1, "registros_por_pagina": 1}
)
