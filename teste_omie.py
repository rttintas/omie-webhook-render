import requests, os

url = "https://app.omie.com.br/api/v1/produtos/pedido/"

payload = {
    "call": "ListarPedidos",
    "app_key": os.getenv("OMIE_APP_KEY"),
    "app_secret": os.getenv("OMIE_APP_SECRET"),
    "param": [{
        "pagina": 1,
        "registros_por_pagina": 1
    }]
}

r = requests.post(url, json=payload)
print("Status:", r.status_code)
print("Texto:", r.text)
