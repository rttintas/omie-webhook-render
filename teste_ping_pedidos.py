import os, requests, json

URL  = "https://app.omie.com.br/api/v1/produtos/pedido/"
CALL = "ListarPedidos"

body = {
  "call": CALL,
  "app_key": os.getenv("OMIE_APP_KEY"),
  "app_secret": os.getenv("OMIE_APP_SECRET"),
  "param": [{"pagina":1, "registros_por_pagina":1, "apenas_importado_api":"N"}]
}

r = requests.post(URL, json=body, timeout=30)
print("STATUS:", r.status_code)
print("TEXTO :", r.text[:500])
