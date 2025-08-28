import os, requests, json

url = "https://app.omie.com.br/api/v1/produtos/dfedocs/"
body = {
    "call": "ObterNfe",
    "app_key": os.environ["OMIE_APP_KEY"],
    "app_secret": os.environ["OMIE_APP_SECRET"],
    "param": [
        {"nChaveNfe": "35250806241162000106550010000095041899293824"}
    ]
}

r = requests.post(url, json=body, timeout=60)
print("HTTP:", r.status_code)
print(r.text)
