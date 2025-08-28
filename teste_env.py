# teste_env.py
import os
from dotenv import load_dotenv

# carrega .env do diretório config
load_dotenv(r"G:\Meu Drive\AutomacoesOmie\config\.env")

vars_ = ["DB_HOST","DB_PORT","DB_NAME","DB_USER","DB_SCHEMA"]
print({k: os.getenv(k) for k in vars_})
