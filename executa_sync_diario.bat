@echo off
cd /d "G:\Meu Drive\AutomacoesOmie\scripts"
call .venv\Scripts\activate.bat

rem === variáveis de ambiente (ajuste uma vez aqui) ===
set OMIE_APP_KEY=SEU_APP_KEY
set OMIE_APP_SECRET=SEU_APP_SECRET
set PG_DSN=host=localhost port=5432 dbname=omie_dw user=postgres password=sua_senha

rem === 1) gerar o ids.csv do dia anterior ===
python -u gerar_ids_csv.py

rem === 2) rodar o downloader de XML/PDF ===
python -u sync_nfe_dfedocs.py --csv ids.csv --debug

rem (opcional) registrar logs com data no nome:
rem python -u gerar_ids_csv.py        >> logs\%date:~6,4%-%date:~3,2%-%date:~0,2%.log 2>&1
rem python -u sync_nfe_dfedocs.py --csv ids.csv --debug >> logs\%date:~6,4%-%date:~3,2%-%date:~0,2%.log 2>&1
