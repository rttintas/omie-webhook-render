@echo off
setlocal enableextensions enabledelayedexpansion

REM =========================================================
REM 1) Ir para a pasta dos scripts
REM =========================================================
cd /d "G:\Meu Drive\AutomacoesOmie\scripts"

REM =========================================================
REM 2) Ativar o venv do Python
REM =========================================================
call .venv\Scripts\activate.bat

REM =========================================================
REM 3) Credenciais / conexões
REM    >>> PREENCHA SEUS VALORES <<<
REM =========================================================
set OMIE_APP_KEY=SEU_APP_KEY
set OMIE_APP_SECRET=SEU_APP_SECRET

REM Ex.: host=localhost port=5432 dbname=omie_dw user=postgres password=minha_senha
set PG_DSN=host=localhost port=5432 dbname=omie_dw user=postgres password=SUA_SENHA

REM =========================================================
REM 4) Preparar pasta de logs e nome do arquivo (data-hora)
REM =========================================================
if not exist logs mkdir logs
for /f "tokens=1-3 delims=/" %%a in ("%date%") do set _d=%%c-%%a-%%b
for /f "tokens=1 delims=." %%a in ("%time%") do set _t=%%a
set _t=%_t::=-%
set LOG=logs\run_%_d%_%_t%.log

echo ==== INICIO %date% %time% ==== > "%LOG%"

REM =========================================================
REM 5) GERAR a lista de IDs (por padrão, notas do dia anterior)
REM    - isso cria/atualiza o arquivo ids.csv na pasta atual
REM    (Se quiser forçar uma data: python -u gerar_ids_csv.py --dia 2025-08-26)
REM =========================================================
python -u gerar_ids_csv.py 1>>"%LOG%" 2>&1
if errorlevel 1 goto :fail

REM =========================================================
REM 6) BAIXAR/GRAVAR NF-e (XML/PDF) a partir do ids.csv
REM    --debug mantém os logs ricos; remova se não quiser
REM =========================================================
python -u sync_nfe_dfedocs.py --csv ids.csv --debug 1>>"%LOG%" 2>&1
if errorlevel 1 goto :fail

echo ==== SUCESSO %date% %time% ==== >> "%LOG%"
goto :end

:fail
echo ==== FALHA %date% %time% ==== >> "%LOG%"
type "%LOG%"
exit /b 1

:end
type "%LOG%"
exit /b 0
