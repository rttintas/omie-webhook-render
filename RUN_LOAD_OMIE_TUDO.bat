@echo off
setlocal EnableExtensions EnableDelayedExpansion

chcp 65001 >nul
set PYTHONUTF8=1

REM Caminho do Python e Script
set PYTHON_EXE=C:\Program Files\Python310\python.exe
set SCRIPT=G:\Meu Drive\AutomacoesOmie\Arquivos API Omie\load_omie_batch.py

REM Banco de Dados (DATABASE_URL) - já pronto!
set "DATABASE_URL=postgresql://neondb_owner:npg_X56oHDTNPtkz@ep-late-heart-acmovnu4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

echo ===============================================================
echo [Python ] "%PYTHON_EXE%"
echo [Script ] "%SCRIPT%"
echo [DB URL ] !DATABASE_URL:~0,70!... (prefixo)
echo ===============================================================

echo.
echo ================== PEDIDOS ==================
"%PYTHON_EXE%" "%SCRIPT%" --debug > pedidos.log 2>&1
echo Fim da carga de pedidos. Abrindo pedidos.log...
notepad pedidos.log

echo.
echo ================== NF-e ==================
"%PYTHON_EXE%" "%SCRIPT%" --debug --nfe --nfe-desde 2025-08-01 --nfe-ate 2025-08-31 > nfe.log 2>&1
echo Fim da carga de NF-e. Abrindo nfe.log...
notepad nfe.log

echo.
echo ===============================================================
echo Execução concluída em %date% %time%
echo ===============================================================

pause
endlocal
exit /b 0
