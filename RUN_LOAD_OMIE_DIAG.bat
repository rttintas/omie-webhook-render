@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ======================================================
REM Ambiente
REM ======================================================
chcp 65001 >nul
set PYTHONUTF8=1

REM Caminhos (sem aspas aqui; usaremos aspas só na hora de chamar)
set PYTHON_EXE=C:\Program Files\Python310\python.exe
set SCRIPT=G:\Meu Drive\AutomacoesOmie\Arquivos API Omie\load_omie_batch.py

REM ====== DEFINA SUA CONEXAO POSTGRES AQUI (use essa linha inteira entre aspas) ======
set "DATABASE_URL=postgresql://neondb_owner:npg_X56oHDTNPtkz@ep-late-heart-acmovnu4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
REM =====================================================================

echo ===============================================================
echo [Python ] "%PYTHON_EXE%"
echo [Script ] "%SCRIPT%"
echo [DB URL ] !DATABASE_URL:~0,60!...   (prefixo)
echo ===============================================================

REM >>> Verifica dentro do Python se a var chegou
"%PYTHON_EXE%" -c "import os; v=os.environ.get('DATABASE_URL') or ''; print('PY SEES DATABASE_URL:', v[:120])"
echo ---------------------------------------------------------------

REM >>> Mostra versao do script
for /f "usebackq delims=" %%V in (`"%PYTHON_EXE%" "%SCRIPT%" --version`) do set VER=%%V
if not defined VER set VER=(sem versao)
echo Versao   !VER!
echo ===============================================================

echo.
echo Iniciando carga Omie…

REM >>> Executa com debug e datas (ajuste as datas se quiser) e grava log
"%PYTHON_EXE%" "%SCRIPT%" --debug --desde 2025-08-01 --ate 2025-08-31 > diag.log 2>&1
set EXITCODE=%ERRORLEVEL%

echo.
echo ===============================================================
echo Encerrado em %date% %time% (exit code %EXITCODE%)
echo ===============================================================

REM Abre o log no bloco de notas e mantém janela aberta
notepad diag.log
pause
endlocal
exit /b %EXITCODE%
