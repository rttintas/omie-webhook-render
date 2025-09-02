@echo off
setlocal ENABLEDELAYEDEXPANSION

REM ======== CONFIGURAR CAMINHOS ========
set PYTHON_EXE="C:\Program Files\Python310\python.exe"
set SCRIPT_PY="G:\Meu Drive\AutomacoesOmie\Arquivos API Omie\load_omie_batch.py"

REM (opcional) logs simples em arquivo .log na mesma pasta:
for /f "tokens=1-3 delims=/ " %%a in ("%date%") do set D=%%c-%%b-%%a
for /f "tokens=1-3 delims=:." %%a in ("%time%") do set T=%%a%%b%%c
set LOGFILE="G:\Meu Drive\AutomacoesOmie\Arquivos API Omie\omie_run_%D%_%T%.log"

echo ================================================ > %LOGFILE%
echo [Python ] %PYTHON_EXE%                          >> %LOGFILE%
echo [Script ] %SCRIPT_PY%                            >> %LOGFILE%
echo ================================================ >> %LOGFILE%

echo ================================================
echo Iniciando carga Omie - %date% %time%
echo ================================================

echo.>> %LOGFILE%
echo Iniciando carga Omie - %date% %time% >> %LOGFILE%

REM Executa e duplica saida
%PYTHON_EXE% %SCRIPT_PY% 1>>%LOGFILE% 2>&1
set EXITCODE=%ERRORLEVEL%

echo.>> %LOGFILE%
echo ================================================ >> %LOGFILE%
echo Encerrado em %date% %time% (exit code %EXITCODE%) >> %LOGFILE%
echo ================================================ >> %LOGFILE%

echo.
echo ================================================
echo Encerrado em %date% %time% (exit code %EXITCODE%)
echo Log salvo em: %LOGFILE%
echo ================================================
echo.

pause
exit /b %EXITCODE%
