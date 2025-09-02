@echo off
setlocal enabledelayedexpansion

REM ==================================================
REM Configurações
set PYTHON_EXE="C:\Program Files\Python310\python.exe"
set SCRIPT="G:\Meu Drive\AutomacoesOmie\Arquivos API Omie\load_omie_batch.py"
set LOGDIR="G:\Meu Drive\AutomacoesOmie\Arquivos API Omie\logs"
if not exist %LOGDIR% mkdir %LOGDIR%
set LOGFILE=%LOGDIR%\omie_diag_%date:~6,4%-%date:~3,2%-%date:~0,2%_%time:~0,2%h%time:~3,2%m.log
REM ==================================================

echo =================================================
echo [Python ] %PYTHON_EXE%
echo [Script ] %SCRIPT%
echo [Log    ] %LOGFILE%
echo =================================================
echo.

echo Iniciando carga Omie - %date% %time%
echo =================================================

echo [CHECK] Versao declarada no .py:
type %SCRIPT% | find "version"
echo =================================================

REM Executa o script com log
%PYTHON_EXE% %SCRIPT% >> %LOGFILE% 2>&1

echo.
echo =================================================
echo Encerrado em %date% %time%
echo Log salvo em: %LOGFILE%
echo =================================================

pause
