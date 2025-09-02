@echo off
setlocal ENABLEDELAYEDEXPANSION

rem ================== CONFIGS ==================
set PY="C:\Program Files\Python310\python.exe"
set SCRIPT="G:\Meu Drive\AutomacoesOmie\Arquivos API Omie\load_omie_batch.py"
rem ============================================

echo ==================================================
echo Iniciando carga Omie -  %date% %time:~0,8%
echo [Python ] %PY%
echo [Script ] %SCRIPT%
echo ==================================================
echo.

%PY% %SCRIPT%

echo.
echo ==================================================
echo Encerrado em %date% %time:~0,8%
echo ==================================================
echo.
pause
