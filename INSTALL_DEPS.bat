@echo off
setlocal
chcp 65001 >nul

set PY="C:\Program Files\Python310\python.exe"

echo Atualizando pip...
%PY% -m pip install --upgrade pip

echo Instalando dependencias (psycopg v3 + requests)...
%PY% -m pip install "psycopg[binary]" requests

echo.
echo OK! Dependencias instaladas.
%PY% -c "import psycopg,requests; print('psycopg', psycopg.__version__); print('requests', requests.__version__)"
endlocal
pause
