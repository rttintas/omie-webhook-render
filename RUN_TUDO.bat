@echo off
setlocal ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

REM ====== AJUSTE RÁPIDO AQUI SE QUISER VARREDURA MAIOR ======
set PAGES=2
REM Para varrer tudo do endpoint, troque para:  set PAGES=ALL
REM ==========================================================

cd /d "G:\Meu Drive\AutomacoesOmie\scripts"

echo ============================================
echo  INICIANDO PIPELINE OMIE (STAGING -> DOMINIO)
echo  Data/Hora: %DATE% %TIME%
echo  PAGES=%PAGES%
echo ============================================
echo.

REM 0) Garantir staging básico (se ainda nao tiver rodado antes)
python init_schema.py

REM 0.1) Garantir tabelas de dominio
python init_domain_schema.py

IF ERRORLEVEL 1 (
  echo [ERRO] Falha ao criar tabelas de dominio. Verifique credenciais PG.
  goto :fim
)

echo.
echo === 1) COLETA PARA STAGING ===
python sync_staging_omie.py clientes --paginas %PAGES%
python sync_staging_omie.py produtos --paginas %PAGES%
python sync_staging_omie.py pedidos  --paginas %PAGES%
python sync_staging_omie.py nfe      --paginas %PAGES%

IF ERRORLEVEL 1 (
  echo [ERRO] Falha na coleta para staging.
  goto :fim
)

echo.
echo === 2) PROMOCAO PARA TABELAS DE DOMINIO (UPSERT) ===
python promote_from_staging.py clientes
python promote_from_staging.py produtos
python promote_from_staging.py pedidos
python promote_from_staging.py nfe

IF ERRORLEVEL 1 (
  echo [ERRO] Falha na promocao para dominio.
  goto :fim
)

echo.
echo ============================================
echo  OK! PIPELINE CONCLUIDO COM SUCESSO.
echo  Data/Hora: %DATE% %TIME%
echo ============================================
echo.
goto :eof

:fim
echo.
echo ============== PIPELINE ENCERRADO ==============
echo.
endlocal
