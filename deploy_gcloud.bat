@echo off
REM ============================================
REM  Deploy do WACC Calculator no Google Cloud
REM  Projeto: dataanloc
REM ============================================

echo.
echo ========================================
echo  Deploy - WACC Calculator (dataanloc)
echo ========================================
echo.

REM Verificar se gcloud está instalado
where gcloud >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] gcloud CLI nao encontrado. Instale em: https://cloud.google.com/sdk/docs/install
    pause
    exit /b 1
)

REM Verificar se o banco de dados existe
if not exist "data\damodaran_data_new.db" (
    echo [ERRO] Banco de dados nao encontrado em data\damodaran_data_new.db
    echo Copie o banco antes de fazer deploy.
    pause
    exit /b 1
)

REM Mostrar projeto atual
echo Projeto GCloud configurado:
gcloud config get-value project
echo.

REM Confirmar projeto
set /p CONFIRM="Deseja fazer deploy no projeto 'dataanloc'? (S/N): "
if /i not "%CONFIRM%"=="S" (
    echo Deploy cancelado.
    pause
    exit /b 0
)

REM Setar o projeto
echo.
echo [1/4] Configurando projeto dataanloc...
gcloud config set project dataanloc

REM Swap requirements para GAE (sem selenium/curl_cffi)
echo.
echo [2/4] Preparando requirements para GAE...
copy requirements.txt requirements-local-backup.txt >nul
copy requirements-gae.txt requirements.txt >nul

REM Deploy
echo.
echo [3/4] Iniciando deploy no App Engine...
echo Isso pode levar alguns minutos...
echo.
gcloud app deploy app.yaml --quiet

REM Restaurar requirements original
copy requirements-local-backup.txt requirements.txt >nul
del requirements-local-backup.txt >nul 2>&1

if %errorlevel% neq 0 (
    echo.
    echo [ERRO] Falha no deploy. Verifique os erros acima.
    pause
    exit /b 1
)

REM Abrir no navegador
echo.
echo [4/4] Deploy concluido com sucesso!
echo.
echo Abrindo aplicacao no navegador...
gcloud app browse

pause
