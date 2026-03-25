<#
.SYNOPSIS
    Rotina periódica de atualização completa da base de dados.
    
.DESCRIPTION
    Pipeline de 7 passos para manter a base atualizada:
    
    ROTINA MENSAL (recomendada):
      Passo 1: Verificar novos tickers (Damodaran + ETFs)
      Passo 2: Importar Excel Damodaran se houve atualização
      Passo 3: Sincronizar company_basic_data
      Passo 4: Normalizar yahoo_codes
      Passo 5: Atualizar dados básicos Yahoo (about, sector, mcap)
      Passo 6: Buscar dados históricos (annual + quarterly)
      Passo 7: Recalcular TTM, ratios e FX 
    
    ROTINA ANUAL (janeiro):
      Todos os passos acima + download de novo Excel Damodaran
    
.PARAMETER Mode
    monthly = Atualiza incrementalmente (novas empresas + refresh)
    annual  = Download novo Excel + atualização completa
    report  = Apenas gera relatório de cobertura
    
.EXAMPLE
    .\run_periodic_update.ps1 -Mode monthly
    .\run_periodic_update.ps1 -Mode annual
    .\run_periodic_update.ps1 -Mode report
#>

param(
    [ValidateSet("monthly", "annual", "report")]
    [string]$Mode = "monthly"
)

$ErrorActionPreference = "Stop"
$venv = ".venv\Scripts\python.exe"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = "cache\periodic_update_${timestamp}.log"

function Log($msg) {
    $line = "[$(Get-Date -Format 'HH:mm:ss')] $msg"
    Write-Host $line
    Add-Content -Path $logFile -Value $line
}

function RunStep($step, $desc, $cmd) {
    Log "=== PASSO $step : $desc ==="
    Log "CMD: $cmd"
    $startTime = Get-Date
    
    try {
        Invoke-Expression "$venv $cmd" 2>&1 | Tee-Object -Variable output
        $output | ForEach-Object { Add-Content -Path $logFile -Value $_ }
        $elapsed = (Get-Date) - $startTime
        Log "OK em $([math]::Round($elapsed.TotalMinutes, 1)) minutos"
    }
    catch {
        Log "ERRO no passo $step : $_"
        Log "Continuando..."
    }
}

# --------------------------------------------------------------------------

Log "=============================================="
Log "ROTINA PERIÓDICA DE ATUALIZAÇÃO - MODO: $Mode"
Log "=============================================="

if ($Mode -eq "report") {
    RunStep 0 "RELATÓRIO DE COBERTURA" "scripts/discover_new_tickers.py --report"
    Log "Relatório gerado."
    exit 0
}

# --------------------------------------------------------------------------
# PASSO 1: Descoberta de novos tickers
# --------------------------------------------------------------------------
RunStep 1 "DESCOBERTA E NOVOS TICKERS" "scripts/discover_new_tickers.py --source all --save cache/discovery_${timestamp}.json"

# --------------------------------------------------------------------------
# PASSO 2: Download + Import Damodaran (só no modo annual)
# --------------------------------------------------------------------------
if ($Mode -eq "annual") {
    RunStep "2a" "DOWNLOAD EXCEL DAMODARAN" "scripts/extract_global_damodaran.py"
    RunStep "2b" "IMPORT CAMPOS COMPLETOS" "scripts/import_excel_full_fields.py --update-existing"
}

# --------------------------------------------------------------------------
# PASSO 3: Sync company_basic_data (se houve novos no Damodaran)
# --------------------------------------------------------------------------
if ($Mode -eq "annual") {
    Log "=== PASSO 3: SYNC (pulado no monthly - risco de recriar IDs) ==="
    # CUIDADO: sync_company_basic_data recria a tabela e muda os IDs!
    # Só executar no annual com backup prévio. No monthly, pular.
    Log "ATENÇÃO: sync_company_basic_data.py recria IDs. Executar manualmente com backup."
    Log "  Comando: python scripts/sync_company_basic_data.py"
} else {
    Log "=== PASSO 3: SYNC (PULADO no monthly) ==="
}

# --------------------------------------------------------------------------
# PASSO 4: Normalizar yahoo_codes
# --------------------------------------------------------------------------
RunStep 4 "NORMALIZAR YAHOO CODES" "scripts/normalize_company_yahoo_codes.py"

# --------------------------------------------------------------------------
# PASSO 5: Atualizar dados básicos do Yahoo (about, sector, mcap)
# --------------------------------------------------------------------------
RunStep 5 "ATUALIZAR DADOS YAHOO BÁSICOS" "scripts/update_company_data_from_yahoo_fast.py --workers 4 --max-rps 3"

# --------------------------------------------------------------------------
# PASSO 6a: Buscar dados históricos annual (apenas novos)
# --------------------------------------------------------------------------
RunStep "6a" "FETCH HISTÓRICO ANNUAL" "scripts/fetch_historical_financials.py --workers 3 --max-rps 2"

# --------------------------------------------------------------------------
# PASSO 6b: Buscar dados históricos quarterly (apenas novos)
# --------------------------------------------------------------------------
RunStep "6b" "FETCH HISTÓRICO QUARTERLY" "scripts/fetch_historical_financials.py --quarterly --workers 3 --max-rps 2"

# --------------------------------------------------------------------------
# PASSO 7a: Recalcular TTM
# --------------------------------------------------------------------------
RunStep "7a" "RECALCULAR TTM" "scripts/calculate_ttm.py"

# --------------------------------------------------------------------------
# PASSO 7b: Recalcular ratios
# --------------------------------------------------------------------------
RunStep "7b" "RECALCULAR RATIOS" "scripts/recalculate_ratios.py"

# --------------------------------------------------------------------------
# PASSO 7c: Recalcular FX rates
# --------------------------------------------------------------------------
RunStep "7c" "RECALCULAR FX RATES" "scripts/recalculate_fx_rates.py"

# --------------------------------------------------------------------------
# PASSO 8: Relatório final
# --------------------------------------------------------------------------
RunStep 8 "RELATÓRIO DE COBERTURA FINAL" "scripts/discover_new_tickers.py --report"

Log ""
Log "=============================================="
Log "ATUALIZAÇÃO COMPLETA!"
Log "Log salvo em: $logFile"
Log "=============================================="
