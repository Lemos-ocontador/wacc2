# Script para reprocessar todos os setores com novos campos
# Ordem: do menor ao maior (Financial Services e Utilities já atualizados)
# Executar a partir da pasta raiz do projeto com venv ativado

$ErrorActionPreference = "Continue"
$startTime = Get-Date

$sectors = @(
    "Energy",              # 1043 emp
    "Communication Services", # 1665 emp
    "Real Estate",         # 2064 emp
    "Consumer Defensive",  # 2473 emp
    "Healthcare",          # 3905 emp
    "Basic Materials",     # 5058 emp
    "Consumer Cyclical",   # 5102 emp
    "Technology",          # 5490 emp
    "Industrials"          # 7294 emp
)

$totalSectors = $sectors.Count
$completed = 0

foreach ($sector in $sectors) {
    $completed++
    $sectorStart = Get-Date
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "[$completed/$totalSectors] Processando: $sector" -ForegroundColor Cyan
    Write-Host "Inicio: $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
    
    python scripts/fetch_historical_financials.py --sector $sector --force --workers 2 --max-rps 1.5
    
    $exitCode = $LASTEXITCODE
    $sectorDuration = (Get-Date) - $sectorStart
    
    if ($exitCode -eq 0) {
        Write-Host "OK: $sector concluido em $([math]::Round($sectorDuration.TotalMinutes, 1)) min" -ForegroundColor Green
    } else {
        Write-Host "ERRO: $sector falhou (exit code: $exitCode) apos $([math]::Round($sectorDuration.TotalMinutes, 1)) min" -ForegroundColor Red
    }
}

$totalDuration = (Get-Date) - $startTime
Write-Host ""
Write-Host "============================================================" -ForegroundColor Yellow
Write-Host "TODOS OS SETORES CONCLUIDOS" -ForegroundColor Yellow
Write-Host "Tempo total: $([math]::Round($totalDuration.TotalHours, 1)) horas" -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Yellow
