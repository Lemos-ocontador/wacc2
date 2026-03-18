# ==========================================================================
# Extração restante: Annual --force (3 setores) + Quarterly --force (6 setores)
# Atualizado 18/mar/2026 — baseado na auditoria de qualidade
# ==========================================================================

# --- FASE 1: Annual --force (setores incompletos) ---
# Consumer Cyclical: apenas 4% feito, Technology e Industrials: 0%
$annual_pending = @(
    'Consumer Cyclical',
    'Technology',
    'Industrials'
)

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "FASE 1: ANNUAL --force ($($annual_pending.Count) setores pendentes)" -ForegroundColor Cyan
Write-Host "  Aplica correcoes: SUBUNIT, CURRENCY_MAP, ADR, Sanity Check" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

foreach ($sector in $annual_pending) {
    Write-Host "`n>>> [$sector] Fetch annual --force..." -ForegroundColor Yellow
    python scripts/fetch_historical_financials.py --sector $sector --force --workers 3 --max-rps 2.0
    if ($LASTEXITCODE -ne 0) {
        Write-Host ">>> [$sector] ERRO no fetch annual (exit code $LASTEXITCODE)" -ForegroundColor Red
    }
    
    Write-Host ">>> [$sector] Recalculando TTM..." -ForegroundColor Green
    python scripts/calculate_ttm.py --sector $sector
    
    Write-Host ">>> [$sector] CONCLUIDO" -ForegroundColor Green
}

# --- FASE 2: Quarterly --force (6 setores que so tem quarterly legado) ---
$quarterly_force = @(
    'Utilities',
    'Financial Services',
    'Energy',
    'Communication Services',
    'Real Estate',
    'Consumer Defensive'
)

Write-Host "`n============================================" -ForegroundColor Magenta
Write-Host "FASE 2: QUARTERLY --force ($($quarterly_force.Count) setores)" -ForegroundColor Magenta
Write-Host "  Re-extrai quarterly com correcoes MCap" -ForegroundColor Magenta
Write-Host "============================================" -ForegroundColor Magenta

foreach ($sector in $quarterly_force) {
    Write-Host "`n>>> [$sector] Fetch quarterly --force..." -ForegroundColor Yellow
    python scripts/fetch_historical_financials.py --sector $sector --quarterly --force --workers 3 --max-rps 2.0
    if ($LASTEXITCODE -ne 0) {
        Write-Host ">>> [$sector] ERRO no fetch quarterly (exit code $LASTEXITCODE)" -ForegroundColor Red
    }
    
    Write-Host ">>> [$sector] Recalculando TTM..." -ForegroundColor Green
    python scripts/calculate_ttm.py --sector $sector
    
    Write-Host ">>> [$sector] CONCLUIDO" -ForegroundColor Green
}

Write-Host "`n============================================" -ForegroundColor Green
Write-Host "TODAS AS FASES CONCLUIDAS!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
