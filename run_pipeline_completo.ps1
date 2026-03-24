# ==========================================================================
# Pipeline completo: Quarterly fetch → TTM → Ratios → FX
# Executa em sequência (cada passo depende do anterior)
# ==========================================================================

$ErrorActionPreference = "Continue"
$startTime = Get-Date

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "PASSO 1/4: FETCH HISTORICOS TRIMESTRAIS" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

python scripts/fetch_historical_financials.py --quarterly --workers 5 --max-rps 4
$step1Exit = $LASTEXITCODE
$step1Time = Get-Date

Write-Host "`n============================================" -ForegroundColor Green
Write-Host "PASSO 1 CONCLUIDO (exit: $step1Exit) em $([math]::Round(($step1Time - $startTime).TotalMinutes, 1)) min" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "PASSO 2/4: CALCULO TTM" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

python scripts/calculate_ttm.py
$step2Exit = $LASTEXITCODE
$step2Time = Get-Date

Write-Host "`n============================================" -ForegroundColor Green
Write-Host "PASSO 2 CONCLUIDO (exit: $step2Exit) em $([math]::Round(($step2Time - $step1Time).TotalMinutes, 1)) min" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "PASSO 3/4: RECALCULAR RATIOS" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

python scripts/recalculate_ratios.py
$step3Exit = $LASTEXITCODE
$step3Time = Get-Date

Write-Host "`n============================================" -ForegroundColor Green
Write-Host "PASSO 3 CONCLUIDO (exit: $step3Exit) em $([math]::Round(($step3Time - $step2Time).TotalMinutes, 1)) min" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "PASSO 4/4: RECALCULAR FX RATES" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

python scripts/recalculate_fx_rates.py
$step4Exit = $LASTEXITCODE
$step4Time = Get-Date

Write-Host "`n============================================" -ForegroundColor Green
Write-Host "PASSO 4 CONCLUIDO (exit: $step4Exit) em $([math]::Round(($step4Time - $step3Time).TotalMinutes, 1)) min" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green

$totalMin = [math]::Round(($step4Time - $startTime).TotalMinutes, 1)
Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "PIPELINE COMPLETO em $totalMin minutos" -ForegroundColor Yellow
Write-Host "  Passo 1 (Quarterly): exit $step1Exit" -ForegroundColor White
Write-Host "  Passo 2 (TTM):       exit $step2Exit" -ForegroundColor White
Write-Host "  Passo 3 (Ratios):    exit $step3Exit" -ForegroundColor White
Write-Host "  Passo 4 (FX):        exit $step4Exit" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Yellow
