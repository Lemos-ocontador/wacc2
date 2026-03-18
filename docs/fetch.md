# Extração de Dados Financeiros Históricos

> Documentação completa do pipeline de extração quarterly + cálculo TTM.
> Atualizado: 2026-03-17

---

## 1. Visão Geral do Pipeline

Cada setor passa por **3 etapas** sequenciais:

```
1. fetch_historical_financials.py --quarterly  →  Extrai dados trimestrais do Yahoo Finance
2. calculate_ttm.py                            →  Calcula TTM (soma 4 quarters) + múltiplos
3. Validação                                   →  Confere contagens no banco
```

## 2. Scripts e Parâmetros

### 2.1 fetch_historical_financials.py

Extrai dados do Yahoo Finance (annual ou quarterly) e grava no SQLite.

```bash
python scripts/fetch_historical_financials.py \
  --sector "SETOR" \
  --quarterly \
  --workers 3 \
  --max-rps 2.0
```

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `--sector` | — | Nome exato do setor Yahoo (case-sensitive) |
| `--quarterly` | false | Busca trimestral (sem flag = anual) |
| `--workers` | 3 | Threads paralelas de extração |
| `--max-rps` | 2.0 | Rate limit: requests por segundo |
| `--force` | false | Re-extrai mesmo se já existir |
| `--limit` | — | Limita N empresas (para testes) |
| `--company` | — | Yahoo code específico |

**Comportamento inteligente:** Sem `--force`, pula empresas que já têm dados no `period_type` solicitado.

**Rate limiting:** O script detecta HTTP 429 automaticamente, pausa 1-3 minutos, limpa cookies e retoma.

### 2.2 calculate_ttm.py

Calcula TTM (Trailing Twelve Months) somando os 4 quarters mais recentes e recalcula múltiplos.

```bash
python scripts/calculate_ttm.py --sector "SETOR"
```

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `--sector` | — | Setor para calcular TTM |
| `--company` | — | Yahoo code específico |

**Campos TTM calculados:** `total_revenue_ttm`, `ebitda_ttm`, `net_income_ttm`, `operating_income_ttm`, `free_cash_flow_ttm`, `operating_cash_flow_ttm`

**Múltiplos recalculados:** EV/Revenue, EV/EBITDA, EV/EBIT, P/E, etc. — usando TTM quando disponível.

**Guards:** Revenue mínimo $100K USD, múltiplos limitados a ±500x, `ttm_quarters_count` deve ser ≥ 4.

## 3. Configuração Ótima de Performance

### 3.1 Parâmetros recomendados

| Tamanho do setor | Workers | Max RPS | Motivo |
|-----------------|---------|---------|--------|
| < 1.000 empresas | 3 | 2.0 | Default seguro |
| 1.000 - 3.000 | 3 | 2.0 | Equilibra velocidade vs rate limit |
| 3.000 - 6.000 | 4 | 2.5 | Aumenta throughput, Yahoo tolera |
| > 6.000 | 4 | 2.5 | Mesma config, batch automático cuida |

### 3.2 Velocidade observada
- **~1.5 empresa/segundo** (efetivo, incluindo empresas sem dados)
- **~0.7s por empresa** com dados (extração + parse + gravação)
- Rate limits são raros com 3 workers / 2 RPS

### 3.3 Dicas de otimização
- **Não usar `--force`** para quarterly — o script pula empresas já extraídas
- **Workers > 5 causa rate limit** frequente no Yahoo Finance
- **max-rps > 3.0** causa mais pausas que ganho de velocidade
- O batch size interno = workers × 5, então 3 workers = batches de 15

## 4. Ordem de Extração (Menor → Maior)

Status em 2026-03-17:

| # | Setor | Empresas | Est. Tempo | Status |
|---|-------|----------|------------|--------|
| ✅ | Utilities | 766 | — | CONCLUÍDO (quarterly + TTM) |
| 1 | Financial Services | 616 | ~7 min | PENDENTE |
| 2 | Energy | 1.127 | ~13 min | PENDENTE |
| 3 | Communication Services | 1.759 | ~20 min | PENDENTE |
| 4 | Real Estate | 2.189 | ~24 min | PENDENTE |
| 5 | Consumer Defensive | 2.636 | ~29 min | PENDENTE |
| 6 | Healthcare | 4.075 | ~45 min | PENDENTE |
| 7 | Basic Materials | 5.221 | ~58 min | PENDENTE |
| 8 | Consumer Cyclical | 5.314 | ~59 min | PENDENTE |
| 9 | Technology | 5.680 | ~63 min | PENDENTE |
| 10 | Industrials | 7.643 | ~85 min | PENDENTE |

**Tempo total estimado: ~6-7 horas** (considerando pausas por rate limit)

## 5. Comandos de Execução Completos

### Sequência setor por setor (copiar e colar):

```powershell
# ===== 1. Financial Services (616 emp, ~7 min) =====
python scripts/fetch_historical_financials.py --sector "Financial Services" --quarterly --workers 3 --max-rps 2.0
python scripts/calculate_ttm.py --sector "Financial Services"

# ===== 2. Energy (1.127 emp, ~13 min) =====
python scripts/fetch_historical_financials.py --sector "Energy" --quarterly --workers 3 --max-rps 2.0
python scripts/calculate_ttm.py --sector "Energy"

# ===== 3. Communication Services (1.759 emp, ~20 min) =====
python scripts/fetch_historical_financials.py --sector "Communication Services" --quarterly --workers 3 --max-rps 2.0
python scripts/calculate_ttm.py --sector "Communication Services"

# ===== 4. Real Estate (2.189 emp, ~24 min) =====
python scripts/fetch_historical_financials.py --sector "Real Estate" --quarterly --workers 3 --max-rps 2.0
python scripts/calculate_ttm.py --sector "Real Estate"

# ===== 5. Consumer Defensive (2.636 emp, ~29 min) =====
python scripts/fetch_historical_financials.py --sector "Consumer Defensive" --quarterly --workers 3 --max-rps 2.0
python scripts/calculate_ttm.py --sector "Consumer Defensive"

# ===== 6. Healthcare (4.075 emp, ~45 min) =====
python scripts/fetch_historical_financials.py --sector "Healthcare" --quarterly --workers 4 --max-rps 2.5
python scripts/calculate_ttm.py --sector "Healthcare"

# ===== 7. Basic Materials (5.221 emp, ~58 min) =====
python scripts/fetch_historical_financials.py --sector "Basic Materials" --quarterly --workers 4 --max-rps 2.5
python scripts/calculate_ttm.py --sector "Basic Materials"

# ===== 8. Consumer Cyclical (5.314 emp, ~59 min) =====
python scripts/fetch_historical_financials.py --sector "Consumer Cyclical" --quarterly --workers 4 --max-rps 2.5
python scripts/calculate_ttm.py --sector "Consumer Cyclical"

# ===== 9. Technology (5.680 emp, ~63 min) =====
python scripts/fetch_historical_financials.py --sector "Technology" --quarterly --workers 4 --max-rps 2.5
python scripts/calculate_ttm.py --sector "Technology"

# ===== 10. Industrials (7.643 emp, ~85 min) =====
python scripts/fetch_historical_financials.py --sector "Industrials" --quarterly --workers 4 --max-rps 2.5
python scripts/calculate_ttm.py --sector "Industrials"
```

### Script automático sequencial (todos de uma vez):

```powershell
# Rodar tudo sequencialmente — recomendado deixar rodando overnight
$sectors = @(
    "Financial Services",
    "Energy",
    "Communication Services",
    "Real Estate",
    "Consumer Defensive",
    "Healthcare",
    "Basic Materials",
    "Consumer Cyclical",
    "Technology",
    "Industrials"
)

foreach ($sector in $sectors) {
    Write-Host "`n========== $sector ==========" -ForegroundColor Cyan
    Write-Host "$(Get-Date -Format 'HH:mm:ss') - Iniciando quarterly..." -ForegroundColor Yellow

    $workers = if (($sectors.IndexOf($sector)) -ge 5) { 4 } else { 3 }
    $rps = if (($sectors.IndexOf($sector)) -ge 5) { 2.5 } else { 2.0 }

    python scripts/fetch_historical_financials.py --sector $sector --quarterly --workers $workers --max-rps $rps

    Write-Host "$(Get-Date -Format 'HH:mm:ss') - Calculando TTM..." -ForegroundColor Yellow
    python scripts/calculate_ttm.py --sector $sector

    Write-Host "$(Get-Date -Format 'HH:mm:ss') - $sector CONCLUÍDO" -ForegroundColor Green
}

Write-Host "`n===== TODOS OS SETORES CONCLUÍDOS =====" -ForegroundColor Green
```

## 6. Validação Pós-Extração

### Query de verificação rápida:

```python
python -c "
import sqlite3
conn = sqlite3.connect('data/damodaran_data_new.db')
print(f\"{'Setor':<35} {'Quarterly':>10} {'TTM':>8}\")
for r in conn.execute('''
    SELECT cbd.yahoo_sector, 
           SUM(CASE WHEN cfh.period_type='quarterly' THEN 1 ELSE 0 END) as q,
           SUM(CASE WHEN cfh.total_revenue_ttm IS NOT NULL THEN 1 ELSE 0 END) as t
    FROM company_financials_historical cfh
    JOIN company_basic_data cbd ON cbd.id=cfh.company_basic_data_id
    GROUP BY cbd.yahoo_sector ORDER BY q
''').fetchall():
    print(f'{r[0]:<35} {r[1]:>10} {r[2]:>8}')
conn.close()
"
```

## 7. Banco de Dados

- **Arquivo:** `data/damodaran_data_new.db`
- **Tabela:** `company_financials_historical` (69 colunas)
- **Unique constraint:** `(company_basic_data_id, period_type, period_date)`
- **Tamanho atual:** ~235 MB (vai crescer ~2-3x com quarterly de todos os setores)

## 8. Troubleshooting

| Problema | Causa | Solução |
|----------|-------|---------|
| Rate limit frequente | Workers/RPS muito altos | Reduzir para 2 workers / 1.5 RPS |
| "database is locked" | Outro processo escrevendo | Fechar outros scripts, não rodar em paralelo |
| Empresa sem dados quarterly | Yahoo não tem dados | Normal, ~25% das empresas |
| TTM = NULL | < 4 quarters disponíveis | Normal para empresas recentes |
| Script trava | Conexão instável | Ctrl+C e re-rodar (pula já extraídas) |

## 9. Pós-Extração Completa

Após todos os setores:

```powershell
# 1. Verificar cobertura
python scripts/validate_data_consistency.py

# 2. Re-deploy com banco atualizado
# (seguir deploy_gcloud.bat ou docs/gcloud.md)

# 3. Commit
git add . && git commit -m "data: quarterly + TTM para todos os setores" && git push
```
