# Metodologias de Cálculo — Dados Financeiros Históricos

> Referência técnica dos cálculos realizados pelo script `scripts/fetch_historical_financials.py`
> e armazenados na tabela `company_financials_historical`.

---

## 1. Fontes de Dados

| Fonte | Dados Obtidos | API |
|-------|--------------|-----|
| **Yahoo Finance (yfinance)** — `ticker.income_stmt` | DRE: Receita, EBIT, EBITDA, Lucro Líquido, etc. | Anual: até 5 períodos |
| **Yahoo Finance (yfinance)** — `ticker.cash_flow` | Fluxo de Caixa: FCF, Operacional, Capex | Anual: até 5 períodos |
| **Yahoo Finance (yfinance)** — `ticker.balance_sheet` | Balanço: Dívida Total, Patrimônio, Ativos, Caixa | Anual: até 5 períodos |
| **Yahoo Finance (yfinance)** — `ticker.get_info()` | Shares Outstanding, moeda financeira, EV atual | Valor corrente |
| **Yahoo Finance (yfinance)** — `ticker.history()` | Preços históricos de fechamento (diário, 10 anos) | Série temporal |
| **Yahoo Finance (yfinance)** — `{MOEDA}USD=X` | Taxa de câmbio histórica para USD (diário, 10 anos) | Série temporal |
| **Damodaran** — tabela `damodaran_global` | Market Cap histórico (dez/2014—2023), EV, múltiplos | Cross-validation |

---

## 2. Enterprise Value (EV) — Estimativa Histórica

### 2.1 Fórmula

$$EV = \text{Market Cap Estimado} + \text{Dívida Total} + \text{Preferred Stock} + \text{Minority Interest} - \text{Caixa e Equivalentes}$$

Quando componentes opcionais não estão disponíveis, são tratados como zero:

$$EV = \text{MCap} + \text{Dívida Total} + \text{Preferred}_{(se\ disponível)} + \text{Minority}_{(se\ disponível)} - \text{Caixa}_{(se\ disponível)}$$

### 2.2 Market Cap Estimado

$$\text{Market Cap} = P_{\text{close}}(t) \times \text{Ordinary Shares Number}(t)$$

Onde:
- $P_{\text{close}}(t)$ = preço de fechamento **mais próximo** da data do período fiscal $t$
- **Ordinary Shares Number(t)** = número de ações ordinárias em circulação no período $t$, extraído do Balance Sheet (`Ordinary Shares Number`). Se não disponível, utiliza `sharesOutstanding` corrente via `ticker.get_info()`

#### Obtenção do Preço Histórico

1. Busca-se a série de preços diários dos últimos 10 anos: `ticker.history(period="10y", interval="1d")`
2. Para cada data de período fiscal (ex: `2024-09-30`), localiza-se o preço mais próximo usando `pd.Index.get_indexer(method="nearest")`
3. **Tratamento de timezone**: o índice retornado pelo yfinance é timezone-aware (ex: `America/New_York`). A data do período é localizada para o mesmo timezone antes da comparação

#### Limitações Conhecidas

| Limitação | Impacto | Mitigação |
|-----------|---------|-----------|
| Shares Outstanding histórico pode não estar disponível em alguns balanços | Nesses casos, usa-se o `sharesOutstanding` corrente via `get_info()` | Para empresas com base acionária estável, o impacto é mínimo. Splits são ajustados automaticamente pelo yfinance nos preços históricos |
| yfinance retorna até 5 períodos anuais, mas o 5° pode ter dados incompletos | FY mais antigo pode ter todas as métricas como N/A | O campo `data_quality` pode ser usado para filtrar |

### 2.3 Componentes do Balanço

| Campo | Origem yfinance | Campo no BD |
|-------|----------------|-------------|
| Dívida Total | `balance_sheet["Total Debt"]` | `total_debt` |
| Dívida Curto Prazo | `balance_sheet["Current Debt"]` | `short_term_debt` |
| Dívida Longo Prazo | `balance_sheet["Long Term Debt"]` | `long_term_debt` |
| Caixa e Equivalentes | `balance_sheet["Cash And Cash Equivalents"]` | `cash_and_equivalents` |
| Patrimônio Líquido | `balance_sheet["Stockholders Equity"]` | `stockholders_equity` |
| Ações Ordinárias | `balance_sheet["Ordinary Shares Number"]` | `ordinary_shares_number` |
| Preferred Stock | `balance_sheet["Preferred Stock"]` | `preferred_stock` |
| Minority Interest | `balance_sheet["Minority Interest"]` | `minority_interest` |
| Ativos Totais | `balance_sheet["Total Assets"]` | `total_assets` |
| Passivos Totais | `balance_sheet["Total Liabilities Net Minority Interest"]` | `total_liabilities` |
| Investimentos CP | `balance_sheet["Other Short Term Investments"]` | `short_term_investments` |
| Ativo Circulante | `balance_sheet["Current Assets"]` | `current_assets` |
| Passivo Circulante | `balance_sheet["Current Liabilities"]` | `current_liabilities` |

### 2.4 Validação Cruzada

Para Apple Inc (AAPL), FY2025:

| Métrica | Yahoo (calculado) | Damodaran | Desvio |
|---------|------------------|-----------|--------|
| EV/EBITDA | 27.80x | 27.90x | **0.3%** |
| EV/Revenue | 9.67x | 9.84x | 1.7% |
| EV | $4.02T | $4.09T | 1.7% |

---

## 3. Demonstrações Financeiras — Income Statement

| Campo | Origem yfinance | Campo no BD | Descrição |
|-------|----------------|-------------|-----------|
| Receita Total | `Total Revenue` | `total_revenue` | Receita bruta total |
| Custo da Receita | `Cost Of Revenue` | `cost_of_revenue` | CPV |
| Lucro Bruto | `Gross Profit` | `gross_profit` | Receita - CPV |
| Receita Operacional | `Operating Income` | `operating_income` | Lucro operacional |
| Despesas Operacionais | `Operating Expense` | `operating_expense` | Total de despesas operacionais |
| EBIT | `EBIT` | `ebit` | Lucro antes de juros e impostos |
| EBITDA | `EBITDA` | `ebitda` | Lucro antes de juros, impostos, depreciação e amortização |
| EBITDA Normalizado | `Normalized EBITDA` | `normalized_ebitda` | EBITDA ajustado por itens não recorrentes |
| Lucro Líquido | `Net Income` | `net_income` | Resultado final |
| Despesas de Juros | `Interest Expense` | `interest_expense` | Custo da dívida |
| Provisão p/ IR | `Tax Provision` | `tax_provision` | Imposto de renda provisionado |
| P&D | `Research And Development` | `research_and_development` | Gastos com pesquisa e desenvolvimento |
| SG&A | `Selling General And Administration` | `sga` | Despesas com vendas, gerais e administrativas |
| Média Diluída Ações | `Diluted Average Shares` | `diluted_average_shares` | Média ponderada de ações diluídas no período |

---

## 4. Fluxo de Caixa

| Campo | Origem yfinance | Campo no BD | Descrição |
|-------|----------------|-------------|-----------|
| Free Cash Flow | `Free Cash Flow` | `free_cash_flow` | Fluxo de caixa livre |
| Caixa Operacional | `Operating Cash Flow` | `operating_cash_flow` | Fluxo de caixa das operações |
| Capex | `Capital Expenditure` | `capital_expenditure` | Investimentos em ativos fixos (valor negativo) |

**Nota:** O Free Cash Flow reportado pelo Yahoo Finance é: $FCF = \text{Operating Cash Flow} + \text{Capital Expenditure}$ (Capex é negativo).

---

## 5. Margens e Indicadores de Rentabilidade

### 5.1 Margens sobre Receita

| Indicador | Fórmula | Campo no BD |
|-----------|---------|-------------|
| Margem EBIT | $\frac{\text{EBIT}}{\text{Receita Total}}$ | `ebit_margin` |
| Margem EBITDA | $\frac{\text{EBITDA}}{\text{Receita Total}}$ | `ebitda_margin` |
| Margem Bruta | $\frac{\text{Lucro Bruto}}{\text{Receita Total}}$ | `gross_margin` |
| Margem Líquida | $\frac{\text{Lucro Líquido}}{\text{Receita Total}}$ | `net_margin` |

### 5.2 Indicadores de Conversão de Caixa

| Indicador | Fórmula | Campo no BD | Interpretação |
|-----------|---------|-------------|---------------|
| FCF/Receita | $\frac{\text{FCF}}{\text{Receita Total}}$ | `fcf_revenue_ratio` | Quanto da receita vira caixa livre |
| FCF/EBITDA | $\frac{\text{FCF}}{\text{EBITDA}}$ | `fcf_ebitda_ratio` | Eficiência de conversão do EBITDA em caixa |
| Capex/Receita | $\frac{|\text{Capex}|}{\text{Receita Total}}$ | `capex_revenue` | Intensidade de capital |

### 5.3 Indicadores de Alavancagem

| Indicador | Fórmula | Campo no BD | Interpretação |
|-----------|---------|-------------|---------------|
| Dívida/PL | $\frac{\text{Dívida Total}}{\text{Patrimônio Líquido}}$ | `debt_equity` | Alavancagem financeira |
| Dívida/EBITDA | $\frac{\text{Dívida Total}}{\text{EBITDA}}$ | `debt_ebitda` | Capacidade de pagamento (em anos de EBITDA) |

### 5.4 Múltiplos de Avaliação

| Indicador | Fórmula | Campo no BD | Interpretação |
|-----------|---------|-------------|---------------|
| EV/Receita | $\frac{\text{EV Estimado}}{\text{Receita Total}}$ | `ev_revenue` | Múltiplo de receita |
| EV/EBITDA | $\frac{\text{EV Estimado}}{\text{EBITDA}}$ | `ev_ebitda` | Múltiplo de EBITDA (mais usado para valuation) |

**Nota:** Os múltiplos usam o Enterprise Value **estimado** do período (não o corrente), permitindo análise temporal de valuation.

---

## 6. Conversão Cambial (FX)

### 6.1 Metodologia

1. A moeda original dos demonstrativos financeiros é obtida via `ticker.get_info()["financialCurrency"]`
2. A série histórica de câmbio é buscada via ticker auxiliar `{MOEDA}USD=X` (ex: `BRLUSD=X`) usando `ticker.history(period="10y", interval="1d")`
3. Para cada período fiscal, a taxa mais próxima da data do balanço é utilizada (método `nearest`)
4. A série FX é **cacheada** por moeda (thread-safe) durante a execução

### 6.2 Campos Convertidos

| Campo Original (moeda local) | Campo USD |
|-----------------------------|-----------|
| `total_revenue` | `total_revenue_usd` |
| `ebit` | `ebit_usd` |
| `ebitda` | `ebitda_usd` |
| `net_income` | `net_income_usd` |
| `free_cash_flow` | `free_cash_flow_usd` |
| `enterprise_value_estimated` | `enterprise_value_usd` |

### 6.3 Limitações

| Limitação | Impacto |
|-----------|---------|
| Série FX limitada a ~10 anos; períodos anteriores usam a taxa mais antiga disponível | Para empresas com histórico >10 anos, períodos muito antigos podem usar taxa de ~10 anos atrás |
| Para moedas sem cotação no Yahoo Finance, assume-se taxa = 1.0 | Algumas moedas exóticas podem não converter corretamente |
| `market_cap_estimated` é em moeda local (mesma do preço da ação) | Para ações em BRL, `market_cap_estimated` estará em BRL; use `enterprise_value_usd` para comparações cross-country |

---

## 7. Estrutura de Dados

### 7.1 Tabela `company_financials_historical`

- **Chave primária**: `id` (autoincrement)
- **Restrição de unicidade**: `UNIQUE(company_basic_data_id, period_type, period_date)` — garante um registro por empresa/tipo/data
- **`period_type`**: `'annual'` ou `'quarterly'`
- **`period_date`**: data de encerramento do período fiscal (formato ISO: `YYYY-MM-DD`)
- **`fiscal_year`**: ano fiscal extraído da data do período
- **`fiscal_quarter`**: trimestre (1-4) para dados trimestrais, NULL para anuais

### 7.2 Índices

| Índice | Colunas | Finalidade |
|--------|---------|-----------|
| `idx_cfh_yahoo` | `yahoo_code` | Busca rápida por ticker |
| `idx_cfh_period` | `period_type, fiscal_year` | Filtros por tipo e ano |
| `idx_cfh_company` | `company_basic_data_id` | Join com `company_basic_data` |

### 7.3 Relacionamentos

```
company_basic_data (1) ──── (N) company_financials_historical
    id                          company_basic_data_id
    yahoo_code                  yahoo_code
    company_name                company_name
    currency                    original_currency
```

---

## 8. Qualidade dos Dados

### 8.1 Cobertura Temporal

O yfinance retorna tipicamente:
- **4-5 períodos anuais** (últimos ~5 anos fiscais)
- **4-5 períodos trimestrais** (últimos ~16 meses)

O período mais antigo pode ter dados incompletos (todos N/A), especialmente para empresas menores.

### 8.2 Classificação de Qualidade

O campo `data_quality` indica a completude dos dados:
- `ok` — dados com métricas principais disponíveis

### 8.3 Validação

Métricas de validação cruzada com Damodaran (exemplo AAPL FY2025):

```
Receita:    Yahoo $416.2B  ≈  Damodaran $416.2B  (match)
EBITDA:     Yahoo $144.7B  ≈  Damodaran $144.7B  (match)
EV:         Yahoo $4.02T   ≈  Damodaran $4.09T   (desvio 1.7%)
EV/EBITDA:  Yahoo 27.80x   ≈  Damodaran 27.90x   (desvio 0.3%)
```

O pequeno desvio no EV se deve a:
1. Diferença na data exata de cotação usada para o Market Cap
2. Shares Outstanding podem diferir ligeiramente entre fontes
3. Damodaran pode incluir minority interests e preferred equity na fórmula do EV

---

## 9. Exemplo Completo — Cálculo do EV

### Apple Inc (AAPL) — FY2025 (encerramento 30/set/2025)

```
1. Preço de fechamento em ~30/set/2025:     $269.86
2. Shares Outstanding (atual):              14,671,100,000
3. Market Cap estimado:                     $269.86 × 14.67B = $3,959B ($3.96T)
4. Dívida Total (Balance Sheet):            $98.7B
5. Caixa e Equivalentes (Balance Sheet):    $35.9B
6. Enterprise Value:                        $3,959B + $98.7B - $35.9B = $4,022B ($4.02T)
```

### Vale S.A. (VALE3.SA) — FY2024 (encerramento 31/dez/2024)

```
1. Preço de fechamento B3 em ~31/dez/2024:   R$49.92
2. Shares Outstanding (atual):               ~4.09B
3. Market Cap estimado (BRL):                R$49.92 × 4.09B = R$204.3B
4. Dívida Total (Balance Sheet):             R$17.7B
5. Caixa e Equivalentes (Balance Sheet):     R$5.0B
6. Enterprise Value (BRL):                   R$204.3B + R$17.7B - R$5.0B = R$217.0B
7. Taxa BRL/USD (corrente):                  0.190
8. Enterprise Value (USD):                   R$217.0B × 0.190 = $41.2B
```

---

## 10. Resumo das Fórmulas

| Métrica | Fórmula |
|---------|---------|
| **Market Cap (est.)** | $P_{\text{close}} \times \text{OrdinarySharesNumber}$ |
| **Enterprise Value** | $\text{MCap} + \text{Dívida Total} + \text{Preferred Stock} + \text{Minority Interest} - \text{Caixa}$ |
| **Margem EBIT** | $\frac{\text{EBIT}}{\text{Receita}}$ |
| **Margem EBITDA** | $\frac{\text{EBITDA}}{\text{Receita}}$ |
| **Margem Bruta** | $\frac{\text{Lucro Bruto}}{\text{Receita}}$ |
| **Margem Líquida** | $\frac{\text{Lucro Líquido}}{\text{Receita}}$ |
| **FCF/Receita** | $\frac{\text{FCF}}{\text{Receita}}$ |
| **FCF/EBITDA** | $\frac{\text{FCF}}{\text{EBITDA}}$ |
| **Capex/Receita** | $\frac{|\text{Capex}|}{\text{Receita}}$ |
| **Dívida/PL** | $\frac{\text{Dívida Total}}{\text{PL}}$ |
| **Dívida/EBITDA** | $\frac{\text{Dívida Total}}{\text{EBITDA}}$ |
| **EV/Receita** | $\frac{\text{EV}}{\text{Receita}}$ |
| **EV/EBITDA** | $\frac{\text{EV}}{\text{EBITDA}}$ |
| **Valor USD** | $\text{Valor Local} \times \text{FX Rate}(t)$ |

---

*Documento gerado em julho/2025. Script: `scripts/fetch_historical_financials.py`*
