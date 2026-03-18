# Metodologias de Cálculo — Dados Financeiros Históricos

> Referência técnica dos cálculos realizados pelos scripts
> `scripts/fetch_historical_financials.py` e `scripts/calculate_ttm.py`,
> armazenados na tabela `company_financials_historical`.
>
> **Legenda de fontes:**
> - 🟣 **Raw Yahoo** — dado extraído diretamente do Yahoo Finance via yfinance (sem transformação)
> - 🟠 **Calculado** — derivado pela aplicação a partir dos dados raw
> - 🟢 **Damodaran** — dado da tabela Damodaran (cross-validation)

---

## 1. Fontes de Dados

| Fonte | Dados Obtidos | API | Tipo |
|-------|--------------|-----|------|
| **Yahoo Finance** — `ticker.income_stmt` / `ticker.quarterly_income_stmt` | DRE: Receita, EBIT, EBITDA, Lucro Líquido, etc. | Anual: até 5 períodos; Trimestral: até 5 trimestres | 🟣 Raw |
| **Yahoo Finance** — `ticker.cash_flow` / `ticker.quarterly_cash_flow` | Fluxo de Caixa: FCF, Operacional, Capex | Anual: até 5 períodos; Trimestral: até 5 trimestres | 🟣 Raw |
| **Yahoo Finance** — `ticker.balance_sheet` / `ticker.quarterly_balance_sheet` | Balanço: Dívida Total, Patrimônio, Ativos, Caixa | Anual: até 5 períodos; Trimestral: até 5 trimestres | 🟣 Raw |
| **Yahoo Finance** — `ticker.get_info()` | Shares Outstanding (fallback), moedas | Valor corrente (snapshot) | 🟣 Raw |
| **Yahoo Finance** — `ticker.history()` | Preços históricos de fechamento (diário, 10 anos) | Série temporal | 🟣 Raw |
| **Yahoo Finance** — `{MOEDA}USD=X` | Taxa de câmbio histórica para USD (diário, 10 anos) | Série temporal | 🟣 Raw |
| **Damodaran** — tabela `damodaran_global` | Market Cap histórico (dez/2014—2023), EV, múltiplos | Cross-validation | 🟢 Damodaran |

### 1.1 O que o Yahoo Finance fornece como TTM (snapshot)

O `ticker.get_info()` retorna alguns campos que representam os **últimos 12 meses correntes**:

| Campo `get_info()` | Descrição | Uso na aplicação |
|---|---|---|
| `totalRevenue` | Receita TTM corrente | **Não usado** — usamos o histórico dos statements |
| `ebitda` | EBITDA TTM corrente | **Não usado** |
| `freeCashflow` | FCF TTM corrente | **Não usado** |
| `netIncomeToCommon` | Lucro Líquido TTM corrente | **Não usado** |
| `trailingPE` | P/E trailing corrente | **Não usado** |
| `trailingEps` | EPS trailing corrente | **Não usado** |
| `enterpriseToEbitda` | EV/EBITDA corrente | **Não usado** (cross-check pontual) |
| `enterpriseToRevenue` | EV/Revenue corrente | **Não usado** (cross-check pontual) |

**Por que não usamos esses valores?** Porque são apenas um **snapshot do momento atual**.
Nossa base precisa de TTM **histórico** — ou seja, para cada trimestre passado (Q3/2024, Q2/2024, Q1/2024...),
qual era o acumulado dos 12 meses anteriores naquele ponto no tempo. O Yahoo **não fornece** TTM histórico
via `quarterly_income_stmt` — esses demonstrativos contêm apenas os valores do trimestre individual.
Portanto, o TTM histórico é **calculado pela aplicação** somando os 4 trimestres mais recentes (ver Seção 7).

---

## 2. Enterprise Value (EV) — Estimativa Histórica 🟠

### 2.1 Fórmula

$$EV = \text{Market Cap Estimado} + \text{Dívida Total} + \text{Preferred Stock} + \text{Minority Interest} - \text{Caixa e Equivalentes}$$

Quando componentes opcionais não estão disponíveis, são tratados como zero:

$$EV = \text{MCap} + \text{Dívida Total} + \text{Preferred}_{(se\ disponível)} + \text{Minority}_{(se\ disponível)} - \text{Caixa}_{(se\ disponível)}$$

### 2.2 Market Cap Estimado

$$\text{Market Cap} = P_{\text{close}}(t) \times \text{Ordinary Shares Number}(t)$$

Onde:
- $P_{\text{close}}(t)$ = preço de fechamento **mais próximo** da data do período fiscal $t$ (armazenado em `close_price`)
- **Ordinary Shares Number(t)** = número de ações ordinárias em circulação no período $t$, extraído do Balance Sheet (`Ordinary Shares Number`). Se não disponível, utiliza `sharesOutstanding` corrente via `ticker.get_info()`
- Quando a moeda de negociação difere da moeda financeira, o MCap é convertido (ver seção 6.4)

#### Obtenção do Preço Histórico

1. Busca-se a série de preços diários dos últimos 10 anos: `ticker.history(period="10y", interval="1d")`
2. Para cada data de período fiscal (ex: `2024-09-30`), localiza-se o preço mais próximo usando `pd.Index.get_indexer(method="nearest")`
3. **Tratamento de timezone**: o índice retornado pelo yfinance é timezone-aware (ex: `America/New_York`). A data do período é localizada para o mesmo timezone antes da comparação

#### Limitações Conhecidas

| Limitação | Impacto | Mitigação |
|-----------|---------|-----------|
| Shares Outstanding histórico pode não estar disponível em alguns balanços | Nesses casos, usa-se o `sharesOutstanding` corrente via `get_info()` | Para empresas com base acionária estável, o impacto é mínimo. Splits são ajustados automaticamente pelo yfinance nos preços históricos |
| yfinance retorna até 5 períodos anuais, mas o 5° pode ter dados incompletos | FY mais antigo pode ter todas as métricas como N/A | O campo `data_quality` pode ser usado para filtrar |

### 2.3 Componentes do Balanço (usados no EV)

| Campo | Origem yfinance | Campo no BD | Fonte |
|-------|----------------|-------------|-------|
| Dívida Total | `balance_sheet["Total Debt"]` | `total_debt` | 🟣 Raw |
| Dívida Curto Prazo | `balance_sheet["Current Debt"]` | `short_term_debt` | 🟣 Raw |
| Dívida Longo Prazo | `balance_sheet["Long Term Debt"]` | `long_term_debt` | 🟣 Raw |
| Caixa e Equivalentes | `balance_sheet["Cash And Cash Equivalents"]` | `cash_and_equivalents` | 🟣 Raw |
| Patrimônio Líquido | `balance_sheet["Stockholders Equity"]` | `stockholders_equity` | 🟣 Raw |
| Ações Ordinárias | `balance_sheet["Ordinary Shares Number"]` | `ordinary_shares_number` | 🟣 Raw |
| Preferred Stock | `balance_sheet["Preferred Stock"]` | `preferred_stock` | 🟣 Raw |
| Minority Interest | `balance_sheet["Minority Interest"]` | `minority_interest` | 🟣 Raw |
| Ativos Totais | `balance_sheet["Total Assets"]` | `total_assets` | 🟣 Raw |
| Passivos Totais | `balance_sheet["Total Liabilities Net Minority Interest"]` | `total_liabilities` | 🟣 Raw |
| Investimentos CP | `balance_sheet["Other Short Term Investments"]` | `short_term_investments` | 🟣 Raw |
| Ativo Circulante | `balance_sheet["Current Assets"]` | `current_assets` | 🟣 Raw |
| Passivo Circulante | `balance_sheet["Current Liabilities"]` | `current_liabilities` | 🟣 Raw |
| Preço de Fechamento | `ticker.history()` → nearest date | `close_price` | 🟣 Raw |
| Market Cap Estimado | `close_price × ordinary_shares_number` | `market_cap_estimated` | 🟠 Calculado |
| Enterprise Value Estimado | `MCap + Dívida + Preferred + Minority − Caixa` | `enterprise_value_estimated` | 🟠 Calculado |

### 2.4 Validação Cruzada

Para Apple Inc (AAPL), FY2025:

| Métrica | Yahoo (calculado) | Damodaran | Desvio |
|---------|------------------|-----------|--------|
| EV/EBITDA | 27.80x | 27.90x | **0.3%** |
| EV/Revenue | 9.67x | 9.84x | 1.7% |
| EV | $4.02T | $4.09T | 1.7% |

---

## 3. Demonstrações Financeiras — Income Statement 🟣

| Campo | Origem yfinance | Campo no BD | Fonte |
|-------|----------------|-------------|-------|
| Receita Total | `Total Revenue` | `total_revenue` | 🟣 Raw |
| Custo da Receita | `Cost Of Revenue` | `cost_of_revenue` | 🟣 Raw |
| Lucro Bruto | `Gross Profit` | `gross_profit` | 🟣 Raw |
| Receita Operacional | `Operating Income` | `operating_income` | 🟣 Raw |
| Despesas Operacionais | `Operating Expense` | `operating_expense` | 🟣 Raw |
| EBIT | `EBIT` | `ebit` | 🟣 Raw |
| EBITDA | `EBITDA` | `ebitda` | 🟣 Raw |
| EBITDA Normalizado | `Normalized EBITDA` | `normalized_ebitda` | 🟣 Raw |
| Lucro Líquido | `Net Income` | `net_income` | 🟣 Raw |
| Despesas de Juros | `Interest Expense` | `interest_expense` | 🟣 Raw |
| Provisão p/ IR | `Tax Provision` | `tax_provision` | 🟣 Raw |
| P&D | `Research And Development` | `research_and_development` | 🟣 Raw |
| SG&A | `Selling General And Administration` | `sga` | 🟣 Raw |
| Média Diluída Ações | `Diluted Average Shares` | `diluted_average_shares` | 🟣 Raw |

> **Nota:** Todos os 14 campos acima são extraídos diretamente do Yahoo Finance sem nenhuma transformação.
> O Yahoo Finance calcula internamente EBIT e EBITDA a partir dos line items da DRE.

---

## 4. Fluxo de Caixa 🟣

| Campo | Origem yfinance | Campo no BD | Fonte |
|-------|----------------|-------------|-------|
| Free Cash Flow | `Free Cash Flow` | `free_cash_flow` | 🟣 Raw |
| Caixa Operacional | `Operating Cash Flow` | `operating_cash_flow` | 🟣 Raw |
| Capex | `Capital Expenditure` | `capital_expenditure` | 🟣 Raw |

**Nota:** O Free Cash Flow reportado pelo Yahoo Finance é: $FCF = \text{Operating Cash Flow} + \text{Capital Expenditure}$ (Capex é negativo).

---

## 5. Margens e Indicadores de Rentabilidade 🟠

> Todos os indicadores desta seção são **calculados pela aplicação** no `fetch_historical_financials.py`.

### 5.1 Margens sobre Receita

| Indicador | Fórmula | Campo no BD | Fonte |
|-----------|---------|-------------|-------|
| Margem EBIT | $\frac{\text{EBIT}}{\text{Receita Total}}$ | `ebit_margin` | 🟠 Calculado |
| Margem EBITDA | $\frac{\text{EBITDA}}{\text{Receita Total}}$ | `ebitda_margin` | 🟠 Calculado |
| Margem Bruta | $\frac{\text{Lucro Bruto}}{\text{Receita Total}}$ | `gross_margin` | 🟠 Calculado |
| Margem Líquida | $\frac{\text{Lucro Líquido}}{\text{Receita Total}}$ | `net_margin` | 🟠 Calculado |

### 5.2 Indicadores de Conversão de Caixa

| Indicador | Fórmula | Campo no BD | Interpretação | Fonte |
|-----------|---------|-------------|---------------|-------|
| FCF/Receita | $\frac{\text{FCF}}{\text{Receita Total}}$ | `fcf_revenue_ratio` | Quanto da receita vira caixa livre | 🟠 Calculado |
| FCF/EBITDA | $\frac{\text{FCF}}{\text{EBITDA}}$ | `fcf_ebitda_ratio` | Eficiência de conversão do EBITDA em caixa | 🟠 Calculado |
| Capex/Receita | $\frac{|\text{Capex}|}{\text{Receita Total}}$ | `capex_revenue` | Intensidade de capital | 🟠 Calculado |

### 5.3 Indicadores de Alavancagem

| Indicador | Fórmula | Campo no BD | Interpretação | Fonte |
|-----------|---------|-------------|---------------|-------|
| Dívida/PL | $\frac{\text{Dívida Total}}{\text{Patrimônio Líquido}}$ | `debt_equity` | Alavancagem financeira | 🟠 Calculado |
| Dívida/EBITDA | $\frac{\text{Dívida Total}}{\text{EBITDA}}$ | `debt_ebitda` | Capacidade de pagamento (em anos de EBITDA) | 🟠 Calculado |

### 5.4 Múltiplos de Avaliação

| Indicador | Fórmula | Campo no BD | Interpretação | Fonte |
|-----------|---------|-------------|---------------|-------|
| EV/Receita | $\frac{\text{EV Estimado}}{\text{Receita Total (ou TTM)}}$ | `ev_revenue` | Múltiplo de receita | 🟠 Calculado |
| EV/EBITDA | $\frac{\text{EV Estimado}}{\text{EBITDA (ou TTM)}}$ | `ev_ebitda` | Múltiplo de EBITDA (mais usado para valuation) | 🟠 Calculado |
| EV/EBIT | $\frac{\text{EV Estimado}}{\text{EBIT (ou TTM)}}$ | `ev_ebit` | Múltiplo de EBIT (apenas quando EBIT > 0) | 🟠 Calculado |

**Nota sobre múltiplos em registros trimestrais:** Para registros `quarterly`, os denominadores usam os valores **TTM** (soma 4Q) quando `ttm_quarters_count = 4`. Isso é feito pelo `calculate_ttm.py` (ver Seção 7.3). Para registros `annual`, usam os valores do período diretamente.

### 5.5 Guardas e Limites de Múltiplos

Para evitar distorções causadas por empresas pré-receita ou com denominadores desprezíveis, os múltiplos são submetidos a dois filtros:

#### 5.5.1 Limite absoluto (clamp)

Todo múltiplo EV com valor absoluto > **500x** é descartado (`NULL`):

```
ev_revenue = NULL se |EV/Rev| > 500
ev_ebitda  = NULL se |EV/EBITDA| > 500
ev_ebit    = NULL se |EV/EBIT| > 500
```

**Racional:** Múltiplos > 500x não representam valuation legítimo e distorcem medianas setoriais. Exemplo: IMSR com EV=$921M e Revenue=$18K produzia EV/Rev=49.492x.

#### 5.5.2 Materialidade de receita

O múltiplo `ev_revenue` só é calculado quando a receita em USD é >= **$100.000**:

$$\text{Revenue}_{\text{USD}} = |\text{Revenue}| \times \text{FX}_{\text{\u2192USD}} \geq 100.000$$

Empresas com receita inferior a esse limiar (pré-operacionais, early-stage) têm `ev_revenue = NULL`.

**Exemplos filtrados:** ELANGO.BO (Rev=$231 USD), GLOBUSCON.BO (Rev=$487 USD), FWTC.V (Rev=$5K USD).

#### 5.5.3 Materialidade de EBITDA

O `ev_ebitda` só é calculado quando o EBITDA em USD é >= **$100**:

$$|\text{EBITDA}_{\text{USD}}| \geq 100$$

Isso evita divisão por valores próximos de zero.

---

## 6. Conversão Cambial (FX) 🟠

### 6.1 Metodologia

1. A moeda original dos demonstrativos financeiros é obtida via `ticker.get_info()["financialCurrency"]` 🟣
2. A série histórica de câmbio é buscada via ticker auxiliar `{MOEDA}USD=X` (ex: `BRLUSD=X`) usando `ticker.history(period="10y", interval="1d")` 🟣
3. Para cada período fiscal, a taxa mais próxima da data do balanço é utilizada (método `nearest`)
4. A série FX é **cacheada** por moeda (thread-safe) durante a execução
5. A taxa aplicada é gravada no campo `fx_rate_to_usd` 🟣 Raw
6. Todos os campos USD abaixo são derivados: `valor_local × fx_rate_to_usd`

### 6.2 Campos Convertidos

| Campo Original (moeda local) | Campo USD | Fonte |
|-----------------------------|-----------|-------|
| `total_revenue` | `total_revenue_usd` | 🟠 Calculado |
| `ebit` | `ebit_usd` | 🟠 Calculado |
| `ebitda` | `ebitda_usd` | 🟠 Calculado |
| `net_income` | `net_income_usd` | 🟠 Calculado |
| `free_cash_flow` | `free_cash_flow_usd` | 🟠 Calculado |
| `enterprise_value_estimated` | `enterprise_value_usd` | 🟠 Calculado |

### 6.3 Limitações

| Limitação | Impacto |
|-----------|---------|
| Série FX limitada a ~10 anos; períodos anteriores usam a taxa mais antiga disponível | Para empresas com histórico >10 anos, períodos muito antigos podem usar taxa de ~10 anos atrás |
| Para moedas sem cotação no Yahoo Finance, assume-se taxa = 1.0 | Algumas moedas exóticas podem não converter corretamente |
| `market_cap_estimated` é na moeda dos demonstrativos financeiros | Quando moeda de negociação difere da moeda de relatório (ex: PGAS.JK negocia em IDR mas reporta em USD), o MCap é convertido automaticamente |

### 6.4 Conversão de Moeda no Market Cap

Quando a moeda de negociação da ação (`info["currency"]`) difere da moeda dos demonstrativos financeiros (`info["financialCurrency"]`), o Market Cap é convertido para a moeda financeira antes de compor o EV:

$$\text{MCap}_{\text{fin}} = P_{\text{close}} \times \text{Shares} \times \frac{\text{FX}_{\text{trading→USD}}}{\text{FX}_{\text{financial→USD}}}$$

Isso garante que todos os componentes do EV (MCap, Dívida, Caixa) estejam na mesma moeda.

Exemplo: PGAS.JK (Perusahaan Gas Negara)
- Preço: 1.436 IDR, Shares: 24,2B → MCap bruto = 34,8T IDR
- `financialCurrency = "USD"`, `currency = "IDR"`
- Conversão: MCap_USD = MCap_IDR × (IDR→USD) / (USD→USD) = 34,8T × 0,0000645 = ~$2,25B USD
- Com isso, EV/Revenue fica ~0,6x ao invés de 9.000x

---

## 7. Dados Trimestrais e TTM (Trailing Twelve Months) 🟠

### 7.1 Contexto: TTM do Yahoo vs TTM Calculado

O Yahoo Finance disponibiliza campos TTM **apenas como snapshot corrente** via `ticker.get_info()`:

| Campo Yahoo (`get_info()`) | Valor (AAPL, mar/2026) | Natureza |
|---|---|---|
| `totalRevenue` | $435.6B | TTM corrente (snapshot) |
| `ebitda` | $152.9B | TTM corrente (snapshot) |
| `freeCashflow` | $106.3B | TTM corrente (snapshot) |
| `trailingPE` | 32.22 | P/E trailing corrente |
| `enterpriseToEbitda` | 24.43 | EV/EBITDA corrente |

**Esses valores não são usados na nossa base**, pois representam apenas o ponto atual no tempo.
Para análise histórica (séries temporais), precisamos saber qual era o TTM em cada trimestre passado.

O `ticker.quarterly_income_stmt` retorna **valores do trimestre individual** (não acumulados 12 meses).
Exemplo AAPL:
```
quarterly_income_stmt["Total Revenue"]:
  Q4/2025: $124.3B  (apenas o trimestre)
  Q3/2025: $85.8B
  Q2/2025: $94.9B
  Q1/2025: $124.0B
  Q4/2024: $119.6B
```

Portanto, o TTM histórico é **inteiramente calculado pela aplicação** somando 4 trimestres consecutivos.

### 7.2 Extração Trimestral 🟣

A extração trimestral usa os mesmos demonstrativos do Yahoo Finance com a flag `--quarterly`:
- `ticker.quarterly_income_stmt` — até 5 trimestres 🟣 Raw
- `ticker.quarterly_cash_flow` — até 5 trimestres 🟣 Raw
- `ticker.quarterly_balance_sheet` — até 5 trimestres 🟣 Raw

Os campos extraídos são **idênticos** aos anuais (Seções 3, 4, 5), porém representam valores de um trimestre individual.

**Uso:**
```bash
python scripts/fetch_historical_financials.py --sector "Utilities" --quarterly --workers 3 --max-rps 2.0
```

### 7.3 Cálculo TTM 🟠

O TTM (Trailing Twelve Months) é calculado pelo script `scripts/calculate_ttm.py` e representa a **soma acumulada dos 4 trimestres mais recentes** até a data de referência.

**Uso:**
```bash
python scripts/calculate_ttm.py --sector "Utilities"
python scripts/calculate_ttm.py --company AAPL
```

#### Regras por Tipo de Período

| Tipo | Cálculo TTM | `ttm_quarters_count` |
|------|------------|---------------------|
| **Annual** | TTM = valor do próprio período (já são 12 meses) | 4 |
| **Quarterly** | TTM = soma dos 4 últimos trimestres disponíveis (incluindo o corrente) | 0–4 (real) |

#### Campos TTM (todos 🟠 Calculados)

| Campo Original (trimestral) | Campo TTM | Fórmula | Fonte |
|----------------------------|-----------|---------|-------|
| `total_revenue` | `total_revenue_ttm` | Σ revenue dos 4Q | 🟠 Calculado |
| `ebitda` | `ebitda_ttm` | Σ ebitda dos 4Q | 🟠 Calculado |
| `ebit` | `ebit_ttm` | Σ ebit dos 4Q | 🟠 Calculado |
| `free_cash_flow` | `free_cash_flow_ttm` | Σ fcf dos 4Q | 🟠 Calculado |
| `net_income` | `net_income_ttm` | Σ net_income dos 4Q | 🟠 Calculado |
| — | `ttm_quarters_count` | Contagem de Q com dados | 🟠 Calculado |

#### Lógica de Cálculo

1. Para cada empresa, os registros trimestrais são ordenados por `period_date`
2. Para cada trimestre `i`, toma-se a janela `[max(0, i-3) .. i]` — até 4 registros
3. Os valores não-nulos são somados; **mínimo de 2 trimestres** para gerar TTM parcial
4. Se menos de 2 trimestres têm dados, o campo TTM fica `NULL`
5. O campo `ttm_quarters_count` registra quantos trimestres de receita contribuíram

### 7.4 Recálculo de Múltiplos com TTM 🟠

Os múltiplos EV (`ev_revenue`, `ev_ebitda`, `ev_ebit`) dos registros trimestrais são **recalculados** pelo `calculate_ttm.py` usando os denominadores TTM ao invés dos valores do trimestre:

$$\text{EV/Revenue} = \frac{\text{EV Estimado}}{\text{Revenue TTM}}$$

$$\text{EV/EBITDA} = \frac{\text{EV Estimado}}{\text{EBITDA TTM}}$$

**Guardas de confiabilidade:**
1. Múltiplos só calculados quando `ttm_quarters_count >= 4`. TTM parcial (< 4Q) → múltiplos `NULL`.
2. Mesmos limites da Seção 5.5: clamp de 500x e materialidade de $100K USD para revenue TTM.

### 7.5 Limitações do TTM

| Limitação | Impacto | Exchanges Afetadas |
|-----------|---------|-------------------|
| Yahoo Finance fornece apenas ~5 trimestres recentes | Empresas sem histórico trimestral longo terão TTM parcial nos primeiros trimestres | Todas |
| Ações chinesas (`.SZ`, `.SS`) frequentemente têm apenas 5 trimestres | Para Q2/2024 (primeiro trimestre disponível), o TTM só terá 1 trimestre | ~99% de divergência quando `ttm_quarters_count < 4` |
| Ações indianas (`.NS`, `.BO`) com padrão similar | TTM parcial resulta em subestimação de ~50-75% | ~98% de divergência quando `ttm_quarters_count < 4` |
| Trimestres fiscais podem não coincidir com trimestres calendário | Empresas com FY terminando em meses não-padrão (ex: março) podem ter TTM mal-alinhado | Japão, Índia |
| Com `ttm_quarters_count = 4`, divergência residual de ~9% entre TTM trimestral e anual | Diferenças de ajuste contábil entre reports trimestrais e anuais | Todas (normal) |

### 7.6 Por que não usar o TTM do Yahoo `get_info()`?

| Aspecto | Yahoo `get_info()` TTM | Nosso TTM calculado |
|---|---|---|
| Cobertura temporal | Apenas **1 ponto** (atual) | **Até 5 pontos** trimestrais (histórico) |
| Permite série temporal? | ❌ Não | ✅ Sim |
| Permite análise de tendência? | ❌ Não | ✅ Sim (ex: EBITDA TTM caindo quarter-a-quarter) |
| Fonte dos dados | Cálculo interno do Yahoo | Soma dos `quarterly_income_stmt` individuais |
| Consistency c/ our EV | Não garantida | ✅ Usa mesmo EV estimado do período |
| Disponível para toda empresa | Nem sempre | Sim (exceto `ttm_quarters_count < 2`) |

### 7.7 Boas Práticas de Consumo

1. **Filtrar por `ttm_quarters_count`**: Para análises confiáveis, usar `WHERE ttm_quarters_count = 4`
2. **Para anuais, preferir valores diretos**: Os campos TTM dos anuais são os próprios valores (redundantes mas consistentes)
3. **Para múltiplos cross-country**: Usar `enterprise_value_usd` e campos `*_usd` para comparabilidade
4. **Para séries temporais**: Combinar anuais + trimestrais com `ttm_quarters_count = 4` para máxima cobertura

---

## 8. Qualidade de Dados

### 8.1 Deduplicação de Empresas

A tabela `company_basic_data` pode conter múltiplos registros com o mesmo `yahoo_code` (oriundos de diferentes fontes Damodaran que listam a mesma empresa em múltiplas indústrias/setores).

**Script:** `scripts/deduplicate_companies.py`

**Estratégia:**
1. Agrupa registros por `yahoo_code`
2. Mantém o registro com mais dados em `company_financials_historical`
3. Migra registros financeiros não-conflitantes do registro removido para o mantido
4. Deleta registros financeiros duplicados (mesma empresa + período + data)
5. Remove o registro `company_basic_data` excedente

**Uso:**
```bash
python scripts/deduplicate_companies.py --dry-run  # simulação
python scripts/deduplicate_companies.py             # execução real
```

### 8.2 Indicadores de Qualidade no Frontend

A página `/company-analysis` exibe **badges de qualidade** acima das tabelas de mercado:

| Badge | Descrição | Cor |
|-------|-----------|-----|
| Períodos | Total de registros | Verde |
| Preço | Registros com `close_price > 0` | Verde ≥90%, Amarelo ≥50%, Vermelho <50% |
| Ações | Registros com `ordinary_shares_number > 0` | Idem |
| EV | Registros com `enterprise_value_estimated` preenchido | Idem |
| Múltiplos | Registros com `ev_revenue` calculado | Idem |
| TTM | Registros com `total_revenue_ttm` (só trimestral) | Idem |
| 4Q completos | Registros com `ttm_quarters_count >= 4` (só trimestral) | Idem |
| Múltiplas moedas | Alerta se a série histórica tem moedas diferentes | Vermelho |

---

## 9. Estrutura de Dados

### 9.1 Tabela `company_financials_historical`

- **Chave primária**: `id` (autoincrement)
- **Restrição de unicidade**: `UNIQUE(company_basic_data_id, period_type, period_date)` — garante um registro por empresa/tipo/data
- **`period_type`**: `'annual'` ou `'quarterly'`
- **`period_date`**: data de encerramento do período fiscal (formato ISO: `YYYY-MM-DD`)
- **`fiscal_year`**: ano fiscal extraído da data do período
- **`fiscal_quarter`**: trimestre (1-4) para dados trimestrais, NULL para anuais

### 9.2 Índices

| Índice | Colunas | Finalidade |
|--------|---------|-----------|
| `idx_cfh_yahoo` | `yahoo_code` | Busca rápida por ticker |
| `idx_cfh_period` | `period_type, fiscal_year` | Filtros por tipo e ano |
| `idx_cfh_company` | `company_basic_data_id` | Join com `company_basic_data` |

### 9.3 Relacionamentos

```
company_basic_data (1) ──── (N) company_financials_historical
    id                          company_basic_data_id
    yahoo_code                  yahoo_code
    company_name                company_name
    currency                    original_currency
```

---

## 10. Qualidade dos Dados

### 10.1 Cobertura Temporal

O yfinance retorna tipicamente:
- **4-5 períodos anuais** (últimos ~5 anos fiscais)
- **4-5 períodos trimestrais** (últimos ~16 meses)

O período mais antigo pode ter dados incompletos (todos N/A), especialmente para empresas menores.

### 10.2 Classificação de Qualidade

O campo `data_quality` indica a completude dos dados:
- `ok` — dados com métricas principais disponíveis

### 10.3 Validação

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

## 11. Exemplo Completo — Cálculo do EV

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

## 12. Resumo: Campos Raw vs Calculados

### 12.1 Campos 🟣 Raw Yahoo (31 campos — extraídos sem transformação)

| # | Campo no BD | Demonstrativo | Campo yfinance |
|---|---|---|---|
| 1 | `total_revenue` | Income Statement | `Total Revenue` |
| 2 | `cost_of_revenue` | Income Statement | `Cost Of Revenue` |
| 3 | `gross_profit` | Income Statement | `Gross Profit` |
| 4 | `operating_income` | Income Statement | `Operating Income` |
| 5 | `operating_expense` | Income Statement | `Operating Expense` |
| 6 | `ebit` | Income Statement | `EBIT` |
| 7 | `ebitda` | Income Statement | `EBITDA` |
| 8 | `normalized_ebitda` | Income Statement | `Normalized EBITDA` |
| 9 | `net_income` | Income Statement | `Net Income` |
| 10 | `interest_expense` | Income Statement | `Interest Expense` |
| 11 | `tax_provision` | Income Statement | `Tax Provision` |
| 12 | `research_and_development` | Income Statement | `Research And Development` |
| 13 | `sga` | Income Statement | `Selling General And Administration` |
| 14 | `diluted_average_shares` | Income Statement | `Diluted Average Shares` |
| 15 | `free_cash_flow` | Cash Flow | `Free Cash Flow` |
| 16 | `operating_cash_flow` | Cash Flow | `Operating Cash Flow` |
| 17 | `capital_expenditure` | Cash Flow | `Capital Expenditure` |
| 18 | `total_assets` | Balance Sheet | `Total Assets` |
| 19 | `total_debt` | Balance Sheet | `Total Debt` |
| 20 | `stockholders_equity` | Balance Sheet | `Stockholders Equity` |
| 21 | `total_liabilities` | Balance Sheet | `Total Liabilities Net Minority Interest` |
| 22 | `cash_and_equivalents` | Balance Sheet | `Cash And Cash Equivalents` |
| 23 | `short_term_debt` | Balance Sheet | `Current Debt` |
| 24 | `long_term_debt` | Balance Sheet | `Long Term Debt` |
| 25 | `short_term_investments` | Balance Sheet | `Other Short Term Investments` |
| 26 | `current_assets` | Balance Sheet | `Current Assets` |
| 27 | `current_liabilities` | Balance Sheet | `Current Liabilities` |
| 28 | `ordinary_shares_number` | Balance Sheet | `Ordinary Shares Number` |
| 29 | `preferred_stock` | Balance Sheet | `Preferred Stock` |
| 30 | `minority_interest` | Balance Sheet | `Minority Interest` |
| 31 | `close_price` | Preço Histórico | `ticker.history()` nearest date |

Adicionais raw (metadata): `original_currency`, `fx_rate_to_usd`

### 12.2 Campos 🟠 Calculados pela Aplicação (26 campos)

| # | Campo no BD | Fórmula / Origem | Script |
|---|---|---|---|
| 1 | `market_cap_estimated` | `close_price × ordinary_shares_number` | fetch |
| 2 | `enterprise_value_estimated` | `MCap + Dívida + Preferred + Minority − Caixa` | fetch |
| 3 | `total_revenue_usd` | `total_revenue × fx_rate_to_usd` | fetch |
| 4 | `ebit_usd` | `ebit × fx_rate_to_usd` | fetch |
| 5 | `ebitda_usd` | `ebitda × fx_rate_to_usd` | fetch |
| 6 | `net_income_usd` | `net_income × fx_rate_to_usd` | fetch |
| 7 | `free_cash_flow_usd` | `free_cash_flow × fx_rate_to_usd` | fetch |
| 8 | `enterprise_value_usd` | `enterprise_value_estimated × fx_rate_to_usd` | fetch |
| 9 | `ebit_margin` | `ebit / total_revenue` | fetch |
| 10 | `ebitda_margin` | `ebitda / total_revenue` | fetch |
| 11 | `gross_margin` | `gross_profit / total_revenue` | fetch |
| 12 | `net_margin` | `net_income / total_revenue` | fetch |
| 13 | `fcf_revenue_ratio` | `free_cash_flow / total_revenue` | fetch |
| 14 | `fcf_ebitda_ratio` | `free_cash_flow / ebitda` | fetch |
| 15 | `capex_revenue` | `abs(capital_expenditure) / total_revenue` | fetch |
| 16 | `debt_equity` | `total_debt / stockholders_equity` | fetch |
| 17 | `debt_ebitda` | `total_debt / ebitda` | fetch |
| 18 | `ev_revenue` | `EV / total_revenue` (ou TTM em quarterly) | fetch + ttm |
| 19 | `ev_ebitda` | `EV / ebitda` (ou TTM em quarterly) | fetch + ttm |
| 20 | `ev_ebit` | `EV / ebit` (ou TTM em quarterly) | fetch + ttm |
| 21 | `total_revenue_ttm` | Σ revenue 4 trimestres | calculate_ttm |
| 22 | `ebitda_ttm` | Σ ebitda 4 trimestres | calculate_ttm |
| 23 | `ebit_ttm` | Σ ebit 4 trimestres | calculate_ttm |
| 24 | `free_cash_flow_ttm` | Σ free_cash_flow 4 trimestres | calculate_ttm |
| 25 | `net_income_ttm` | Σ net_income 4 trimestres | calculate_ttm |
| 26 | `ttm_quarters_count` | Contagem de Q com revenue não-nulo | calculate_ttm |

## 13. Resumo das Fórmulas

| Métrica | Fórmula | Fonte |
|---------|---------|-------|
| **Market Cap (est.)** | $P_{\text{close}} \times \text{OrdinarySharesNumber}$ | 🟠 |
| **Enterprise Value** | $\text{MCap} + \text{Dívida Total} + \text{Preferred} + \text{Minority} - \text{Caixa}$ | 🟠 |
| **Revenue TTM** | $\sum_{i=0}^{3} \text{Revenue}_{Q_{t-i}}$ | 🟠 |
| **Margem EBIT** | $\frac{\text{EBIT}}{\text{Receita}}$ | 🟠 |
| **Margem EBITDA** | $\frac{\text{EBITDA}}{\text{Receita}}$ | 🟠 |
| **Margem Bruta** | $\frac{\text{Lucro Bruto}}{\text{Receita}}$ | 🟠 |
| **Margem Líquida** | $\frac{\text{Lucro Líquido}}{\text{Receita}}$ | 🟠 |
| **FCF/Receita** | $\frac{\text{FCF}}{\text{Receita}}$ | 🟠 |
| **FCF/EBITDA** | $\frac{\text{FCF}}{\text{EBITDA}}$ | 🟠 |
| **Capex/Receita** | $\frac{|\text{Capex}|}{\text{Receita}}$ | 🟠 |
| **Dívida/PL** | $\frac{\text{Dívida Total}}{\text{PL}}$ | 🟠 |
| **Dívida/EBITDA** | $\frac{\text{Dívida Total}}{\text{EBITDA}}$ | 🟠 |
| **EV/Receita** | $\frac{\text{EV}}{\text{Receita (ou TTM)}}$ | 🟠 |
| **EV/EBITDA** | $\frac{\text{EV}}{\text{EBITDA (ou TTM)}}$ | 🟠 |
| **Valor USD** | $\text{Valor Local} \times \text{FX Rate}(t)$ | 🟠 |

---

*Documento atualizado em março/2026. Scripts: `fetch_historical_financials.py`, `calculate_ttm.py`*
