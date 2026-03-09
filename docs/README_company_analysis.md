# Anloc — Aplicação de Análise de Empresas

> Última atualização: Março/2026

## Visão Geral

Aplicação web para análise e benchmarking de empresas utilizando a base de dados Damodaran com ~47 mil empresas globais. A plataforma permite análises comparativas por país, região, setor e indústria, com dados históricos de 2021 a 2026.

O app principal (`app.py`, porta 5000) inclui todas as funcionalidades. O `company_analysis_app.py` (porta 5001) é uma versão enxuta focada apenas em análise/benchmarking.

---

## Funcionalidades

### Métricas Disponíveis

**Margens e Rentabilidade:**
- EBITDA Margin, EBIT Margin, Gross Margin, Net Margin
- ROE, ROA, Operating Margin

**Múltiplos de Valuation:**
- EV/EBITDA, EV/EBIT, EV/Revenue
- P/E Ratio, Dividend Yield

**Alavancagem:**
- Debt/Equity, Debt/EBITDA

**Eficiência:**
- FCF/Revenue, Capex/Revenue, FCF/EBITDA

**Valores Absolutos (USD):**
- Revenue, EBITDA, Net Income, Enterprise Value, Free Cash Flow, Market Cap

---

### Páginas e Funcionalidades

#### `/data-yahoo` — Dashboard Yahoo Finance
- KPIs globais (total empresas, cobertura, médias)
- Agregações por setor, indústria, país, atividade Anloc
- Análise cruzada setor × país
- Drill-down paginado com exportação CSV

#### `/data-yahoo-historico` — Dados Históricos
- **Aba Empresa**: busca por ticker, gráficos de evolução individual
- **Aba Consolidado**: selecionar múltiplas empresas e analisar
  - KPIs agregados, 6 gráficos (margens, múltiplos, receita, alavancagem, dispersão)
  - Tabela estatística (N, Média, Mediana, P25, P75, Min, Max)
  - Toggle **Consolidado/Detalhado** com dados por empresa
  - Multi-select de métricas (14 opções)
  - Ranking das empresas no último período
  - Exportação CSV (formato pt-BR: `;` delimitador, `,` decimal)
- **Aba Comparar**: comparação direta entre tickers
- **Aba Setores**: evolução temporal por setor

#### `/analise-setor` — Análise por Setor
- Filtros multi-select: setores, regiões, países, anos
- 3 abas: Agregado, Evolução, Detalhes
- Exportação CSV

#### `/company-analysis` — Análise Individual
- Filtros por país, setor, faixa de market cap
- Lista paginada com métricas principais
- Benchmarking por grupo (setor/país)
- Análise detalhada: rankings setoriais, nacionais e globais

#### `/exporta-data` — Exportação de Dados
- Exportação Excel (.xlsx) com filtros e preview
- Seleção de campos, formatos e agrupamentos

---

### APIs de Análise

| Rota | Descrição |
|------|-----------|
| `GET /api/companies` | Lista empresas com filtros hierárquicos (geo + setor + market cap) |
| `GET /api/benchmarks` | Benchmarks estatísticos por grupo |
| `GET /api/company/<name>/analysis` | Análise detalhada + posição em rankings |
| `GET /api/yahoo_dashboard_summary` | KPIs globais |
| `GET /api/yahoo_dashboard_sectors` | Métricas por setor Yahoo |
| `GET /api/yahoo_dashboard_countries` | Métricas por país |
| `GET /api/yahoo_drill/companies` | Drill-down paginado |
| `GET /api/historico/search` | Busca empresas com históricos |
| `GET /api/historico/company/<code>` | Dados históricos de uma empresa |
| `POST /api/historico/consolidated` | Consolidado + detalhado por empresa |
| `GET /api/analise_setor/data` | Análise agregada por setor |
| `POST /api/export_excel` | Exportar para Excel |

---

## Base de Dados

SQLite (`data/damodaran_data_new.db`):

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `company_basic_data` | ~42.878 | Dados atuais das empresas (Yahoo Finance) |
| `company_financials_historical` | ~156.585 | Séries históricas 2021-2026, 26 métricas, 37k+ empresas |
| `damodaran_global` | ~47.000 | Dados originais Damodaran (Excel anual) |
| `country_risk` | ~200 | Prêmios de risco por país |
| `size_premium` | 13 | Decis de size premium (Ibbotson) |

---

## Como Usar

```bash
# App principal
python app.py
# Abrir http://localhost:5000

# App de análise (alternativo)
python company_analysis_app.py
# Abrir http://localhost:5001
```

### Fluxo Típico

1. Acessar `/data-yahoo-historico`
2. Buscar empresas por setor/país/ticker
3. Selecionar empresas na tabela (checkbox)
4. Clicar "Consolidado" ou "Detalhado"
5. Alternar métricas no multi-select
6. Exportar CSV quando necessário
