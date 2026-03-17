# Resumo da Aplicação — WACC Hub + Benchmarking Financeiro

> Última atualização: Março/2026

## 1) Visão Geral

Plataforma Flask para **análise financeira e valuation** com dois focos:

- **Cálculo automatizado de WACC** (Weighted Average Cost of Capital)
- **Benchmarking global de empresas** usando base Damodaran (~47 mil empresas)

### Ambientes

| Ambiente | URL / Porta | Infra |
|----------|-------------|-------|
| **Produção (GCloud)** | `https://dataanloc.rj.r.appspot.com` | GAE Standard, Python 3.12, F4_1G |
| Local — Principal | `http://localhost:5000` | Flask dev server |
| Local — Análise | `http://localhost:5001` | Flask dev server |

### Aplicações Flask

| App | Arquivo | Porta | Foco |
|-----|---------|-------|------|
| Principal | `app.py` | 5000 | WACC + Dashboard + Dados Yahoo + Análises |
| Análise | `company_analysis_app.py` | 5001 | Análise/benchmarking de empresas (escopo enxuto) |

---

## 2) Arquitetura

### Backend

- **Flask** — rotas web + APIs REST (84 rotas no app principal)
- **SQLite** — `data/damodaran_data_new.db` (~47k empresas, ~157k registros históricos)
  - **Produção (GAE)**: modo `?immutable=1` (read-only, sem journal/WAL)
  - **Local**: modo padrão (leitura/escrita)
- **Pandas/NumPy** — processamento estatístico e agregações
- **yfinance** — coleta de dados financeiros do Yahoo Finance
- **Extratores** — FRED, BCB, Damodaran, web scraping

### Módulos Core

| Módulo | Função |
|--------|--------|
| `wacc_calculator.py` | Motor de cálculo WACC (`WACCComponents` + `WACCCalculator`) |
| `wacc_data_connector.py` | Acesso a componentes WACC (JSON + SQLite) |
| `data_source_manager.py` | Gerenciamento de fontes de dados com status/auditoria |
| `field_categories_manager.py` | Organização de campos em categorias p/ frontend |
| `geographic_mappings.py` | Classificação geográfica ONU M49 (país → região → sub-região) |
| `data_extractors/` | Pacote de extratores (FRED, BCB, Damodaran, web scraper) |

### Base de Dados (SQLite)

| Tabela | Registros | Função |
|--------|-----------|--------|
| `company_basic_data` | ~42.878 | Dados cadastrais e financeiros atuais (Yahoo) |
| `company_financials_historical` | ~156.585 | Séries históricas (2021-2026) de 37k+ empresas |
| `damodaran_global` | ~47.000 | Dados originais do Excel Damodaran |
| `country_risk` | ~200 | Prêmios de risco por país |
| `size_premium` | 13 | Decis de size premium |

### Frontend

- Templates HTML/Jinja2 (16 páginas)
- **Chart.js 4.4.4** — gráficos interativos
- Tema escuro nativo
- Responsivo com CSS Grid

---

## 3) Estrutura de Diretórios

```
bd_damodaran/
├── app.py                        # App Flask principal (~3680 linhas, 84 rotas)
├── company_analysis_app.py       # App de análise de empresas
├── wacc_calculator.py            # Motor de cálculo WACC
├── wacc_data_connector.py        # Conector de dados WACC
├── data_source_manager.py        # Gerenciador de fontes
├── field_categories_manager.py   # Categorias de campos
├── geographic_mappings.py        # Mapeamentos geográficos ONU
├── requirements.txt              # Dependências Python (local)
├── requirements-gae.txt          # Dependências Python (GAE — sem selenium/curl_cffi)
├── README.md                     # Documentação principal
├── atualizar_github.bat          # Script de push GitHub
├── deploy_gcloud.bat             # Script de deploy GAE
├── app.yaml                      # Config Google App Engine
├── main.py                       # Entrypoint WSGI (GAE)
├── .gcloudignore                 # Ignore para deploy GAE
│
├── data_extractors/              # Extratores de dados financeiros
│   ├── base_extractor.py         # Classe base abstrata
│   ├── bcb_extractor.py          # Banco Central do Brasil
│   ├── fred_extractor.py         # FRED (US Treasury)
│   ├── damodaran_extractor.py    # Dados Damodaran
│   ├── wacc_data_manager.py      # Orquestrador dos extratores
│   └── web_scraper.py            # Scraper web genérico
│
├── scripts/                      # Scripts de ETL e manutenção (24 scripts)
│   ├── fetch_historical_financials.py   # Download de históricos via Yahoo
│   ├── extract_global_damodaran.py      # Importar Excel Damodaran
│   ├── sync_company_basic_data.py       # Sincronizar company_basic_data
│   └── ...
│
├── templates/                    # Templates HTML/Jinja2 (17 templates)
│   ├── main_dashboard.html       # Dashboard principal
│   ├── wacc_interface.html       # Interface WACC
│   ├── data_yahoo.html           # Dashboard dados Yahoo (treemap, cross-filters)
│   ├── data_yahoo_historico.html # Dados históricos com consolidado
│   ├── company_analysis.html     # Perfil de empresa (5 abas)
│   ├── metodologias.html         # Página de metodologias
│   ├── analise_setor.html        # Análise por setor
│   └── ...
│
├── static/                       # Assets estáticos
│   ├── css/style.css
│   ├── js/app.js
│   ├── BDWACC.json               # Dados WACC (Damodaran)
│   └── BDSize.json               # Size Premium (Ibbotson)
│
├── data/                         # Dados (gitignored)
│   ├── damodaran_data_new.db     # BD SQLite principal
│   ├── wacc_data_sources_catalog.json
│   └── mapeamento_campos_bolsas.json
│
└── docs/                         # Documentação
    ├── RESUMO_APLICACAO.md       # Este arquivo
    ├── gcloud.md                 # Diretrizes de deploy GCloud
    ├── README_company_analysis.md
    ├── metodologias.md
    ├── oportunidades_melhorias.md
    └── documentacao/             # Docs técnicos
```

---

## 4) Mapa de Rotas — `app.py`

### 4.1 Páginas Web (15 rotas)

| Rota | Template | Descrição |
|------|----------|-----------|
| `GET /` | `main_dashboard.html` | Dashboard principal |
| `GET /wacc` | `wacc_interface.html` | Calculadora WACC |
| `GET /company-analysis` | `company_analysis.html` | Perfil de empresa (5 abas) |
| `GET /dashboard` | `dashboard.html` | Dashboard de saúde WACC |
| `GET /calculator` | `calculator.html` | Calculadora interativa |
| `GET /wacc_interface` | `wacc_interface.html` | Interface WACC aprimorada |
| `GET /data-updates` | `data_updates_dashboard.html` | Dashboard de atualizações |
| `GET /parametros-wacc` | `wacc_parameters.html` | Documentação parâmetros |
| `GET /history` | `history.html` | Histórico de cálculos |
| `GET /data-yahoo` | `data_yahoo.html` | Dashboard dados Yahoo |
| `GET /exporta-data` | `exporta_data.html` | Exportação de dados |
| `GET /data-yahoo-historico` | `data_yahoo_historico.html` | Dados históricos Yahoo |
| `GET /analise-setor` | `analise_setor.html` | Análise por setor/localidade |
| `GET /metodologias` | `metodologias.html` | Documentação de metodologias |

### 4.2 API — Cálculo WACC (23 rotas)

| Rota | Descrição |
|------|-----------|
| `POST /api/calculate_wacc` | Calcula WACC completo |
| `GET /api/get_wacc_components` | Todos componentes WACC |
| `POST /api/calculate_unlevered_beta` | Beta desalavancado: βU = βL / [1 + (1-T) × (D/E)] |
| `GET /api/validate_wacc_data` | Validação de dados WACC |
| `GET /api/get_risk_free_options` | Opções de RF (10y/30y) |
| `GET /api/get_risk_free_rate` | Taxa livre de risco |
| `GET /api/get_beta_sectors` | Setores disponíveis p/ beta |
| `GET /api/get_sector_beta` | Beta de um setor específico |
| `GET /api/get_country_risk_options` | Países com risco-país |
| `GET /api/get_country_risk` | Prêmio de risco-país |
| `GET /api/get_market_risk_premium` | Prêmio de risco de mercado (ERP) |
| `GET /api/get_kd_selic` | Selic live / Kd (150% Selic) |
| `GET /api/get_ipca` | IPCA 12m (BCB) |
| `GET /api/get_wacc_all_live` | Componentes WACC live |
| `GET /api/get_size_premium` | Size premium por market cap |
| `GET /api/get_size_deciles` | Decis de tamanho (Ibbotson) |
| `GET /api/get_market_data` | Dados de mercado |
| `GET /api/get_sectors` | Lista de setores |
| `GET /api/get_countries` | Lista de países |
| `GET /api/get_history` | Histórico de cálculos |
| `GET /api/health` | Health check |
| `GET /api/size_premium_data` | Dados BDSize.json |
| `POST /api/benchmark_calculate` | Benchmark β e D/E médios |

### 4.3 API — Hierarquias e Filtros (7 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/get_broad_groups` | Grupos geográficos amplos |
| `GET /api/get_sub_groups` | Subgrupos regionais |
| `GET /api/get_primary_sectors` | Setores primários |
| `GET /api/get_industry_groups` | Grupos de indústria |
| `GET /api/get_subdivision_hierarchy` | Hierarquia completa de subdivisões |
| `GET /api/hierarchy` | Hierarquia p/ filtros frontend |
| `GET /api/filters` | Opções de filtros (países, indústrias) |

### 4.4 API — Análise de Empresas (8 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/companies` | Lista empresas c/ filtros hierárquicos |
| `GET /api/benchmarks` | Benchmarks estatísticos por grupo |
| `GET /api/company/<name>/analysis` | Análise detalhada com rankings |
| `GET /api/company_profile` | Perfil completo da empresa (básico + Damodaran + históricos) |
| `GET /api/benchmark_companies` | Empresas p/ seleção de benchmark |
| `GET /api/get_field_categories` | Categorias de campos |
| `GET /api/get_category_fields/<id>` | Campos de uma categoria |
| `GET /api/get_field_info/<field>` | Metadados de um campo |

### 4.5 API — Dashboard Yahoo (8 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/yahoo_filter_options` | Opções de filtros com cross-filter (aceita filtros ativos) |
| `GET /api/yahoo_dashboard_summary` | Resumo geral (KPIs) |
| `GET /api/yahoo_dashboard_sectors` | Métricas por setor Yahoo |
| `GET /api/yahoo_dashboard_industries` | Métricas por indústria |
| `GET /api/yahoo_dashboard_countries` | Métricas por país |
| `GET /api/yahoo_dashboard_atividades` | Métricas por atividade Anloc |
| `GET /api/yahoo_dashboard_cross` | Cruzamento setor × país |
| `GET /api/yahoo_dashboard_treemap` | Treemap de market cap por setor/país |

### 4.6 API — Drill-Down (6 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/yahoo_drill/companies` | Empresas paginadas c/ filtros |
| `GET /api/yahoo_drill/distribution` | Distribuição de indicador |
| `GET /api/yahoo_drill/coverage` | Cobertura de campos |
| `GET /api/yahoo_drill/currencies` | Moedas + contagem |
| `GET /api/historico_drill/companies` | Drill-down empresas históricas |
| `GET /api/historico_drill/year_detail` | Detalhe por ano/métrica |

### 4.7 API — Dados Históricos (7 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/historico/summary` | Resumo geral |
| `GET /api/historico/search` | Busca empresas c/ dados históricos |
| `GET /api/historico/company/<code>` | Dados históricos de uma empresa |
| `GET /api/historico/sector_evolution` | Evolução temporal por setor |
| `GET /api/historico/compare` | Comparar múltiplas empresas |
| `GET /api/historico/sectors_list` | Setores com dados históricos |
| `POST /api/historico/consolidated` | Consolidado + detalhado por empresa |

### 4.8 API — Análise por Setor (3 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/analise_setor/filters` | Filtros: setores, regiões, países, anos |
| `GET /api/analise_setor/data` | Dados agregados por setor/localidade |
| `GET /api/analise_setor/detail` | Dados individuais p/ tabela/export |

### 4.9 API — Exportação (2 rotas)

| Rota | Descrição |
|------|-----------|
| `POST /api/export_excel` | Exportar p/ Excel (.xlsx) |
| `POST /api/export_preview` | Preview dos dados a exportar |

### 4.10 API — Gestão de Fontes (4 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/data_sources_status` | Status de todas as fontes |
| `POST /api/update_data_source/<id>` | Atualizar fonte específica |
| `GET /api/update_all_sources` | Atualizar todas (SSE streaming) |
| `GET /api/data_update_history` | Histórico de atualizações |

---

## 5) Mapa de Rotas — `company_analysis_app.py`

App focado em análise de empresas (porta 5001):

| Rota | Descrição |
|------|-----------|
| `GET /` | Página principal (`company_analysis.html`) |
| `GET /api/filters` | Países e indústrias disponíveis |
| `GET /api/companies` | Consulta empresas c/ filtros |
| `GET /api/benchmarks` | Benchmarks por grupo (setor/país) |
| `GET /api/company/<name>/analysis` | Análise detalhada + rankings |

---

## 6) Funcionalidades Principais

### WACC Calculator
- Cálculo automatizado com componentes: Rf, β, ERP, Risco-País, Size Premium, Kd, D/E
- Fontes live: FRED (US T-bonds), BCB (Selic, IPCA), Damodaran (β, ERP)
- Fórmula: `Ke = Rf + β × ERP + Rp + SP`
- WACC: `WACC = Ke × E/(D+E) + Kd × (1-T) × D/(D+E)`

### Dashboard Yahoo Finance
- 42.878 empresas com dados atuais do Yahoo Finance
- Agregações por setor, indústria, país, atividade Anloc
- **Treemap interativo** de market cap por setor/país
- **Cross-filter** em KPI cards (filtros cruzados entre dimensões)
- **Tabela de empresas** com busca e paginação
- Drill-down paginado com exportação CSV (respeita filtros ativos)
- Análise cruzada setor × país

### Perfil de Empresa
- Busca multi-estratégia: yahoo_code, google_finance_code, company_name (LIKE)
- **5 abas**: Institucional, Mercado, Financeiro, Histórico, Balanço & DRE
- Gráficos Chart.js com séries históricas (receita, margens, múltiplos, capital)
- Demonstrações contábeis: DRE, Balanço Patrimonial, Fluxo de Caixa
- Suporta dados anuais e trimestrais

### Dados Históricos
- 156.585 registros históricos (2021-2026) de 37k+ empresas
- 4 abas: Empresa, Consolidado, Comparar, Setores
- Consolidado: KPIs, 6 gráficos, tabela estatística, ranking
- Toggle Consolidado/Detalhado com multi-select de métricas
- 26 métricas: margens, múltiplos, alavancagem, eficiência, valores USD
- Exportação CSV formatada p/ Excel pt-BR (`;` delimitador, `,` decimal)

### Análise por Setor
- Filtros multi-select: setores, regiões, países, anos
- 3 abas: Agregado, Evolução, Detalhes
- Exportação CSV

### Exportação de Dados
- Exportação Excel (.xlsx) com filtros e preview
- Exportações CSV em todas as tabelas de dados

---

## 7) Scripts de Manutenção (`scripts/`)

| Script | Função | Frequência |
|--------|--------|------------|
| `fetch_historical_financials.py` | Download de históricos via Yahoo | Trimestral |
| `extract_global_damodaran.py` | Importar Excel Damodaran | Anual |
| `import_excel_full_fields.py` | Importar campos completos | Anual |
| `sync_company_basic_data.py` | Sincronizar dados básicos | Sob demanda |
| `update_company_data_from_yahoo.py` | Atualizar dados via Yahoo | Mensal |
| `update_company_data_from_yahoo_fast.py` | Versão rápida do acima | Mensal |
| `update_company_about_from_yahoo.py` | Atualizar "about" | Semestral |
| `fix_yahoo_codes.py` | Corrigir códigos Yahoo | Sob demanda |
| `create_country_risk_db.py` | Popular risco-país | Anual |
| `import_size_premium.py` | Popular size premium | Anual |
| `recalculate_fx_rates.py` | Recalcular taxas FX históricas (USD) | Sob demanda |
| `run_ev_fix_all_sectors.py` | Corrigir EV/Market Cap por setor | Sob demanda |
| `run_all_sectors.ps1` | Orquestrador PowerShell p/ reprocessamento em massa | Sob demanda |

---

## 8) Como Executar

### Local

```bash
# Instalar dependências
pip install -r requirements.txt

# App principal (porta 5000)
python app.py

# App de análise (porta 5001) — opcional
python company_analysis_app.py
```

Acessar: `http://localhost:5000`

### Produção (Google App Engine)

```bash
# Deploy (usando script automatizado)
deploy_gcloud.bat

# Ou manualmente
copy requirements-gae.txt requirements.txt
gcloud app deploy app.yaml --project=dataanloc --quiet
copy requirements-local-backup.txt requirements.txt
```

Acessar: `https://dataanloc.rj.r.appspot.com`

> Veja `docs/gcloud.md` para detalhes completos de infraestrutura e troubleshooting.

---

## 9) Dependências

### Local (`requirements.txt`)

```
Flask==3.1.2
pandas==3.0.0
numpy==2.4.2
openpyxl==3.1.5
requests==2.32.5
beautifulsoup4==4.14.3
yfinance==1.1.0
selenium==4.40.0
wikipedia==1.4.0
curl_cffi==0.13.0
```

### Produção GAE (`requirements-gae.txt`)

```
Flask==3.1.2
gunicorn==23.0.0
pandas==3.0.0
numpy==2.4.2
openpyxl==3.1.5
requests==2.32.5
beautifulsoup4==4.14.3
yfinance==1.1.0
wikipedia==1.4.0
```

> Excluídos do GAE: `selenium` (precisa de browser), `curl_cffi` (dependência nativa). Adicionado: `gunicorn`.

