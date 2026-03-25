# Resumo da Aplicação — WACC Hub + Benchmarking Financeiro

> Última atualização: Março/2026

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura](#2-arquitetura)
3. [Estrutura de Diretórios](#3-estrutura-de-diretórios)
4. [Base de Dados](#4-base-de-dados)
5. [Mapa de Rotas — `app.py` (145 rotas)](#5-mapa-de-rotas--apppy-145-rotas)
   - 5.1 [Páginas Web (21)](#51-páginas-web-21-rotas)
   - 5.2 [API WACC — Cálculos e Componentes (17)](#52-api-wacc--cálculos-e-componentes-17-rotas)
   - 5.3 [API Benchmark (2)](#53-api-benchmark-2-rotas)
   - 5.4 [API Hierarquias e Filtros (7)](#54-api-hierarquias-e-filtros-7-rotas)
   - 5.5 [API Análise de Empresas (7)](#55-api-análise-de-empresas-7-rotas)
   - 5.6 [API Dashboard Yahoo Finance (8)](#56-api-dashboard-yahoo-finance-8-rotas)
   - 5.7 [API Drill-Down Yahoo (4)](#57-api-drill-down-yahoo-4-rotas)
   - 5.8 [API Dados Históricos Yahoo (9)](#58-api-dados-históricos-yahoo-9-rotas)
   - 5.9 [API Drill-Down Histórico (2)](#59-api-drill-down-histórico-2-rotas)
   - 5.10 [API Análise por Setor (3)](#510-api-análise-por-setor-3-rotas)
   - 5.11 [API Consistência de Dados (1)](#511-api-consistência-de-dados-1-rota)
   - 5.12 [API Qualidade de Dados (3)](#512-api-qualidade-de-dados-3-rotas)
   - 5.13 [API Exportação de Dados (2)](#513-api-exportação-de-dados-2-rotas)
   - 5.14 [API Gestão de Fontes (5)](#514-api-gestão-de-fontes-5-rotas)
   - 5.15 [API Categorias de Campos (3)](#515-api-categorias-de-campos-3-rotas)
   - 5.16 [API Histórico e Downloads (2)](#516-api-histórico-e-downloads-2-rotas)
   - 5.17 [API ETF Explorer (13)](#517-api-etf-explorer-13-rotas)
   - 5.18 [API Estudo Anloc — Múltiplos Setoriais (7)](#518-api-estudo-anloc--múltiplos-setoriais-7-rotas)
   - 5.19 [API Estudo Anloc — Insights e LLM (5)](#519-api-estudo-anloc--insights-e-llm-5-rotas)
   - 5.20 [API Estudo Anloc — Relatório Periódico (3)](#520-api-estudo-anloc--relatório-periódico-3-rotas)
   - 5.21 [API Gestão de Dados de Empresas (8)](#521-api-gestão-de-dados-de-empresas-8-rotas)
   - 5.22 [API Utilitárias e Health (1)](#522-api-utilitárias-e-health-1-rota)
   - 5.23 [Error Handlers (2)](#523-error-handlers-2)
6. [Mapa de Rotas — `company_analysis_app.py`](#6-mapa-de-rotas--company_analysis_apppy)
7. [Funcionalidades Principais](#7-funcionalidades-principais)
8. [Scripts de Manutenção](#8-scripts-de-manutenção-scripts)
9. [Como Executar](#9-como-executar)
10. [Dependências](#10-dependências)

---

## 1) Visão Geral

Plataforma Flask para **análise financeira e valuation** com múltiplos focos:

- **Cálculo automatizado de WACC** (Weighted Average Cost of Capital)
- **Benchmarking global de empresas** usando base Damodaran (~48 mil empresas)
- **Dashboards analíticos** com dados Yahoo Finance, ETFs e múltiplos setoriais
- **Gestão de dados** com pipeline de atualização, qualidade e consistência

### Ambientes

| Ambiente | URL / Porta | Infra |
|----------|-------------|-------|
| **Produção (GCloud)** | `https://dataanloc.rj.r.appspot.com` | GAE Standard, Python 3.12, F4_1G |
| Local — Principal | `http://localhost:5000` | Flask dev server |
| Local — Análise | `http://localhost:5001` | Flask dev server |

### Aplicações Flask

| App | Arquivo | Porta | Foco |
|-----|---------|-------|------|
| Principal | `app.py` | 5000 | WACC + Dashboards + Dados Yahoo + ETFs + Estudos + Gestão |
| Análise | `company_analysis_app.py` | 5001 | Análise/benchmarking de empresas (escopo enxuto) |

---

## 2) Arquitetura

### Backend

- **Flask** — rotas web + APIs REST (**145 rotas** no app principal)
- **SQLite** — `data/damodaran_data_new.db` (~48k empresas, ~258k registros históricos)
  - **Produção (GAE)**: modo `?immutable=1` (read-only, sem journal/WAL)
  - **Local**: modo padrão (leitura/escrita) com WAL
- **Pandas/NumPy** — processamento estatístico e agregações
- **yfinance** — coleta de dados financeiros do Yahoo Finance
- **Extratores** — FRED, BCB, Damodaran, web scraping
- **LLM** — Gemini/OpenAI/Anthropic para insights e chat (Estudo Anloc)

### Módulos Core

| Módulo | Linhas | Função |
|--------|--------|--------|
| `app.py` | ~8.350 | App Flask principal — 145 rotas, dashboards, APIs |
| `wacc_calculator.py` | — | Motor de cálculo WACC (`WACCComponents` + `WACCCalculator`) |
| `wacc_data_connector.py` | — | Acesso a componentes WACC (JSON + SQLite) |
| `data_source_manager.py` | — | Gerenciamento de fontes de dados com status/auditoria |
| `company_update_manager.py` | ~620 | Gestão de atualização de dados (jobs, filtros, progresso) |
| `field_categories_manager.py` | — | Organização de campos em categorias p/ frontend |
| `geographic_mappings.py` | — | Classificação geográfica ONU M49 (país → região → sub-região) |
| `data_extractors/` | — | Pacote de extratores (FRED, BCB, Damodaran, ETF, web scraper) |

### Frontend

- Templates HTML/Jinja2 (**22 páginas**)
- **Chart.js 4.4.4** — gráficos interativos
- **Bootstrap 5.1.3** + **Font Awesome 6.0.0**
- Tema escuro nativo com CSS Variables
- Responsivo com CSS Grid

---

## 3) Estrutura de Diretórios

```
bd_damodaran/
├── app.py                          # App Flask principal (~8350 linhas, 145 rotas)
├── company_analysis_app.py         # App de análise de empresas
├── wacc_calculator.py              # Motor de cálculo WACC
├── wacc_data_connector.py          # Conector de dados WACC
├── data_source_manager.py          # Gerenciador de fontes
├── company_update_manager.py       # Gestão de atualização de dados de empresas
├── field_categories_manager.py     # Categorias de campos
├── geographic_mappings.py          # Mapeamentos geográficos ONU
├── requirements.txt                # Dependências Python (local)
├── requirements-gae.txt            # Dependências Python (GAE — sem selenium/curl_cffi)
├── README.md                       # Documentação principal
├── atualizar_github.bat            # Script de push GitHub
├── deploy_gcloud.bat               # Script de deploy GAE
├── run_periodic_update.ps1         # Orquestrador PowerShell de atualizações periódicas
├── run_all_sectors.ps1             # Orquestrador de reprocessamento por setor
├── app.yaml                        # Config Google App Engine
├── main.py                         # Entrypoint WSGI (GAE)
├── .gcloudignore                   # Ignore para deploy GAE
│
├── data_extractors/                # Extratores de dados financeiros
│   ├── base_extractor.py           # Classe base abstrata
│   ├── bcb_extractor.py            # Banco Central do Brasil
│   ├── fred_extractor.py           # FRED (US Treasury)
│   ├── damodaran_extractor.py      # Dados Damodaran
│   ├── etf_extractor.py            # Extrator de ETFs (iShares, etc.)
│   ├── holdings_providers.py       # Provedores de holdings de ETFs
│   ├── wacc_data_manager.py        # Orquestrador dos extratores
│   └── web_scraper.py              # Scraper web genérico
│
├── scripts/                        # Scripts de ETL e manutenção (~39 scripts)
│   ├── fetch_historical_financials.py   # Download de históricos via Yahoo
│   ├── update_company_data_from_yahoo_fast.py # Atualização rápida dados Yahoo
│   ├── extract_global_damodaran.py      # Importar Excel Damodaran
│   ├── discover_new_tickers.py          # Descoberta de novos tickers
│   ├── fix_yahoo_code_suffix.py         # Correção de sufixos de bolsa
│   ├── migrate_full_globalcomp.py       # Migração completa de empresas
│   ├── populate_etf_database.py         # Popular base de ETFs
│   ├── sync_company_basic_data.py       # Sincronizar company_basic_data
│   └── ...                              # +31 scripts adicionais
│
├── templates/                      # Templates HTML/Jinja2 (25 arquivos)
│   ├── main_dashboard.html         # Dashboard principal
│   ├── wacc_interface.html         # Interface WACC
│   ├── data_yahoo.html             # Dashboard dados Yahoo (treemap, cross-filters)
│   ├── data_yahoo_historico.html   # Dados históricos com consolidado
│   ├── company_analysis.html       # Perfil de empresa (5 abas)
│   ├── company_data_management.html# Gestão de atualização de dados
│   ├── etf_explorer.html           # ETF Explorer
│   ├── estudoanloc.html            # Estudo Múltiplos Setoriais Anloc
│   ├── estudoanloc_insights.html   # Insights com LLM
│   ├── estudoanloc_relatorio.html  # Relatório Periódico Anloc
│   ├── data_quality_dashboard.html # Dashboard de qualidade
│   ├── data_consistency.html       # Análise de consistência
│   ├── analise_setor.html          # Análise por setor
│   ├── metodologias.html           # Página de metodologias
│   ├── exporta_data.html           # Exportação de dados
│   └── ...                         # +10 templates adicionais
│
├── static/                         # Assets estáticos
│   ├── css/style.css
│   ├── js/app.js
│   ├── BDWACC.json                 # Dados WACC (Damodaran)
│   └── BDSize.json                 # Size Premium (Ibbotson)
│
├── data/                           # Dados (gitignored)
│   ├── damodaran_data_new.db       # BD SQLite principal
│   ├── wacc_data_sources_catalog.json
│   ├── mapeamento_campos_bolsas.json
│   └── damodaran_data/             # Excel Damodaran importados
│
└── docs/                           # Documentação
    ├── RESUMO_APLICACAO.md         # Este arquivo
    ├── gcloud.md                   # Diretrizes de deploy GCloud
    ├── README_company_analysis.md
    ├── metodologias.md
    ├── oportunidades_melhorias.md
    └── documentacao/               # Docs técnicos
```

---

## 4) Base de Dados

### SQLite — `data/damodaran_data_new.db`

| Tabela | Registros | Função |
|--------|-----------|--------|
| `company_basic_data` | ~48.156 | Dados cadastrais e financeiros atuais (Yahoo). Flag `yahoo_no_data` marca 7.685 empresas sem dados no Yahoo |
| `company_financials_historical` | ~257.534 | Séries históricas (2021-2026) de ~39.673 empresas |
| `damodaran_global` | ~48.156 | Dados originais do Excel Damodaran |
| `country_risk` | ~200 | Prêmios de risco por país |
| `size_premium` | 13 | Decis de size premium (Ibbotson) |
| `company_update_history` | variável | Histórico de jobs de atualização |

### Cobertura de Dados

| Métrica | Valor |
|---------|-------|
| Empresas com `yahoo_code` | ~48.156 |
| Empresas ativas (com dados Yahoo) | ~40.471 |
| Empresas marcadas `yahoo_no_data=1` | 7.685 |
| Empresas com dados históricos anuais | ~39.673 |
| Cobertura históricos (ativas) | ~98% |

---

## 5) Mapa de Rotas — `app.py` (145 rotas)

### 5.1 Páginas Web (21 rotas)

| Rota | Template | Descrição |
|------|----------|-----------|
| `GET /` | `main_dashboard.html` | Dashboard principal |
| `GET /wacc` | `wacc_interface.html` | Calculadora WACC |
| `GET /company-analysis` | `company_analysis.html` | Perfil de empresa (5 abas) |
| `GET /calculator` | `calculator.html` | Calculadora interativa |
| `GET /wacc_interface` | `wacc_interface.html` | Interface WACC aprimorada |
| `GET /data-updates` | `data_updates_dashboard.html` | Dashboard de atualizações |
| `GET /parametros-wacc` | `wacc_parameters.html` | Documentação parâmetros WACC |
| `GET /data-quality` | `data_quality_dashboard.html` | Dashboard visual de qualidade |
| `GET /history` | `history.html` | Histórico de cálculos WACC |
| `GET /data-yahoo` | `data_yahoo.html` | Dashboard analítico dados Yahoo |
| `GET /data-yahoo-historico` | `data_yahoo_historico.html` | Dashboard histórico Yahoo |
| `GET /exporta-data` | `exporta_data.html` | Exportação de dados |
| `GET /metodologias` | `metodologias.html` | Documentação de metodologias |
| `GET /data-consistency` | `data_consistency.html` | Análise de consistência de dados |
| `GET /analise-setor` | `analise_setor.html` | Análise comparativa por setor |
| `GET /etfs` | `etf_explorer.html` | ETF Explorer |
| `GET /estudoanloc` | `estudoanloc.html` | Estudo de múltiplos setoriais Anloc |
| `GET /estudoanloc/insights` | `estudoanloc_insights.html` | Página de insights com LLM |
| `GET /estudoanloc/relatorio` | `estudoanloc_relatorio.html` | Relatório periódico Estudo Anloc |
| `GET /company-data-management` | `company_data_management.html` | Gestão de atualização de dados de empresas |
| `GET /test_filter_debug.html` | — | Página de teste de filtros (debug) |

### 5.2 API WACC — Cálculos e Componentes (17 rotas)

| Rota | Descrição |
|------|-----------|
| `POST /api/calculate_wacc` | Calcula WACC completo com parâmetros customizados |
| `GET /api/get_wacc_components` | Todos os componentes WACC |
| `GET /api/get_wacc_all_live` | Diagnóstico WACC com status fontes (live vs fallback) |
| `POST /api/calculate_unlevered_beta` | Beta desalavancado: βU = βL / [1 + (1-T) × (D/E)] |
| `GET /api/validate_wacc_data` | Validação de disponibilidade e qualidade dados WACC |
| `GET /api/get_risk_free_options` | Opções de taxa livre de risco (10y/30y) |
| `GET /api/get_risk_free_rate` | Taxa livre de risco específica |
| `GET /api/get_beta_sectors` | Setores disponíveis p/ cálculo de beta |
| `GET /api/get_sector_beta` | Beta de um setor específico |
| `GET /api/get_country_risk_options` | Países com risco-país |
| `GET /api/get_country_risk` | Prêmio de risco-país específico |
| `GET /api/get_market_risk_premium` | Prêmio de risco de mercado (ERP) |
| `GET /api/get_kd_selic` | Selic live / Kd (150% Selic) |
| `GET /api/get_ipca` | IPCA 12m ao vivo (BCB) |
| `GET /api/get_market_data` | Dados de mercado em tempo real |
| `GET /api/get_size_premium` | Size premium por market cap |
| `GET /api/get_size_deciles` | Todos os decis de tamanho (Ibbotson) |

### 5.3 API Benchmark (2 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/benchmark_companies` | Lista empresas p/ seleção de benchmark |
| `POST /api/benchmark_calculate` | Calcula βU e D/E médios do grupo selecionado |

### 5.4 API Hierarquias e Filtros (7 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/get_broad_groups` | Grupos geográficos amplos (Broad Groups) |
| `GET /api/get_sub_groups` | Subgrupos regionais |
| `GET /api/get_primary_sectors` | Setores primários |
| `GET /api/get_industry_groups` | Grupos de indústria |
| `GET /api/get_subdivision_hierarchy` | Hierarquia completa de subdivisões |
| `GET /api/hierarchy` | Hierarquia para filtros do frontend |
| `GET /api/filters` | Opções de filtros (países, indústrias, setores) |

### 5.5 API Análise de Empresas (7 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/companies` | Lista empresas com filtros hierárquicos |
| `GET /api/benchmarks` | Benchmarks estatísticos por grupo |
| `GET /api/company/<name>/analysis` | Análise detalhada com rankings |
| `GET /api/company_profile` | Perfil unificado empresa (básico + Damodaran + históricos) |
| `GET /api/company_search` | Busca autocomplete por ticker/nome |
| `GET /api/get_sectors` | Lista setores disponíveis |
| `GET /api/get_countries` | Lista países disponíveis |

### 5.6 API Dashboard Yahoo Finance (8 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/yahoo_filter_options` | Opções de filtros com cross-filter (aceita filtros ativos) |
| `GET /api/yahoo_dashboard_summary` | Resumo geral (KPI cards) |
| `GET /api/yahoo_dashboard_sectors` | Métricas por setor Yahoo |
| `GET /api/yahoo_dashboard_industries` | Métricas por indústria |
| `GET /api/yahoo_dashboard_countries` | Métricas por país |
| `GET /api/yahoo_dashboard_atividades` | Métricas por atividade Anloc |
| `GET /api/yahoo_dashboard_cross` | Análise cruzada setor × país |
| `GET /api/yahoo_dashboard_treemap` | Dados para treemap de market cap |

### 5.7 API Drill-Down Yahoo (4 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/yahoo_drill/companies` | Empresas paginadas com múltiplos filtros |
| `GET /api/yahoo_drill/distribution` | Distribuição de indicador |
| `GET /api/yahoo_drill/coverage` | Cobertura de campos (com/sem dados) |
| `GET /api/yahoo_drill/currencies` | Lista moedas com contagem |

### 5.8 API Dados Históricos Yahoo (9 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/historico/summary` | Resumo geral dos dados históricos |
| `GET /api/historico_filter_options` | Opções de filtro cross-filter |
| `GET /api/historico/search` | Busca empresas com dados históricos |
| `GET /api/historico/company/<yahoo_code>` | Dados históricos de uma empresa |
| `GET /api/historico/sector_evolution` | Evolução temporal de métricas por setor |
| `GET /api/historico/compare` | Comparar múltiplas empresas |
| `GET /api/historico/sectors_list` | Lista setores com dados históricos |
| `POST /api/historico/consolidated` | Dados consolidados + detalhado por empresa |

### 5.9 API Drill-Down Histórico (2 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/historico_drill/companies` | Lista paginada com filtros |
| `GET /api/historico_drill/year_detail` | Detalhe individual por ano/métrica |

### 5.10 API Análise por Setor (3 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/analise_setor/filters` | Filtros: setores, regiões, países, anos |
| `GET /api/analise_setor/data` | Dados agregados por setor/localidade |
| `GET /api/analise_setor/detail` | Dados individuais p/ tabela e exportação |

### 5.11 API Consistência de Dados (1 rota)

| Rota | Descrição |
|------|-----------|
| `GET /api/data_consistency/validate` | Executa validação de consistência |

### 5.12 API Qualidade de Dados (3 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/data_quality_results` | Resultados do último teste de qualidade |
| `POST /api/run_data_quality_test` | Inicia teste de qualidade (background) |
| `GET /api/data_quality_progress` | Progresso do teste de qualidade |

### 5.13 API Exportação de Dados (2 rotas)

| Rota | Descrição |
|------|-----------|
| `POST /api/export_excel` | Exportar dados filtrados para Excel (.xlsx) |
| `POST /api/export_preview` | Preview dos dados antes de exportar |

### 5.14 API Gestão de Fontes (5 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/data_sources_status` | Status de todas as fontes de dados |
| `POST /api/update_data_source/<source_id>` | Atualizar fonte específica |
| `GET /api/update_all_sources` | Atualizar todas as fontes (SSE streaming) |
| `GET /api/data_update_history` | Histórico de atualizações |
| `GET /api/size_premium_data` | Auditoria Size Premium (BDSize.json) |

### 5.15 API Categorias de Campos (3 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/get_field_categories` | Categorias de campos disponíveis |
| `GET /api/get_category_fields/<category_id>` | Campos de uma categoria específica |
| `GET /api/get_field_info/<field_name>` | Metadados de um campo específico |

### 5.16 API Histórico e Downloads (2 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/get_history` | Histórico de cálculos WACC |
| `GET /api/download_calculation/<filename>` | Download de cálculo específico |

### 5.17 API ETF Explorer (13 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/etfs` | Lista ETFs com filtros e cross-filter |
| `GET /api/etfs/stats` | Estatísticas da base de ETFs |
| `GET /api/etfs/filter_options` | Opções de filtros cross-filter + tags |
| `GET /api/etfs/drill/top_holdings` | Top holdings agregados |
| `GET /api/etfs/tags/stats` | Estatísticas de tags de ETFs |
| `GET /api/etfs/tags/search` | Busca ETFs por tag |
| `GET /api/etfs/tags/values` | Valores únicos por tag_type |
| `POST /api/etfs/tags/auto-tag` | Auto-tagging (all ou ticker específico) |
| `GET /api/etfs/<ticker>` | Detalhes do ETF + holdings + breakdowns |
| `GET /api/etfs/search` | Busca reversa: em quais ETFs um ticker aparece |
| `GET /api/etfs/overlap` | Calcula sobreposição entre dois ETFs |
| `GET /api/etfs/compare` | Compara N ETFs side-by-side |
| `GET /api/etfs/export` | Exporta holdings como CSV |

### 5.18 API Estudo Anloc — Múltiplos Setoriais (7 rotas)

| Rota | Descrição |
|------|-----------|
| `POST /api/estudoanloc/cross_sector` | Múltiplos cross-sector |
| `GET /api/estudoanloc/filters` | Opções de filtro |
| `GET /api/estudoanloc/industries` | Indústrias de um setor específico |
| `POST /api/estudoanloc/calculate` | Calcula múltiplos com 4 camadas de filtro |
| `POST /api/estudoanloc/evolution` | Evolução de múltiplos ao longo de vários anos |
| `POST /api/estudoanloc/company_detail` | Histórico multi-ano de uma empresa |
| `POST /api/estudoanloc/companies_multiyear` | Base analítica de todas as empresas |

### 5.19 API Estudo Anloc — Insights e LLM (5 rotas)

| Rota | Descrição |
|------|-----------|
| `POST /api/estudoanloc/insights` | Gera insights heurísticos |
| `POST /api/estudoanloc/insights_llm` | Gera insights com LLM (Gemini/OpenAI/Anthropic) |
| `POST /api/estudoanloc/evolution_data` | Estatísticas de múltiplos por ano |
| `POST /api/estudoanloc/chat` | Chat conversacional com LLM sobre dados |
| `POST /api/estudoanloc/companies_full` | Base analítica completa |

### 5.20 API Estudo Anloc — Relatório Periódico (3 rotas)

| Rota | Descrição |
|------|-----------|
| `GET /api/estudoanloc/check_ai` | Verifica conectividade com API de IA |
| `GET /api/estudoanloc/relatorio/sectors_industries` | Setores/indústrias disponíveis |
| `POST /api/estudoanloc/relatorio/companies_detail` | Dados de empresas em nível detalhado |

### 5.21 API Gestão de Dados de Empresas (8 rotas)

> Backend: `company_update_manager.py` — gerencia jobs de atualização via subprocessos

| Rota | Descrição |
|------|-----------|
| `GET /api/company-updates/stats` | Estatísticas do banco (totais, cobertura, pendências) |
| `GET /api/company-updates/filters` | Opções de filtro (setores, indústrias, países) com cascade setor→indústria |
| `GET /api/company-updates/count` | Contagem de empresas afetadas por filtro |
| `GET /api/company-updates/filtered-stats` | Estatísticas filtradas (cards dinâmicos) |
| `POST /api/company-updates/start` | Inicia job de atualização (8 tipos de job) |
| `GET /api/company-updates/progress` | Polling de progresso do job ativo |
| `POST /api/company-updates/cancel` | Cancela job em execução |
| `GET /api/company-updates/history` | Histórico de jobs com status e duração |

**Tipos de Job disponíveis:**
- `update_yahoo_data` — Atualizar dados Yahoo Finance (básicos)
- `fetch_historical` — Download de demonstrações financeiras históricas
- `fetch_quarterly` — Download de demonstrações trimestrais
- `update_about` — Atualizar descrição/about das empresas
- `fetch_prices` — Atualizar preços atuais
- `discover_tickers` — Descobrir novos tickers
- `sync_basic_data` — Sincronizar dados básicos (Damodaran → Yahoo)
- `recalculate_ratios` — Recalcular indicadores financeiros

### 5.22 API Utilitárias e Health (1 rota)

| Rota | Descrição |
|------|-----------|
| `GET /api/health` | Health check da aplicação |

### 5.23 Error Handlers (2)

| Handler | Descrição |
|---------|-----------|
| `404` | Página não encontrada |
| `500` | Erro interno do servidor |

---

## 6) Mapa de Rotas — `company_analysis_app.py`

App focado em análise de empresas (porta 5001):

| Rota | Descrição |
|------|-----------|
| `GET /` | Página principal (`company_analysis.html`) |
| `GET /api/filters` | Países e indústrias disponíveis |
| `GET /api/companies` | Consulta empresas com filtros |
| `GET /api/benchmarks` | Benchmarks por grupo (setor/país) |
| `GET /api/company/<name>/analysis` | Análise detalhada + rankings |

---

## 7) Funcionalidades Principais

### WACC Calculator
- Cálculo automatizado com componentes: Rf, β, ERP, Risco-País, Size Premium, Kd, D/E
- Fontes live: FRED (US T-bonds), BCB (Selic, IPCA), Damodaran (β, ERP)
- Fórmula: `Ke = Rf + β × ERP + Rp + SP`
- WACC: `WACC = Ke × E/(D+E) + Kd × (1-T) × D/(D+E)`

### Dashboard Yahoo Finance
- ~40.471 empresas ativas com dados atuais do Yahoo Finance
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
- ~257.534 registros históricos (2021-2026) de ~39.673 empresas
- 4 abas: Empresa, Consolidado, Comparar, Setores
- Consolidado: KPIs, 6 gráficos, tabela estatística, ranking
- Toggle Consolidado/Detalhado com multi-select de métricas
- 26 métricas: margens, múltiplos, alavancagem, eficiência, valores USD
- Exportação CSV formatada p/ Excel pt-BR (`;` delimitador, `,` decimal)

### Análise por Setor
- Filtros multi-select: setores, regiões, países, anos
- 3 abas: Agregado, Evolução, Detalhes
- Exportação CSV

### ETF Explorer
- Base de ETFs com holdings, breakdowns e tags
- Busca reversa (em quais ETFs um ticker aparece)
- Comparação side-by-side de múltiplos ETFs
- Cálculo de sobreposição entre ETFs
- Auto-tagging baseado em categorização
- Exportação de holdings em CSV

### Estudo Anloc — Múltiplos Setoriais
- Cálculo de múltiplos (EV/EBITDA, P/E, P/B, etc.) com 4 camadas de filtro
- Análise cross-sector de múltiplos
- Evolução temporal multi-ano
- **Insights heurísticos** automatizados
- **Chat com LLM** (Gemini/OpenAI/Anthropic) sobre dados dos setores
- Relatórios periódicos automatizados

### Gestão de Dados de Empresas
- Página administrativa para gerenciamento de atualizações
- **8 tipos de job** (dados Yahoo, históricos, trimestrais, about, preços, tickers, sync, ratios)
- Filtros com cascade setor → indústria
- **Multi-select** de jobs com fila de execução sequencial
- **8 cards de resumo** com estatísticas do banco
- Pipeline de atualização com visualização de progresso
- Histórico de execuções com status e duração
- Flag `yahoo_no_data` para excluir ~7.685 empresas sem dados no Yahoo

### Qualidade e Consistência de Dados
- Dashboard de qualidade com testes automatizados
- Análise de consistência entre tabelas
- Progresso em tempo real de testes

### Exportação de Dados
- Exportação Excel (.xlsx) com filtros e preview
- Exportações CSV em todas as tabelas de dados

---

## 8) Scripts de Manutenção (`scripts/`)

### Scripts Principais

| Script | Função | Frequência |
|--------|--------|------------|
| `fetch_historical_financials.py` | Download de históricos via Yahoo (anual/trimestral) | Trimestral |
| `update_company_data_from_yahoo_fast.py` | Atualizar dados financeiros atuais via Yahoo | Mensal |
| `update_company_data_from_yahoo.py` | Versão completa (mais lenta) | Sob demanda |
| `update_company_about_from_yahoo.py` | Atualizar "about" das empresas | Semestral |
| `update_about_lotes_grandes.py` | About em lotes grandes | Sob demanda |
| `extract_global_damodaran.py` | Importar Excel Damodaran | Anual |
| `import_excel_full_fields.py` | Importar campos completos do Excel | Anual |
| `sync_company_basic_data.py` | Sincronizar dados básicos | Sob demanda |
| `discover_new_tickers.py` | Descobrir e integrar novos tickers | Sob demanda |
| `fix_yahoo_codes.py` | Corrigir códigos Yahoo | Sob demanda |
| `fix_yahoo_code_suffix.py` | Corrigir sufixos de bolsa | Sob demanda |
| `create_country_risk_db.py` | Popular tabela de risco-país | Anual |
| `import_size_premium.py` | Popular size premium (Ibbotson) | Anual |

### Scripts de ETL e Migração

| Script | Função |
|--------|--------|
| `migrate_full_globalcomp.py` | Migração completa de empresas (Damodaran → Yahoo) |
| `safe_migrate_globalcomp_2026.py` | Migração segura com validação |
| `migrate_add_classification_columns.py` | Adicionar colunas de classificação |
| `migrate_add_financial_columns.py` | Adicionar colunas financeiras |
| `migrate_sic_atividade_anloc.py` | Migrar SIC → Atividade Anloc |
| `normalize_company_yahoo_codes.py` | Normalizar códigos Yahoo |
| `recalculate_fx_rates.py` | Recalcular taxas FX históricas (USD) |
| `recalculate_ratios.py` | Recalcular indicadores financeiros |
| `calculate_ttm.py` | Calcular TTM (trailing twelve months) |
| `deduplicate_companies.py` | Deduplicar empresas |

### Scripts de ETFs

| Script | Função |
|--------|--------|
| `populate_etf_database.py` | Popular base de ETFs |
| `add_etfs.py` | Adicionar novos ETFs |
| `batch_extract_holdings.py` | Extrair holdings em lote |
| `extract_missing_holdings.py` | Extrair holdings faltantes |
| `enrich_tags_from_category.py` | Enriquecer tags por categoria |
| `enrich_tags_pass2.py` | Segundo passo de enriquecimento |

### Scripts Auxiliares

| Script | Função |
|--------|--------|
| `run_ev_fix_all_sectors.py` | Corrigir EV/Market Cap por setor |
| `implement_priority_fields.py` | Implementar campos prioritários |
| `validate_data_consistency.py` | Validar consistência de dados |
| `test_data_quality.py` | Testes de qualidade de dados |
| `wacc_data_sources_catalog.py` | Catálogo de fontes de dados WACC |
| `yahoo_code_normalizer.py` | Normalizar yahoo codes |

### Orquestradores

| Script | Função |
|--------|--------|
| `run_periodic_update.ps1` | Orquestrador PowerShell de atualizações periódicas |
| `run_all_sectors.ps1` | Orquestrador PowerShell p/ reprocessamento em massa |

---

## 9) Como Executar

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

## 10) Dependências

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
