# Estudo Anloc — Relatório Periódico de Múltiplos de Mercado

## Documentação Técnica Completa

**Versão:** 2.0  
**Última atualização:** Março/2026  
**Responsável técnico:** Eduardo Lemos

---

## 1. Visão Geral

O **Estudo Anloc** é um relatório periódico (trimestral) que compara múltiplos de mercado (EV/EBITDA, EV/Revenue) entre setores, com segmentação geográfica em 3 camadas: **Global → LATAM → Brasil**.

O relatório combina dados quantitativos extraídos de bases Damodaran/Yahoo Finance com narrativas analíticas geradas por IA (Claude Anthropic), resultando em um estudo profissional para C-level, investidores e profissionais de valuation.

### 1.1. Stack Tecnológico

| Componente | Tecnologia |
|---|---|
| Backend | Flask 3.1.2 (Python 3.12) |
| Banco de Dados | SQLite (`data/damodaran_data_new.db`) |
| Frontend | HTML/CSS/JS vanilla, Bootstrap 5.1.3, Chart.js 4.4.6 |
| IA | Anthropic Claude (claude-sonnet-4-20250514) |
| Hospedagem | Google App Engine (projeto `dataanloc`) |
| Tema visual | Dark mode profissional (navy/gold/blue) |

### 1.2. URL de Acesso

- **Produção:** `https://dataanloc.rj.r.appspot.com/estudoanloc/relatorio`
- **Local:** `http://localhost:5000/estudoanloc/relatorio`

---

## 2. Arquitetura do Sistema

```
┌──────────────────────────────────────────────────────────┐
│                    NAVEGADOR (CLIENT)                     │
│  ┌──────────────────────────────────────────────────────┐ │
│  │  estudoanloc_relatorio.html                          │ │
│  │  ┌──────────┐ ┌──────────┐ ┌────────────────────┐   │ │
│  │  │ Config   │ │ 11 Seções│ │ Chart.js           │   │ │
│  │  │ Panel    │ │ Relatório│ │ (3 gráficos)       │   │ │
│  │  └──────────┘ └──────────┘ └────────────────────┘   │ │
│  │  ┌──────────┐ ┌──────────┐ ┌────────────────────┐   │ │
│  │  │ Cache    │ │ Toolbar  │ │ Chat Drawer        │   │ │
│  │  │ List     │ │ (topo)   │ │ (contexto LLM)     │   │ │
│  │  └──────────┘ └──────────┘ └────────────────────┘   │ │
│  └──────────────────────────────────────────────────────┘ │
│                         │ fetch()                         │
└─────────────────────────┼────────────────────────────────┘
                          ▼
┌──────────────────────────────────────────────────────────┐
│                    FLASK SERVER (app.py)                  │
│  ┌────────────────────┐  ┌────────────────────────────┐  │
│  │  Endpoints REST    │  │  Funções de Cálculo        │  │
│  │  /api/estudoanloc/ │  │  _report_calc_sector_stats  │  │
│  │  generate_report   │  │  _report_evolution_sector   │  │
│  │  report_cache/*    │  │  _generate_report_narratives│  │
│  │  relatorio/chat    │  │  _generate_graph_comments   │  │
│  │  sector_deep_*     │  │                             │  │
│  └────────────────────┘  └────────────────────────────┘  │
│              │                        │                   │
│              ▼                        ▼                   │
│  ┌────────────────────┐  ┌────────────────────────────┐  │
│  │  SQLite DB         │  │  Anthropic Claude API      │  │
│  │  damodaran_data    │  │  claude-sonnet-4-20250514  │  │
│  │  report_cache      │  │  4 pontos de integração    │  │
│  └────────────────────┘  └────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## 3. Seções do Relatório

| # | Seção | ID HTML | Conteúdo | Fonte |
|---|-------|---------|----------|-------|
| 1 | Capa | `#coverSection` | Logo, título, metadados (setores, empresas, ano, trimestre) | Dados |
| 2 | Resumo Executivo | `#secResumo` | 5 KPIs + narrativa panorâmica | Dados + IA |
| 3 | Educacional | `#secEducacional` | Explicação de múltiplos e contexto geográfico | Estático |
| 4 | Ranking Setorial | `#secRanking` | Gráfico barras + tabela + análise IA | Dados + IA |
| 5 | Análise Geográfica | `#secGeo` | Gráfico agrupado + spreads + narrativa Brasil | Dados + IA |
| 6 | Contexto Macro | `#secMacro` | Análise macro (juros, inflação, câmbio) | IA |
| 7 | Evolução 2021-2025 | `#secEvolution` | Gráfico linha + tendências | Dados + IA |
| 8 | Destaques Setoriais | `#secDestaques` | Cards por setor (métricas, tendência, citação) | Dados + IA |
| 9 | Deep Dive por Setor | `#secDeepDive` | Geo comparison, indústrias, top BR, análise profunda | Dados + IA sob demanda |
| 10 | Perspectivas | `#secPerspectivas` | Tendências e perspectivas futuras | IA |
| 11 | Institucional | `#secInstitucional` | Sobre Anloc, disclaimer, fontes | Estático |

---

## 4. Banco de Dados

### 4.1. Tabelas Principais

#### `company_basic_data`
Dados cadastrais das empresas.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID interno |
| `ticker` | TEXT | Código na bolsa (ex: MGLU3) |
| `company_name` | TEXT | Nome da empresa |
| `yahoo_sector` | TEXT | Setor (ex: Technology, Healthcare) |
| `yahoo_industry` | TEXT | Indústria mais específica |
| `yahoo_country` | TEXT | País (ex: Brazil) |
| `enterprise_value` | REAL | Enterprise Value em USD |
| `damodaran_company_id` | INTEGER | FK para damodaran_global |

#### `company_financials_historical`
Dados financeiros históricos (anuais e trimestrais).

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `company_basic_data_id` | INTEGER FK | Referência à empresa |
| `period_type` | TEXT | 'annual' ou 'quarterly' |
| `fiscal_year` | INTEGER | Ano fiscal |
| `total_revenue` | REAL | Receita total em USD |
| `normalized_ebitda` | REAL | EBITDA normalizado em USD |
| `free_cash_flow` | REAL | Fluxo de caixa livre |
| `enterprise_value_estimated` | REAL | EV estimado no período |
| `ttm_quarters_count` | INTEGER | Trimestres no TTM |

#### `damodaran_global`
Mapeamento de empresas Damodaran para regiões globais.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `damodaran_company_id` | INTEGER | FK para company_basic_data |
| `sub_group` | TEXT | Região (ex: 'Latin America & Caribbean') |

#### `report_cache`
Cache de relatórios gerados.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID do cache |
| `generated_at` | TEXT UNIQUE | Data/hora da geração |
| `fiscal_year` | INTEGER | Ano fiscal |
| `quarter` | TEXT | Trimestre (ex: Q1/2026) |
| `report_data` | TEXT (JSON) | metadata + ranking + sectors |
| `narratives` | TEXT (JSON) | Narrativas IA |
| `graph_comments` | TEXT (JSON) | Comentários dos gráficos |
| `chat_history` | TEXT (JSON) | Histórico do chat |
| `deep_analyses` | TEXT (JSON) | Análises profundas por setor |
| `label` | TEXT | Rótulo customizado pelo usuário |

### 4.2. Fluxo de Dados

```
company_basic_data ──────────┐
     │                       │
     ├── JOIN ON id ──► company_financials_historical
     │                       │
     ├── JOIN ON damodaran_   │
     │   company_id ──► damodaran_global (região)
     │                       │
     ▼                       ▼
   Filtros:               Cálculos:
   - EV ≥ 100M USD       - EV/EBITDA = EV / EBITDA
   - EV/EBITDA ≤ 60x     - EV/Revenue = EV / Revenue
   - fiscal_year match    - Medianas por segmento geográfico
                          - Percentis P25/P75
                          - Spreads (BR vs Global, BR vs LATAM)
```

---

## 5. Endpoints REST

### 5.1. Geração do Relatório

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/api/estudoanloc/generate_report` | Gera relatório completo (dados + IA) |

**Parâmetros:** `{ fiscal_year, sectors[], use_llm, api_key }`  
**Retorno:** `{ metadata, ranking[], sectors[], narratives{}, graph_comments{}, cache_id }`

### 5.2. Cache (CRUD)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/api/estudoanloc/report_cache/list` | Lista de estudos salvos |
| `GET` | `/api/estudoanloc/report_cache/load?id=N` | Carrega estudo por ID |
| `POST` | `/api/estudoanloc/report_cache/rename` | Renomeia estudo |
| `POST` | `/api/estudoanloc/report_cache/delete` | Exclui estudo |
| `POST` | `/api/estudoanloc/report_cache/save_chat` | Salva histórico do chat |
| `POST` | `/api/estudoanloc/report_cache/save_deep_analysis` | Salva análises profundas |

### 5.3. IA Interativa

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/api/estudoanloc/relatorio/chat` | Chat conversacional com contexto do relatório |
| `POST` | `/api/estudoanloc/relatorio/sector_deep_analysis` | Análise profunda de um setor |

### 5.4. Dados Auxiliares

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/api/estudoanloc/filters` | Setores, anos, regiões disponíveis |
| `GET` | `/api/estudoanloc/industries` | Indústrias por setor |
| `POST` | `/api/estudoanloc/cross_sector` | Múltiplos agregados cross-sector |
| `POST` | `/api/estudoanloc/evolution` | Série temporal de múltiplos |

---

## 6. Integração com IA (Claude)

### 6.1. Pontos de Integração

O sistema faz **4 chamadas distintas** ao Claude:

| # | Função | Quando | O que gera |
|---|--------|--------|------------|
| 1 | `_generate_report_narratives()` | Na geração do relatório | Resumo executivo, análise macro, análise Brasil, destaques setoriais, perspectivas, citações, fontes |
| 2 | `_generate_graph_comments()` | Na geração do relatório | Comentários analíticos para os 3 gráficos (ranking, geografia, evolução) |
| 3 | `api_estudoanloc_relatorio_chat()` | Sob demanda do usuário | Respostas conversacionais com contexto completo do relatório |
| 4 | `api_estudoanloc_sector_deep_analysis()` | Sob demanda, por setor | Análise profunda: panorama, Brasil, indústrias, tendências, riscos/oportunidades |

### 6.2. Modelo e Configuração

- **Modelo:** `claude-sonnet-4-20250514`
- **Max tokens:** 4096 (todas as chamadas)
- **API Key:** variável de ambiente `ANTHROPIC_API_KEY`
- **Formato de resposta:** JSON estruturado (exceto chat, que é texto livre)

### 6.3. Fluxo de Geração com IA

```
1. Backend calcula dados de TODOS os setores (50+)
2. Monta contexto textual com rankings, spreads, evolução
3. Chamada #1: _generate_report_narratives() → JSON com narrativas
4. Chamada #2: _generate_graph_comments() → JSON com comentários dos gráficos
5. Salva tudo no report_cache (SQLite)
6. Frontend renderiza dados + narrativas
7. [Sob demanda] Chamada #3/#4: chat ou deep analysis → atualiza cache
```

---

## 7. Frontend — Funções JavaScript

### 7.1. Orquestração

| Função | Descrição |
|--------|-----------|
| `generateReport()` | Chama `/generate_report`, invoca todas as funções de rendering |
| `regenerateReport()` | Reseta e regenera com novas chamadas LLM |
| `renderReport(data)` | Função mestre: invoca todos os `render*()` |

### 7.2. Rendering

| Função | Seção |
|--------|-------|
| `renderCover(metadata)` | Capa |
| `renderKPIs(ranking)` | 5 KPIs do resumo |
| `renderNarratives(narratives)` | Blocos narrativos de IA |
| `renderRanking(ranking)` | Tabela + gráfico de ranking |
| `renderRankingChart(ranking)` | Gráfico Chart.js do ranking |
| `renderGeoChart(sectors)` | Gráfico geográfico agrupado |
| `renderEvolutionChart(sectors)` | Gráfico de evolução temporal |
| `renderAiChartComments(comments)` | Cards IA ao lado dos gráficos |
| `renderDestaques(narratives, sectors)` | Cards de destaque setorial |
| `renderDeepDives(sectors, metadata)` | Seções deep dive por setor |
| `renderReferencias(narratives)` | Seção consolidada de referências |
| `renderDeepAnalysisHtml(analysis, sector)` | HTML de uma análise profunda |

### 7.3. Cache

| Função | Descrição |
|--------|-----------|
| `checkCacheOnLoad()` | Busca lista de estudos ao carregar página |
| `renderCacheList()` | Renderiza lista com filtros de ano |
| `loadFromCache(id)` | Carrega estudo do cache e re-renderiza |
| `toggleCacheList()` | Expande/colapsa lista |
| `filterCacheByYear(year)` | Filtra por ano |
| `startRenameCache(id, label)` | Inicia edição inline do nome |
| `saveRenameCache(id)` | Persiste novo nome |
| `deleteCache(id)` | Exclui com confirmação |

### 7.4. Chat e Deep Analysis

| Função | Descrição |
|--------|-----------|
| `initReportChat()` | Inicializa chat drawer |
| `sendReportChat()` | Envia mensagem ao Claude com contexto |
| `requestDeepAnalysis(idx, sector, fy)` | Solicita análise profunda de um setor |
| `buildReportContext()` | Monta texto-contexto completo do relatório para o chat |

### 7.5. Utilitários

| Função | Descrição |
|--------|-----------|
| `exportTableToCSV(id, filename)` | Exporta qualquer tabela para CSV |
| `scrollToSectorDive(sector)` | Scroll suave até seção do setor |
| `renderFontesInline(fontes)` | Renderiza fontes como links clicáveis |
| `formatDateBR(isoStr)` | Formata data para DD/MM/AAAA HH:MM |
| `getSectorColor(sector, idx)` | Retorna cor fixa por setor |
| `updateAiStatusBar(connected, model)` | Atualiza indicador de IA |
| `updateFreshnessBadge(freshness)` | Atualiza badge de data dos dados |

---

## 8. Gráficos (Chart.js)

| ID Canvas | Tipo | Dados |
|-----------|------|-------|
| `#chartRanking` | Barras horizontais | EV/EBITDA Global vs Brasil por setor |
| `#chartGeo` | Barras agrupadas | Global vs LATAM vs Brasil por setor |
| `#chartEvolution` | Linhas | EV/EBITDA 2021–2025 por setor |

Cada gráfico usa:
- Plugin `chartjs-plugin-datalabels` para rótulos inline
- Cores por setor via `SECTOR_COLORS` (mapa fixo) + `FALLBACK_COLORS`
- Layout 2 colunas: gráfico (8col) + card IA (4col)

---

## 9. Deploy

### 9.1. Google App Engine

- **Projeto:** `dataanloc`
- **Runtime:** Python 3.12
- **Entrypoint:** `gunicorn -b :$PORT -w 2 --threads 4 --timeout 120 --preload app:app`
- **Instance class:** F4_1G
- **Scaling:** 0-3 instâncias (auto)
- **URL:** `https://dataanloc.rj.r.appspot.com`

### 9.2. Variáveis de Ambiente (app.yaml)

| Variável | Descrição |
|----------|-----------|
| `GAE_ENV` | `standard` (detecta ambiente GAE) |
| `ANTHROPIC_API_KEY` | Chave API do Claude |
| `DB_PATH` | Caminho do SQLite (default: `data/damodaran_data_new.db`) |

### 9.3. Procedimento de Deploy

```bash
# 1. Swap requirements (remove selenium/curl_cffi)
copy requirements-gae.txt requirements.txt

# 2. Deploy
gcloud app deploy app.yaml --quiet

# 3. Restaurar requirements original
copy requirements-local-backup.txt requirements.txt
```

---

## 10. Segurança e Compliance

### 10.1. Proteções Implementadas

- API Key via variável de ambiente (`.env` local, `app.yaml` no GAE)
- `.gitignore` protege `.env` e `app.yaml` (sem segredos no Git)
- Banco SQLite read-only no GAE (`?immutable=1`)
- Inputs sanitizados nos endpoints (validação de `cache_id`, `sector`, `fiscal_year`)
- Disclaimer institucional na seção 11 do relatório

### 10.2. Arquivos Protegidos no .gitignore

```
.env                    # Chave API
app.yaml               # Configuração com segredos
*.db                    # Bancos de dados
cache/                  # Cache local
__pycache__/            # Bytecode
```

---

## 11. Manutenção

### 11.1. Atualizar Dados

Os dados são atualizados via pipeline separado (scripts de extração Damodaran/Yahoo).
O relatório mostra a **data de frescor** dos dados no toolbar (`updateFreshnessBadge`).

### 11.2. Alterar Prompts IA

Os prompts estão centralizados em `app.py`:
- `_generate_report_narratives()` — Narrativas principais
- `_generate_graph_comments()` — Comentários dos gráficos
- `api_estudoanloc_relatorio_chat()` — System prompt do chat
- `api_estudoanloc_sector_deep_analysis()` — Análise profunda

### 11.3. Adicionar Nova Seção

1. Adicionar HTML com ID único no template
2. Criar função `renderNovaSecao()` em JS
3. Chamar a função em `renderReport()`
4. Se usar IA, adicionar campo no prompt de `_generate_report_narratives()`
5. Atualizar o sumário na seção Institucional
