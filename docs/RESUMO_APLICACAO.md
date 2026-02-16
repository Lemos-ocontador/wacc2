# Resumo da Aplicação (WACC + Benchmarking)

## 1) Visão Geral

Este projeto implementa uma plataforma Flask para **análise financeira e valuation**, com dois focos principais:

- **Cálculo automatizado de WACC** (Weighted Average Cost of Capital).
- **Benchmarking de empresas** usando base Damodaran global.

Há dois aplicativos Flask no repositório:

- `app.py` → app principal e mais completo (porta **5000**).
- `company_analysis_app.py` → app dedicado à análise de empresas (porta **5001**), com escopo mais enxuto/paralelo.

---

## 2) Arquitetura Principal

### Backend

- **Flask** para rotas web e APIs.
- **Pandas/Numpy** para processamento estatístico.
- **SQLite** (`data/damodaran_data_new.db`) para dados de empresas e hierarquias.
- **JSON/Cache** para componentes e histórico de cálculos.

### Módulos-chave

- `wacc_calculator.py`:
  - Estrutura `WACCComponents`.
  - Classe `WACCCalculator` para cálculo completo do WACC.
  - Prioridade de entrada dos componentes: **customizados > extraídos > defaults**.

- `wacc_data_connector.py`:
  - Acesso a componentes WACC (JSON + SQLite).
  - Endpoints de taxa livre de risco, beta setorial, risco-país, prêmio de mercado e size premium.

- `data_extractors/`:
  - Conjunto de extratores (`FRED`, `BCB`, `Damodaran`, scraper web) orquestrados por `WACCDataManager`.

- `field_categories_manager.py`:
  - Organização de campos em categorias para consumo no frontend.

### Frontend (templates)

Páginas principais em `templates/`:

- `main_dashboard.html`
- `wacc_interface.html`
- `company_analysis.html`
- `dashboard.html`
- `calculator.html`
- `history.html`
- `error.html`

---

## 3) Fluxo Funcional (WACC)

1. Frontend solicita componentes/cálculo via API.
2. `WACCCalculator` coleta dados via `WACCDataManager` e/ou `WACCDataConnector`.
3. Componentes são normalizados (taxas, beta, D/E, prêmio de risco etc.).
4. São calculados:
   - custo do capital próprio,
   - pesos de dívida/equity,
   - WACC final.
5. Resultado pode ser persistido no diretório `cache/`.

---

## 4) Mapa de Rotas — `app.py` (App Principal)

## 4.1 Páginas Web

- `GET /` → dashboard principal (`main_dashboard.html`).
- `GET /wacc` → interface WACC.
- `GET /company-analysis` → página de análise de empresas.
- `GET /test_filter_debug.html` → página de debug de filtros.
- `GET /dashboard` → visão consolidada de saúde + dados recentes + WACC padrão.
- `GET /calculator` → calculadora interativa.
- `GET /wacc_interface` → interface WACC aprimorada.
- `GET /history` → histórico dos últimos cálculos salvos.

## 4.2 API — Cálculo WACC

- `POST /api/calculate_wacc`
  - Calcula WACC com parâmetros de entrada (`sector`, `country`, valores de mercado e componentes customizados).
  - Retorna WACC, componentes, fontes e resumo.

- `GET /api/get_wacc_components`
  - Retorna pacote consolidado de componentes WACC por setor/país/região.

- `POST /api/calculate_unlevered_beta`
  - Calcula beta desalavancado usando:
  - fórmula: `βU = βL / [1 + (1 - T) × (D/E)]`.

- `GET /api/validate_wacc_data`
  - Verifica disponibilidade dos componentes críticos e status geral (`healthy/degraded`).

## 4.3 API — Componentes de Mercado

- `GET /api/get_risk_free_options` → opções de RF (10y/30y/custom).
- `GET /api/get_risk_free_rate?term=10y|30y` → taxa livre de risco.
- `GET /api/get_beta_sectors` → lista de setores com métricas.
- `GET /api/get_sector_beta?sector=...&region=global|emkt` → beta setorial.
- `GET /api/get_country_risk_options` → países disponíveis para risco-país.
- `GET /api/get_country_risk?country=...` → prêmio de risco-país.
- `GET /api/get_market_risk_premium` → ERP/prêmio de risco de mercado.
- `GET /api/get_size_premium?market_cap=...` → size premium por faixa de market cap.
- `GET /api/get_size_deciles` → decis/faixas de size premium.

## 4.4 API — Catálogos, Histórico e Saúde

- `GET /api/get_market_data` → snapshot de dados de mercado extraídos.
- `GET /api/get_sectors` → lista de setores suportados.
- `GET /api/get_countries` → lista de países suportados.
- `GET /api/get_history` → histórico de cálculos + estatísticas.
- `GET /api/download_calculation/<filename>` → download de cálculo em cache.
- `GET /api/health` → health check da aplicação/extratores.

## 4.5 API — Hierarquias e Filtros (Damodaran)

- `GET /api/get_broad_groups` → grupos geográficos amplos.
- `GET /api/get_sub_groups?broad_group=...` → subgrupos regionais.
- `GET /api/get_primary_sectors` → setores primários.
- `GET /api/get_industry_groups` → grupos de indústria.
- `GET /api/get_subdivision_hierarchy` → hierarquia geográfica + setorial completa.
- `GET /api/hierarchy` → hierarquias para popular filtros no frontend.
- `GET /api/filters` → países, indústrias e estruturas hierárquicas.

## 4.6 API — Empresas e Benchmarking

- `GET /api/companies`
  - Lista empresas com filtros hierárquicos:
    - geografia (`country`, `subregion`, `region`),
    - setor (`industry`, `subsector`, `sector`),
    - faixa de `market_cap`.

- `GET /api/benchmarks`
  - Gera benchmarks estatísticos por agrupamento (`group_by`).

- `GET /api/company/<company_name>/analysis`
  - Retorna análise da empresa selecionada.

## 4.7 API — Catálogo de Campos

- `GET /api/get_field_categories` → categorias de campos disponíveis.
- `GET /api/get_category_fields/<category_id>` → campos de uma categoria.
- `GET /api/get_field_info/<field_name>` → metadados de um campo.

---

## 5) Mapa de Rotas — `company_analysis_app.py`

App focado apenas em análise de empresas, com rotas principais:

- `GET /` → `company_analysis.html`.
- `GET /api/filters` → países e indústrias.
- `GET /api/companies` → consulta de empresas com filtros hierárquicos + market cap.
- `GET /api/benchmarks` → benchmarks por grupo (`industry`, `country` etc.).
- `GET /api/company/<company_name>/analysis` → análise detalhada da empresa:
  - dados da empresa,
  - benchmark setorial/país,
  - rankings por métricas (setor, país, global).

---

## 6) Como Executar

### App principal

```bash
python app.py
```

- URL padrão: `http://localhost:5000`

### App de análise (alternativo)

```bash
python company_analysis_app.py
```

- URL padrão: `http://localhost:5001`

---

## 7) Observações Técnicas

- Existe **sobreposição de responsabilidades** entre `app.py` e `company_analysis_app.py` nas rotas de análise de empresas.
- A base Damodaran é o núcleo para filtros hierárquicos (geografia e setor) e benchmarks.
- O projeto já possui boa base para expansão (exportações, análises avançadas e visualizações adicionais).

---

## 8) Correções Recentes (Fev/2026)

### 8.1 Beta Setorial — Indústrias Damodaran
- Corrigido: dropdown de setores agora carrega corretamente as indústrias Damodaran.
- Campos `value`, `label`, `companies_count` adicionados à API `/api/get_beta_sectors`.

### 8.2 D/E Médio do Setor
- Corrigido: campo `debt_equity_ratio` agora é retornado pela API `/api/get_sector_beta`.
- Campo `data_quality` (high/medium/low) adicionado baseado no número de empresas.
- Preenchimento automático do campo "D/E Médio do Setor" no frontend.

### 8.3 Seleção de País — Risco País
- Corrigido: endpoint `/api/get_country_risk_options` agora retorna estrutura correta.
- Países organizados em grupos: **Principais** (Brasil, EUA, China, etc.) e **Outros**.
- Cada país com campos `value`, `label`, `risk_premium` (%).
- Brasil selecionado por padrão.

### 8.4 Prêmio de Tamanho (Size Premium) — NOVO
- Implementada seção completa no frontend WACC.
- Input de Market Cap (US$) com cálculo automático do decil e prêmio.
- Tabela expansível com todos os 13 decis de tamanho.
- Prêmio de tamanho integrado na fórmula WACC: `Ke = Rf + β×ERP + Rp + SP`.
- APIs: `/api/get_size_premium?market_cap=X` e `/api/get_size_deciles`.

---

## 9) Plano de Atualização Automática de Dados

### 9.1 Fontes e Frequência

| Fonte | Componente | Frequência Ideal | Método |
|-------|-----------|-----------------|--------|
| **FRED API** | Taxa Livre de Risco (10Y/30Y) | Diária | API REST automática |
| **Damodaran (NYU)** | Betas setoriais, ERP, D/E | Anual (janeiro) | Download Excel + ETL |
| **Damodaran (NYU)** | Risco-País | Anual (janeiro) | Download Excel + ETL |
| **Damodaran (NYU)** | Size Premium (Ibbotson) | Anual | Download + ETL |
| **Yahoo Finance** | About/Descrição empresas | Semestral | Script batch Yahoo API |
| **BCB API** | Selic, IPCA, câmbio | Diária | API REST automática |

### 9.2 Scripts Existentes para Atualização

- `scripts/extract_global_damodaran.py` — importar Excel Damodaran global
- `scripts/import_excel_full_fields.py` — importar todos os campos do Excel
- `scripts/create_country_risk_db.py` — popular tabela `country_risk`
- `scripts/import_size_premium.py` — popular tabela `size_premium`
- `scripts/update_company_about_from_yahoo.py` — atualizar "about" via Yahoo
- `scripts/normalize_company_yahoo_codes.py` — normalizar códigos Yahoo

### 9.3 Plano de Automação (a implementar)

**Fase 1 — Script de atualização unificado:**
- Criar `scripts/auto_update_all.py` que orquestra as atualizações.
- Download automático do Excel Damodaran quando nova versão disponível.
- Atualização da taxa livre de risco via FRED em cada inicialização do app.

**Fase 2 — Agendamento:**
- GitHub Actions (workflow CRON) para atualização semanal dos dados FRED/BCB.
- Script local agendado (Task Scheduler / cron) para atualização anual Damodaran.

**Fase 3 — Monitoramento:**
- Health check expandido (`/api/health`) com idade dos dados.
- Alerta no dashboard quando dados estiverem desatualizados (>30 dias para FRED, >13 meses para Damodaran).

