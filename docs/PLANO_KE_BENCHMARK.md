# 📊 Plano de Implementação — Ke por Benchmark de Empresas

## 1. Objetivo

Permitir ao usuário calcular o **Custo do Capital Próprio (Ke)** com base em um **benchmark personalizado de empresas comparáveis**, em vez de usar apenas médias setoriais agregadas.

### Fluxo do Usuário (resumo)

```
Selecionar Setor → Ver tabela de empresas → Selecionar empresas comparáveis
→ Obter βU médio e D/E médio → Calcular βL → Alimentar fórmula Ke
```

---

## 2. Estado Atual (o que já existe)

### 2.1 Interface WACC (`/wacc`)
- Card **"Setor & Beta"** com 3 opções (radio buttons):
  - ✅ **Beta Setorial** — seleciona industry + região → retorna media setorial
  - 🔒 **Benchmark** — `disabled`, label "A implementar"
  - ✅ **Personalizado** — input manual
- Campos exibidos: β Alavancado, β Desalavancado, D/E Setor
- Fórmula: `Ke = Rf + βL × ERP + Rp + Rs`

### 2.2 APIs existentes
| Endpoint | O que faz |
|---|---|
| `GET /api/get_beta_sectors` | Lista setores disponíveis com estatísticas |
| `GET /api/get_sector_beta?sector=X&region=Y` | Retorna βL, βU, D/E médio do setor |
| `GET /api/companies?sector=X&industry=Y&...` | Lista empresas com filtros (usado em `/company-analysis`) |
| `GET /api/filters` | Retorna filtros hierárquicos (região, país, setor, subsetor, indústria) |

### 2.3 Banco de Dados (`damodaran_global`)
- **~42.878 empresas**, 85 indústrias, 10 setores primários
- Hierarquia: `primary_sector` → `industry_group` → `industry`
- Campos relevantes por empresa:
  - `company_name`, `ticker`, `exchange`, `country`
  - `beta` (alavancado), `debt_equity` (D/E)
  - `market_cap`, `revenue`, `net_income`
  - `bottom_up_beta_sector`, `bottom_up_beta_for_sector`
  - `unlevered_beta` (pode ser calculado)
  - `operating_margin`, `roe`, `roa`

### 2.4 Tabela de referência (`/company-analysis`)
- Filtros: Região, Sub-região, País, Setor, Subsetor, Indústria, Market Cap
- Tabela de empresas com checkboxes para seleção
- Export CSV/Excel
- Abas: Empresas, Benchmarks, Análise Detalhada

---

## 3. Especificação Funcional

### 3.1 Fluxo Completo

```
┌──────────────────────────────────────────────────────────────────┐
│  CARD Ke — Opção "Benchmark"                                     │
│                                                                  │
│  1. Usuário clica "Benchmark" (radio button)                     │
│  2. Aparece seletor de setor (industry) + região                 │
│  3. Botão "Selecionar Empresas Comparáveis"                      │
│  4. Abre MODAL com tabela de empresas (estilo company-analysis)  │
│  5. Usuário filtra/seleciona empresas via checkbox               │
│  6. Ao confirmar, calcula média dos selecionados:                │
│     - βU médio (desalavancado)                                   │
│     - D/E médio                                                  │
│  7. Valores alimentam os campos β e D/E do card Ke               │
│  8. Re-alavanca beta com D/E da empresa avaliada                 │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 Modal de Seleção de Empresas

#### Layout do Modal
```
┌─────────────────────────────────────────────────────────────────────┐
│  ✕  Selecionar Empresas Comparáveis                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─ Filtros ──────────────────────────────────────────────────┐     │
│  │ Setor: [dropdown]  Região: [dropdown]  Market Cap: [__-__] │     │
│  │ País: [dropdown multi]  [🔍 Buscar]                        │     │
│  └────────────────────────────────────────────────────────────┘     │
│                                                                     │
│  📊 Resumo: 847 empresas | Selecionadas: 5                         │
│  βU médio seleção: 0.9234 | D/E médio seleção: 0.4521              │
│                                                                     │
│  ┌─ Tabela ───────────────────────────────────────────────────┐     │
│  │ ☑ | Empresa          | Ticker | País    | Beta | D/E | MC │     │
│  │───┼──────────────────┼────────┼─────────┼──────┼─────┼────│     │
│  │ ☐ | Apple Inc        | AAPL   | USA     | 1.28 | 1.87| 3T │     │
│  │ ☐ | Microsoft Corp   | MSFT   | USA     | 0.91 | 0.35| 2T │     │
│  │ ☐ | Samsung Elec.    | 005930 | S.Korea | 1.05 | 0.12| 300B│    │
│  │ ☑ | TOTVS SA         | TOTS3  | Brazil  | 0.85 | 0.15| 15B│    │
│  │ ...                                                        │     │
│  └────────────────────────────────────────────────────────────┘     │
│                                                                     │
│  [Selecionar Tudo] [Limpar] [Exportar CSV]                         │
│                   [✅ Confirmar Seleção (5 empresas)]               │
└─────────────────────────────────────────────────────────────────────┘
```

#### Colunas da Tabela
| Coluna | Campo DB | Formato |
|---|---|---|
| ☑ (checkbox) | — | Seleção |
| Empresa | `company_name` | Texto |
| Ticker | `ticker` | Texto |
| Bolsa | `exchange` | Texto |
| País | `country` | Texto |
| Beta (βL) | `beta` | 4 decimais |
| D/E | `debt_equity` | 4 decimais |
| Market Cap | `market_cap` | Formatado (B/M) |
| Margem Op. | `operating_margin` | % |

#### Filtros do Modal
- **Setor (industry)**: Pré-preenchido se já selecionado no card Ke
- **Região (broad_group)**: Global / Emerging Markets
- **País**: Multi-select
- **Market Cap**: Min/Max (USD)
- **Busca por nome/ticker**: Input texto

#### Cálculos na Seleção (em tempo real)
```
Para cada empresa selecionada (i = 1..n):
  
  βU_i = βL_i / [1 + (1 - T) × (D/E)_i]     onde T = taxa de imposto (34%)
  
Média:
  βU_benchmark = Σ(βU_i) / n     (média simples)
  D/E_benchmark = Σ(D/E_i) / n   (média simples)
  
  OU (opção avançada):
  βU_benchmark = Σ(βU_i × MCi) / Σ(MCi)   (média ponderada por market cap)
```

### 3.3 Integração com Card Ke

Após confirmar seleção no modal:
1. `βU (Desalavancado)` ← `βU_benchmark` calculado
2. `D/E Setor` ← `D/E_benchmark` calculado (ou D/E da empresa avaliada, editável)
3. `βL (Alavancado)` ← recalculado: `βL = βU × [1 + (1-T) × (D/E)]`
4. Exibir resumo abaixo: *"Benchmark: 5 empresas selecionadas (média simples)"*
5. `source-info` → *"Benchmark personalizado (5 empresas)"*

### 3.4 Opções de Média
- **Média simples** (default): Peso igual para todas
- **Ponderada por Market Cap**: Empresas maiores pesam mais
- Toggle no modal para escolher

---

## 4. Especificação Técnica

### 4.1 Backend — Novo Endpoint

```python
# GET /api/benchmark_companies?industry=X&region=Y&country=Z&min_mc=N&max_mc=N
# Retorna empresas com campos relevantes para benchmark de beta

@app.route('/api/benchmark_companies')
def get_benchmark_companies():
    """
    Retorna empresas para seleção de benchmark com beta e D/E.
    
    Params:
        industry: filtro por industry (obrigatório)
        region: broad_group filter (opcional)
        country: lista de países (opcional, multi)
        min_market_cap: market cap mínimo (opcional)
        max_market_cap: market cap máximo (opcional)
        search: busca por nome/ticker (opcional)
    
    Returns:
        {
            companies: [
                {
                    company_name, ticker, exchange, country,
                    beta, debt_equity, market_cap,
                    operating_margin, revenue,
                    unlevered_beta  (calculado server-side)
                }, ...
            ],
            stats: {
                total: N,
                avg_beta: X.XX,
                avg_de: X.XX,
                avg_unlevered_beta: X.XX,
                median_beta: X.XX
            }
        }
    """
```

### 4.2 Backend — Endpoint de Cálculo

```python
# POST /api/benchmark_calculate
# Recebe lista de tickers selecionados, retorna beta e D/E calculados

@app.route('/api/benchmark_calculate', methods=['POST'])
def calculate_benchmark():
    """
    Calcula βU e D/E médios a partir de empresas selecionadas.
    
    Body (JSON):
        {
            tickers: ["AAPL", "MSFT", "TOTS3.SA"],
            method: "simple" | "weighted",  # tipo de média
            tax_rate: 0.34                   # taxa de imposto para desalavancar
        }
    
    Returns:
        {
            success: true,
            benchmark: {
                unlevered_beta: 0.9234,
                levered_beta_avg: 1.1456,
                debt_equity_avg: 0.4521,
                companies_used: 5,
                method: "simple",
                companies: [
                    {name, ticker, beta, de, unlevered_beta, market_cap, weight}, ...
                ]
            }
        }
    """
```

### 4.3 Frontend — Mudanças em `wacc_interface.html`

#### A) Habilitar radio "Benchmark"
```html
<!-- Remover disabled, adicionar onclick -->
<input class="form-check-input" type="radio" name="beta-option" 
       id="beta-benchmark" value="benchmark" 
       onchange="onBetaOptionChange('benchmark')">
<label class="form-check-label" for="beta-benchmark">
    <strong>Benchmark</strong><br>
    <small class="text-muted">Empresas comparáveis</small>
</label>
```

#### B) Seção Benchmark (aparece quando selecionado)
```html
<div id="benchmark-selection" style="display:none;">
    <div class="row mb-2">
        <div class="col-md-6">
            <label>Setor:</label>
            <select id="benchmark-sector-select">...</select>
        </div>
        <div class="col-md-4">
            <label>Região:</label>
            <select id="benchmark-region-select">
                <option value="global">Global</option>
                <option value="emkt">Mercados Emergentes</option>
            </select>
        </div>
        <div class="col-md-2 d-flex align-items-end">
            <button class="btn btn-primary btn-sm w-100" onclick="openBenchmarkModal()">
                <i class="fas fa-users"></i> Selecionar
            </button>
        </div>
    </div>
    <div id="benchmark-summary" class="setor-info" style="display:none;">
        <!-- Preenchido dinamicamente após seleção -->
    </div>
</div>
```

#### C) Modal de Seleção
- Novo `<div class="modal" id="benchmarkModal">` no final do HTML
- Tabela com DataTables-like (scroll, sort, busca)
- Checkboxes por linha
- Cálculo em tempo real no footer do modal
- Botão "Confirmar Seleção"

#### D) JavaScript — Funções principais
```javascript
// Abrir modal e carregar empresas
async function openBenchmarkModal() { ... }

// Carregar empresas do setor selecionado
async function loadBenchmarkCompanies(industry, region, filters) { ... }

// Toggle seleção de empresa
function toggleBenchmarkCompany(ticker) { ... }

// Recalcular médias em tempo real
function recalculateBenchmarkStats() { ... }

// Confirmar seleção e fechar modal
function confirmBenchmarkSelection() { ... }

// Aplicar valores no card Ke
function applyBenchmarkToKe(benchmarkData) { ... }
```

### 4.4 Fluxo de Dados

```
┌──────────┐    GET /api/benchmark_companies     ┌──────────┐
│ Frontend │ ──────────────────────────────────→  │ Backend  │
│  (Modal) │                                      │  (Flask) │
│          │  ←─── JSON: companies[] + stats      │          │
│          │                                      │          │
│  Usuário │    POST /api/benchmark_calculate     │          │
│  selects │ ──────────────────────────────────→  │  SQLite  │
│          │                                      │          │
│          │  ←─── JSON: βU, D/E, detalhes        │          │
│          │                                      │          │
│ Card Ke  │  ← applyBenchmarkToKe()              │          │
│ atualiza │                                      │          │
└──────────┘                                      └──────────┘
```

---

## 5. Regras de Negócio

### 5.1 Validações
- Mínimo **2 empresas** para benchmark (exibir alerta se < 2)
- Excluir empresas com `beta <= 0` ou `beta IS NULL`
- Excluir empresas com `debt_equity IS NULL` (ou tratar como 0)
- Excluir outliers opcionais: beta > 5 ou D/E > 10 (flag toggle?)

### 5.2 Desalavancagem
```
βU = βL / [1 + (1 - T) × (D/E)]

Onde:
  βL = beta da tabela (alavancado)
  T  = taxa de imposto marginal (default 34% Brasil, editável)
  D/E = debt_equity da tabela
```

### 5.3 Re-alavancagem (ao aplicar no Ke)
```
βL_empresa = βU_benchmark × [1 + (1 - T) × (D/E)_empresa]

Onde:
  βU_benchmark = média dos βU das empresas selecionadas
  T = taxa de imposto da empresa avaliada (34%)
  D/E_empresa = pode ser:
    a) D/E médio do benchmark (default)
    b) D/E informado manualmente pelo usuário
    c) D/E do setor (já existente)
```

### 5.4 Persistência (opcional — fase 2)
- Salvar seleções de benchmark do usuário em `localStorage`
- Permitir "favoritar" uma seleção para reutilização

---

## 6. Plano de Implementação

### Fase 1 — MVP (estimativa: 1 sessão)
| # | Tarefa | Arquivos |
|---|---|---|
| 1 | Endpoint `GET /api/benchmark_companies` | `app.py` |
| 2 | Endpoint `POST /api/benchmark_calculate` | `app.py` |
| 3 | Habilitar radio "Benchmark" + seção filtros | `wacc_interface.html` |
| 4 | Modal de seleção de empresas (tabela + checkboxes) | `wacc_interface.html` |
| 5 | JS: carregar empresas, seleção, cálculo real-time | `wacc_interface.html` |
| 6 | JS: confirmar e aplicar no card Ke | `wacc_interface.html` |
| 7 | Testar fluxo completo | — |

### Fase 2 — Melhorias (futuro)
| # | Tarefa |
|---|---|
| 1 | Média ponderada por market cap (toggle) |
| 2 | Filtro de outliers (toggle beta > 5) |
| 3 | Salvar benchmark em localStorage |
| 4 | Mini-gráfico scatter βU × D/E das selecionadas |
| 5 | Exportar seleção (CSV/PDF) |
| 6 | Tooltip por empresa com detalhes extras |

---

## 7. Referências Internas

| Recurso | Localização |
|---|---|
| Interface WACC | `templates/wacc_interface.html` |
| API empresas existente | `app.py` → `/api/companies` |
| Cálculo beta setorial | `wacc_data_connector.py` → `get_sector_beta()` |
| Tabela de referência visual | `templates/company_analysis.html` |
| Banco de dados | `data/damodaran_data_new.db` → `damodaran_global` |
| Hierarquia setores | `primary_sector` (10) → `industry_group` (85) → `industry` (85) |

---

*Documento criado em 16/02/2026 — Versão 1.0*
