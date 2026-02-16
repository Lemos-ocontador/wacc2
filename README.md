# WACC Hub — Calculadora WACC & Análise de Empresas

Plataforma web para **cálculo automatizado de WACC** (Weighted Average Cost of Capital) e **benchmarking de empresas** usando a base global Damodaran (~47.000+ empresas).

## Funcionalidades

### Calculadora WACC
- Taxa livre de risco (FRED — US Treasury 10Y/30Y)
- Beta setorial (Damodaran — alavancado/desalavancado)
- Prêmio de risco-país (Damodaran + BCB)
- Prêmio de risco de mercado (ERP)
- Size premium por faixa de market cap
- Custo de dívida e estrutura de capital
- Cálculo completo com fontes rastreáveis

### Análise de Empresas
- Filtros hierárquicos: região → sub-região → país / setor → subsetor → indústria
- Benchmarks estatísticos por agrupamento
- Perfil individual com dados financeiros e descrição ("about" via Yahoo Finance)
- Ranking por métricas (setor, país, global)

## Tech Stack

| Camada | Tecnologia |
|--------|-----------|
| Backend | Flask |
| Banco de dados | SQLite |
| Processamento | Pandas, NumPy |
| Dados externos | FRED API, BCB API, Damodaran (Excel), Yahoo Finance |
| Frontend | HTML/Jinja2, CSS, JavaScript |

## Instalação

```bash
# Clonar o repositório
git clone https://github.com/Lemos-ocontador/wacc2.git
cd wacc2

# Criar ambiente virtual
python -m venv .venv

# Ativar (Windows)
.venv\Scripts\activate

# Instalar dependências
pip install -r requirements.txt
```

## Executando

### App principal (WACC + Empresas)
```bash
python app.py
```
Acesse: http://localhost:5000

### App de análise de empresas (standalone)
```bash
python company_analysis_app.py
```
Acesse: http://localhost:5001

## Estrutura do Projeto

```
├── app.py                      # App principal Flask (porta 5000)
├── company_analysis_app.py     # App análise de empresas (porta 5001)
├── wacc_calculator.py          # Motor de cálculo WACC
├── wacc_data_connector.py      # Conector de dados WACC (JSON + SQLite)
├── field_categories_manager.py # Gerenciador de categorias de campos
├── geographic_mappings.py      # Mapeamentos geográficos hierárquicos
├── data_extractors/            # Extratores de dados (FRED, BCB, Damodaran)
├── scripts/                    # Scripts de ETL e manutenção de dados
├── templates/                  # Templates HTML/Jinja2
├── static/                     # CSS, JS, assets
├── data/                       # Banco SQLite + JSONs de referência
└── docs/                       # Documentação detalhada
```

## API — Principais Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| POST | `/api/calculate_wacc` | Calcula WACC completo |
| GET | `/api/get_wacc_components` | Componentes WACC por setor/país |
| GET | `/api/get_risk_free_rate` | Taxa livre de risco |
| GET | `/api/get_sector_beta` | Beta setorial |
| GET | `/api/get_country_risk` | Prêmio de risco-país |
| GET | `/api/get_size_premium` | Size premium por market cap |
| GET | `/api/companies` | Lista empresas com filtros |
| GET | `/api/benchmarks` | Benchmarks estatísticos |
| GET | `/api/company/<name>/analysis` | Análise individual |
| GET | `/api/health` | Health check |

## Fontes de Dados

- **[FRED](https://fred.stlouisfed.org/)** — Federal Reserve Economic Data (taxa livre de risco)
- **[BCB](https://www.bcb.gov.br/)** — Banco Central do Brasil (dados macro BR)
- **[Damodaran](https://pages.stern.nyu.edu/~adamodar/)** — NYU Stern (betas, ERP, risco-país, dados globais)
- **[Yahoo Finance](https://finance.yahoo.com/)** — Descrições de empresas

## Licença

Uso educacional e profissional. Dados Damodaran sujeitos aos termos de uso de NYU Stern.
