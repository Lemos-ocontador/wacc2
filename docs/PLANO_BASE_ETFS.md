# Plano: Base de Dados de ETFs e Composições

## 1. Objetivo

Construir uma base de dados abrangente de ETFs (Exchange-Traded Funds), contendo:
- Informações cadastrais de cada ETF
- Composição completa (holdings/tickers que compõem o ETF)
- Pesos de cada ativo na composição
- Metadados relevantes (setor, região, tipo, AUM, etc.)

---

## 2. Fontes de Dados Disponíveis (Gratuitas)

### 2.1 Yahoo Finance (yfinance)
- **Prós**: Já usado no projeto, API Python madura, dados de holdings disponíveis
- **Contras**: Holdings podem estar desatualizados (frequência trimestral), limite de rate
- **Dados disponíveis**: Top 10 holdings, setor, total assets, expense ratio, preços
- **Método**: `yfinance.Ticker("SPY").institutional_holders`, `.major_holders`, `.info`

### 2.2 ETF Database (etfdb.com)
- **Prós**: Base muito completa, cobertura global
- **Contras**: Requer scraping (sem API pública gratuita), pode ter anti-bot
- **Dados disponíveis**: Holdings completos, AUM, expense ratio, classificações

### 2.3 SEC EDGAR (para ETFs dos EUA)
- **Prós**: Dados oficiais, holdings completos (13F/N-PORT filings), API gratuita
- **Contras**: Apenas ETFs registrados nos EUA, formato XML complexo
- **Dados disponíveis**: Holdings completos com pesos, valores de mercado
- **API**: `https://efts.sec.gov/LATEST/search-index?q=...`

### 2.4 IEX Cloud (plano gratuito limitado)
- **Prós**: API REST bem documentada
- **Contras**: Limite de requisições no plano free

### 2.5 OpenFIGI
- **Prós**: Mapeamento de identificadores (FIGI ↔ ticker ↔ ISIN)
- **Contras**: Não tem composição, mas útil para padronizar tickers

### 2.6 CVM (Brasil)
- **Prós**: Dados oficiais de ETFs brasileiros (composição mensal)
- **Contras**: Apenas Brasil, formato CSV/XML
- **URL**: Portal de dados abertos da CVM

### 2.7 Investing.com / MarketWatch (scraping)
- **Prós**: Cobertura ampla
- **Contras**: Termos de uso restritivos, anti-scraping agressivo

---

## 3. Estratégia Recomendada (Fonte Primária + Fallback)

```
Prioridade 1: yfinance        → Holdings parciais + metadados (fácil integração)
Prioridade 2: SEC EDGAR       → Holdings completos para ETFs dos EUA (oficial)
Prioridade 3: CVM             → ETFs brasileiros (oficial)
Prioridade 4: Web scraping    → Complementar dados faltantes
```

### 3.1 Lista de ETFs para Popular
- **Fonte inicial**: Listar ETFs a partir de índices conhecidos ou listas curadas
- Opção A: Scraping de lista de ETFs do etfdb.com (categorias)
- Opção B: Lista fixa inicial (top 500-1000 ETFs por AUM)
- Opção C: API do Yahoo Finance para listar ETFs por screener

---

## 4. Modelo de Dados (SQLite)

### 4.1 Tabela `etfs` (Cadastro do ETF)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | INTEGER PK | ID interno |
| `ticker` | TEXT UNIQUE | Ticker do ETF (ex: SPY, IVVB11) |
| `name` | TEXT | Nome completo |
| `exchange` | TEXT | Bolsa (NYSE, B3, LSE...) |
| `currency` | TEXT | Moeda (USD, BRL, EUR...) |
| `category` | TEXT | Categoria (Equity, Fixed Income, Commodity...) |
| `subcategory` | TEXT | Subcategoria (Large Cap Blend, Emerging Markets...) |
| `region` | TEXT | Região geográfica foco |
| `country` | TEXT | País de domicílio |
| `index_tracked` | TEXT | Índice rastreado (S&P 500, Ibovespa...) |
| `issuer` | TEXT | Gestora (BlackRock, Vanguard, Itaú...) |
| `inception_date` | TEXT | Data de criação |
| `expense_ratio` | REAL | Taxa de administração (%) |
| `aum` | REAL | Assets Under Management (USD) |
| `avg_volume` | INTEGER | Volume médio diário |
| `total_holdings` | INTEGER | Número total de holdings |
| `last_updated` | TEXT | Data da última atualização |
| `data_source` | TEXT | Fonte dos dados |

### 4.2 Tabela `etf_holdings` (Composição)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | INTEGER PK | ID interno |
| `etf_ticker` | TEXT FK | Ticker do ETF |
| `holding_ticker` | TEXT | Ticker do ativo (AAPL, PETR4...) |
| `holding_name` | TEXT | Nome do ativo |
| `weight` | REAL | Peso no ETF (%) |
| `shares` | INTEGER | Quantidade de ações |
| `market_value` | REAL | Valor de mercado da posição |
| `sector` | TEXT | Setor do holding |
| `asset_class` | TEXT | Classe (Equity, Bond, Cash...) |
| `report_date` | TEXT | Data do relatório de composição |
| `last_updated` | TEXT | Data da última atualização |

### 4.3 Tabela `etf_update_log` (Auditoria)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | INTEGER PK | ID interno |
| `etf_ticker` | TEXT | Ticker do ETF |
| `update_type` | TEXT | Tipo (metadata, holdings, full) |
| `source` | TEXT | Fonte usada |
| `status` | TEXT | Sucesso/falha |
| `records_count` | INTEGER | Registros atualizados |
| `timestamp` | TEXT | Data/hora |
| `error_message` | TEXT | Mensagem de erro (se houver) |

---

## 5. Arquitetura do Extrator

### 5.1 Novo Módulo: `data_extractors/etf_extractor.py`

```python
class ETFExtractor(BaseExtractor):
    """Extrator de dados de ETFs com múltiplas fontes."""
    
    def get_etf_list(source='yahoo') -> List[str]:
        """Obtém lista de tickers de ETFs."""
    
    def get_etf_metadata(ticker: str) -> dict:
        """Obtém metadados do ETF (nome, AUM, categoria...)."""
    
    def get_etf_holdings(ticker: str) -> List[dict]:
        """Obtém composição/holdings do ETF."""
    
    def get_etf_holdings_sec(cik: str) -> List[dict]:
        """Obtém holdings via SEC EDGAR (N-PORT filings)."""
    
    def get_etf_holdings_cvm(ticker: str) -> List[dict]:
        """Obtém holdings de ETFs brasileiros via CVM."""
    
    def bulk_update(tickers: List[str], batch_size=10):
        """Atualização em lote com controle de rate limit."""
```

### 5.2 Integração com Arquitetura Existente

```
WACCDataManager
  ├── DamodaranExtractor
  ├── FREDExtractor  
  ├── BCBExtractor
  ├── WebScraper
  └── ETFExtractor  ← NOVO
        ├── Yahoo Finance (yfinance)
        ├── SEC EDGAR API
        └── CVM API
```

---

## 6. Plano de Implementação (Fases)

### Fase 1 — MVP (yfinance) ✅ CONCLUÍDA
- [x] Criar tabelas SQLite (`etfs`, `etf_holdings`, `etf_update_log`)
- [x] Implementar `ETFExtractor` com yfinance como fonte primária
- [x] Criar lista inicial curada de ~200 ETFs principais (EUA + Brasil)
- [x] Script de carga inicial (`scripts/populate_etf_database.py`)
- [x] Testar extração de holdings para os top 50 ETFs

### Fase 2 — Expansão de Cobertura ✅ CONCLUÍDA
- [x] Integrar SEC EDGAR para holdings completos (EUA) — N-PORT filings, 503 holdings para SPY
- [x] Integrar CVM para ETFs brasileiros — dados abertos cda_fie, 178 holdings para BOVA11
- [x] Expandir lista para ~350 ETFs (EUA + Brasil + Global)
- [x] Implementar fallback chain (SEC → yfinance para EUA; CVM → yfinance para BR)
- [x] Criar rotina de atualização periódica (`--update-stale N`)
- [x] Análise de sobreposição entre ETFs (`--overlap ETF1,ETF2`)
- [x] Flags `--no-sec` e `--no-cvm` para controle de fontes
- [x] Stats com breakdown por fonte e região

### Fase 3 — Interface Flask e Análises
- [ ] **Template** `etf_explorer.html` — página SPA com Bootstrap 5 + Chart.js
  - Barra de busca (por ticker ou nome)
  - Tabela de ETFs com filtros (região, fonte, categoria)
  - Drill-down em ETF → composição com tabela ordenável + gráfico pizza top 10
  - Busca reversa (ex: "AAPL" → lista ETFs que contêm AAPL)
  - Sobreposição visual entre 2 ETFs (Venn/bar chart)
  - Dashboard de stats (cards com contadores, gráficos por região/fonte)
- [ ] **Rotas Flask** em `app.py`:
  - `GET /etfs` → renderiza template
  - `GET /api/etfs` → JSON lista paginada de ETFs com filtros
  - `GET /api/etfs/<ticker>` → JSON detalhes + holdings
  - `GET /api/etfs/search?q=AAPL` → JSON busca reversa
  - `GET /api/etfs/overlap?etf1=SPY&etf2=QQQ` → JSON sobreposição
  - `GET /api/etfs/stats` → JSON estatísticas
- [ ] **Link de navegação** no `main_dashboard.html` (novo app-card ETF Explorer)
- [ ] **Testar** todas as rotas e interações no browser

### Fase 4 — Avançado
- [ ] Histórico de composição (mudanças ao longo do tempo)
- [ ] Integração com WACC (usar composição de ETF para análise setorial)
- [ ] Classificação automática por setor/região/tipo
- [ ] API REST para consulta externa
- [ ] Comparação de ETFs similares

---

## 7. Lista Inicial de ETFs Sugerida

### EUA — Principais por Categoria
| Categoria | ETFs |
|-----------|------|
| Broad Market | SPY, VOO, IVV, VTI, QQQ |
| Setoriais | XLF, XLK, XLE, XLV, XLI, XLC, XLU, XLB, XLP, XLY, XLRE |
| Small Cap | IWM, VB, SCHA |
| International | EFA, VEA, IEFA, VWO, EEM, IEMG |
| Renda Fixa | BND, AGG, TLT, LQD, HYG, TIP |
| Commodities | GLD, SLV, USO, DBC |
| Dividendos | VYM, DVY, SCHD, HDV |
| Crescimento | VUG, IWF, VONG |
| Valor | VTV, IWD, VONV |
| Temáticos | ARKK, ICLN, TAN, BOTZ, HACK |

### Brasil (B3)
| Categoria | ETFs |
|-----------|------|
| Ibovespa | BOVA11, BOVV11 |
| S&P 500 | IVVB11, SPXI11 |
| Small Cap | SMAL11, SMAC11 |
| Renda Fixa | IMAB11, IRFM11, FIXA11 |
| Dividendos | DIVO11 |
| Sustentabilidade | ISUS11 |
| Ouro | GOLD11 |
| Nasdaq | NASD11, QQQM11 |
| Cripto | HASH11, ETHE11, BITH11 |

### Europa
| Categoria | ETFs |
|-----------|------|
| Broad | VWRL, IWDA, CSPX |
| Euro Stoxx | FEZ, EZU |

---

## 8. Considerações Técnicas

### Rate Limiting
- Yahoo Finance: ~2000 req/hora (recomendado 1-2 req/seg)
- SEC EDGAR: 10 req/seg (com User-Agent obrigatório)
- Implementar `time.sleep()` entre requisições
- Usar cache local para evitar re-fetch

### Tratamento de Dados
- Padronizar tickers (remover sufixos de bolsa quando necessário)
- Normalizar pesos (somar 100%)
- Tratar holdings não-equity (bonds, cash, derivativos)
- Mapear tickers entre bolsas (ex: AAPL ↔ AAPL34)

### Armazenamento
- Usar banco existente: `data/damodaran_data_new.db`
- Criar índices em `etf_ticker` e `holding_ticker` para performance
- Manter histórico com `report_date`

### Dependências Adicionais
```
yfinance          # já existente no projeto
requests          # já existente
beautifulsoup4    # já existente (para SEC/CVM parsing)
lxml              # para parsing XML do SEC
```

---

## 9. Métricas de Sucesso

| Métrica | Meta Fase 1 | Meta Final |
|---------|-------------|------------|
| ETFs cadastrados | 200+ | 2000+ |
| Holdings mapeados | 5000+ | 50000+ |
| Cobertura EUA | Top 100 | Top 1000 |
| Cobertura Brasil | Top 20 | Todos listados |
| Atualização | Manual | Automática semanal |

---

## 10. Próximos Passos

1. Validar este plano
2. Decidir se a Fase 1 (MVP com yfinance) atende às necessidades
3. Definir se há ETFs específicos prioritários  
4. Iniciar implementação do `ETFExtractor`
5. Criar script de carga inicial
