# Insights — Estudo Anloc de Múltiplos Setoriais

## Visão Geral

O módulo **Estudo Anloc** (`/estudoanloc`) permite analisar e comparar múltiplos de valuation (EV/EBITDA, EV/Revenue, FCF/Revenue, FCF/EBITDA) por setor, indústria e geografia, com dados históricos e atuais.

---

## Layout Atual (v2 — Março 2026)

### 1. Filtros
- Setor, Ano Fiscal, EV Mínimo, Teto EV/EBITDA
- TTM fallback, EBITDA positivo
- Seleção de indústrias específicas
- Anos para evolução (multi-seleção)

### 2. Tabela Analítica (aba "Resumo" + "Base Analítica")
- **Resumo**: Tabelas consolidadas — Geográfico (Global/LATAM/Brasil), Por Indústria, Por Região
- **Base Analítica**: Tabela completa de todas empresas × períodos, com ordenação, busca, seleção de colunas e exportação CSV

### 3. Infográficos (aba "Infográficos")
- Múltiplos por Indústria (bar chart horizontal)
- Múltiplos por Região (bar chart horizontal)
- Distribuição P25/Mediana/P75 por Indústria (stacked bar)
- Top 20 Empresas por múltiplo (bar chart com cor por geografia)
- Seletor de métrica (EV/EBITDA, EV/Vendas, FCF/Vendas, FCF/EBITDA)

### 4. Evolução (aba "Evolução")
- Gráficos de linha (Global, LATAM, Brasil) para cada múltiplo
- Tabelas de evolução com variação percentual (Δ)
- Tabela de cobertura (n empresas por ano)

---

## Plano Futuro: Insights Automatizados

### Fase 5 — Insights com Agente de Análise

**Objetivo**: Com base nos dados selecionados (setor, indústria, geografia), gerar insights automatizados que expliquem os múltiplos observados cruzando com informações da web.

#### 5.1 Arquitetura Proposta

```
┌──────────────────────────────────────────────┐
│           Frontend (estudoanloc.html)         │
│  ┌────────────────────────────────────────┐   │
│  │  Aba "Insights"                        │   │
│  │  - Relatório gerado por AI             │   │
│  │  - Explicações dos múltiplos           │   │
│  │  - Comparações setoriais               │   │
│  │  - Alertas e anomalias                 │   │
│  └──────────────┬─────────────────────────┘   │
└─────────────────┼─────────────────────────────┘
                  │ POST /api/estudoanloc/insights
                  ▼
┌──────────────────────────────────────────────┐
│           Backend (app.py)                    │
│  ┌────────────────────────────────────────┐   │
│  │  InsightsEngine                        │   │
│  │  - Recebe: setor, indústria, filtros   │   │
│  │  - Prepara contexto dos dados          │   │
│  │  - Chama AI Agent (LLM)               │   │
│  │  - Retorna insights formatados         │   │
│  └──────────────┬─────────────────────────┘   │
└─────────────────┼─────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
┌──────────────┐   ┌──────────────────┐
│  LLM API     │   │  Web Search API  │
│  (OpenAI /   │   │  (Serper / Bing  │
│   Gemini /   │   │   / Google)      │
│   Claude)    │   │                  │
└──────────────┘   └──────────────────┘
```

#### 5.2 Tipos de Insights a Gerar

| Categoria | Descrição | Exemplo |
|-----------|-----------|---------|
| **Contexto Setorial** | Panorama do setor e fatores macro | "O setor de Utilities apresenta EV/EBITDA 9.2x, acima da média histórica de 8.5x, refletindo a transição energética e investimentos em renováveis" |
| **Anomalias** | Múltiplos fora do range esperado | "A indústria de Water Utilities tem EV/EBITDA 35x, significativamente acima do setor — possível reflexo de escassez hídrica e regulação ESG" |
| **Comparação Geo** | Diferenças entre regiões | "Brasil opera com desconto de 25% vs Global em EV/EBITDA — risco-país e taxa de juros elevada" |
| **Evolução** | Tendências temporais | "EV/Revenue caiu de 3.2x para 2.1x entre 2022-2025 — correlação com aumento de Selic e fuga de capital" |
| **Empresas Destaque** | Empresas outliers | "TSLA (EV/EBITDA 45x) é outlier vs mediana auto 8x — prêmio por growth e margem de software" |

#### 5.3 Implementação — Fases

**Fase 5.1 — Geração de Insights Local (sem API externa)**
- Regras baseadas em heurísticas:
  - Detectar outliers (>2σ da mediana)
  - Comparar Brasil vs Global (desconto/prêmio)
  - Detectar tendências na evolução (crescente/decrescente)
  - Identificar indústrias com maior dispersão (P75-P25)
- Sem custo de API, funciona offline

**Fase 5.2 — Integração com LLM API**
- Endpoint: `POST /api/estudoanloc/insights`
- Payload: dados calculados + contexto
- Prompt engineering: enviar tabela resumo + pedir análise
- Providers: OpenAI GPT-4o, Google Gemini, Anthropic Claude
- Configurável via variável de ambiente (`INSIGHTS_LLM_PROVIDER`)

**Fase 5.3 — Web Search + LLM (RAG)**
- Buscar notícias recentes sobre o setor/indústria
- APIs de busca: Google Custom Search, Serper.dev, Bing Search
- Combinar dados financeiros + notícias no prompt
- Gerar insights contextualizados com informações atuais

#### 5.4 Configurações Necessárias

```python
# Variáveis de ambiente
INSIGHTS_LLM_PROVIDER = "openai"  # openai | gemini | claude
INSIGHTS_LLM_API_KEY = "sk-..."
INSIGHTS_LLM_MODEL = "gpt-4o"
INSIGHTS_WEB_SEARCH_PROVIDER = "serper"  # serper | google | bing
INSIGHTS_WEB_SEARCH_API_KEY = "..."
```

#### 5.5 Estimativa de Custos (por requisição)

| Componente | Custo Estimado |
|-----------|---------------|
| LLM (GPT-4o, ~2K tokens) | ~$0.02 |
| Web Search (10 results) | ~$0.005 |
| **Total por insight** | **~$0.025** |

---

## Melhorias Futuras Adicionais

- [ ] **Export PDF**: Gerar relatório em PDF com gráficos e insights
- [ ] **Comparação entre setores**: Selecionar 2+ setores e comparar lado a lado
- [ ] **Benchmark empresa vs setor**: Posicionar uma empresa específica vs peers
- [ ] **Alertas de múltiplos**: Notificar quando múltiplos cruzam thresholds
- [ ] **Cache de insights**: Armazenar insights gerados para evitar custos redundantes
- [ ] **Histórico de estudos**: Salvar configurações de filtros e resultados
