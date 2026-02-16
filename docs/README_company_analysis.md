# 🏢 Anloc - Aplicação de Análise de Empresas

## Visão Geral

Esta aplicação web foi desenvolvida para análise e benchmarking de empresas utilizando a base de dados do Damodaran com aproximadamente 47 mil empresas globais. A plataforma permite análises comparativas por país, região e setor, fornecendo insights valiosos para decisões de investimento e valuation.

## Funcionalidades Principais

### 📊 KPIs e Métricas Implementadas

**Métricas Financeiras Fundamentais:**
- Market Cap (Valor de Mercado)
- Enterprise Value (Valor da Empresa)
- Revenue (Receita)
- Net Income (Lucro Líquido)
- EBITDA

**Indicadores de Rentabilidade:**
- ROE (Return on Equity)
- ROA (Return on Assets)
- Operating Margin (Margem Operacional)
- Dividend Yield

**Métricas de Valuation:**
- P/E Ratio (Preço/Lucro)
- Debt/Equity (Dívida/Patrimônio)
- Beta (Risco Sistemático)

**Indicadores de Crescimento:**
- Revenue Growth (Crescimento da Receita)

### 🔍 Funcionalidades de Análise

1. **Filtros Avançados:**
   - Por país (todos os países disponíveis na base)
   - Por setor/indústria
   - Por faixa de Market Cap (mínimo e máximo)

2. **Visualização de Empresas:**
   - Lista paginada com principais métricas
   - Ordenação por Market Cap
   - Limite de 1000 empresas por consulta para performance

3. **Benchmarking:**
   - Estatísticas por setor ou país
   - Métricas: média, mediana, quartis, mín/máx
   - Comparação com pares do mesmo segmento

4. **Análise Detalhada:**
   - Análise individual de empresas
   - Rankings setoriais, nacionais e globais
   - Comparação com benchmarks do setor
   - Percentis de performance

## Estrutura Técnica

### Arquivos Principais

- `company_analysis_app.py` - Aplicação Flask principal
- `templates/company_analysis.html` - Interface web
- `data/damodaran_data_new.db` - Base de dados SQLite

### Rotas da API

- `GET /` - Página principal
- `GET /api/filters` - Opções de filtros (países e setores)
- `GET /api/companies` - Lista de empresas com filtros
- `GET /api/benchmarks` - Benchmarks por grupo
- `GET /api/company/<nome>/analysis` - Análise detalhada

### Classe CompanyAnalyzer

**Métodos Principais:**
- `get_companies_data()` - Extrai dados com filtros
- `calculate_benchmarks()` - Calcula estatísticas por grupo
- `get_company_ranking()` - Determina posição em rankings

## Como Usar

### 1. Iniciar a Aplicação
```bash
python company_analysis_app.py
```

### 2. Acessar a Interface
- URL: http://localhost:5001
- A aplicação roda na porta 5001 para evitar conflitos

### 3. Funcionalidades Disponíveis

**Aba Empresas:**
- Aplicar filtros desejados
- Clicar em "Buscar Empresas"
- Visualizar resultados em tabela
- Clicar em "Analisar" para análise detalhada

**Aba Benchmarks:**
- Escolher agrupamento (setor ou país)
- Aplicar filtros opcionais
- Clicar em "Ver Benchmarks"
- Visualizar estatísticas comparativas

**Aba Análise Detalhada:**
- Digitar nome da empresa
- Clicar em "Analisar"
- Visualizar métricas, rankings e comparações

## Casos de Uso para Valuation

### 1. Análise de Múltiplos
- Comparar P/E ratios por setor
- Identificar empresas sub/sobrevalorizadas
- Benchmarking de múltiplos de receita

### 2. Análise de Rentabilidade
- Comparar ROE e ROA setoriais
- Identificar líderes em eficiência
- Análise de margens operacionais

### 3. Análise de Risco
- Comparar betas por setor
- Análise de alavancagem (D/E)
- Identificação de perfis de risco

### 4. Análise Regional
- Comparar métricas por país
- Identificar oportunidades geográficas
- Análise de mercados emergentes vs desenvolvidos

## Melhorias Futuras Sugeridas

1. **Visualizações Gráficas:**
   - Gráficos de dispersão para correlações
   - Histogramas de distribuição de métricas
   - Gráficos de barras para comparações

2. **Análises Avançadas:**
   - Análise de regressão para múltiplos
   - Correlações entre métricas
   - Análise de tendências temporais

3. **Exportação de Dados:**
   - Export para Excel/CSV
   - Relatórios em PDF
   - APIs para integração

4. **Filtros Adicionais:**
   - Por faixa de receita
   - Por faixa de funcionários
   - Por idade da empresa

## Dependências

```python
flask
pandas
numpy
sqlite3 (built-in)
```

## Notas Técnicas

- Base de dados: SQLite com ~47k empresas
- Performance otimizada com limites de consulta
- Interface responsiva básica
- Tratamento de valores nulos e infinitos
- Formatação automática de números (K, M, B)

Esta aplicação fornece uma base sólida para análises de benchmarking empresarial, sendo facilmente extensível para funcionalidades mais avançadas conforme necessário.