# Oportunidades de Melhorias

> Análise realizada em Março/2026

---

## Prioridade 1 — Segurança (Imediata)

### 1.1 Remover `debug=True` e `host='0.0.0.0'`
- **Arquivo**: `app.py` (final do arquivo)
- **Risco**: O debugger interativo do Werkzeug expõe execução remota de código para qualquer IP na rede
- **Ação**: Usar variáveis de ambiente (`FLASK_DEBUG`, `FLASK_HOST`) ou mover para arquivo de configuração

### 1.2 SECRET_KEY previsível
- **Arquivo**: `app.py` (linha ~35)
- **Risco**: Chave `'wacc-calculator-2025'` é hardcoded e previsível — session cookies podem ser forjados
- **Ação**: Gerar chave aleatória e armazenar em variável de ambiente

### 1.3 SQL Injection via interpolação de colunas
- **Arquivos**: `app.py` (~6 pontos), `company_analysis_app.py` (linha ~28)
- **Risco**: Nomes de colunas interpolados em f-strings SQL. Embora mitigado por whitelists em alguns casos, o padrão é perigoso
- **Ação**: Garantir whitelist explícita em TODOS os pontos de interpolação de colunas

### 1.4 Exposição de erros internos
- **Arquivo**: `app.py` (30+ endpoints)
- **Risco**: `str(e)` retornado ao cliente expõe stack traces, caminhos e detalhes internos
- **Ação**: Logar erro internamente, retornar mensagem genérica ao usuário

---

## Prioridade 2 — Estabilidade

### 2.1 Conexões SQLite sem `with` statement
- **Arquivo**: `app.py` (~36 chamadas `conn.close()`)
- **Risco**: Se exceção ocorre antes do `conn.close()`, a conexão vaza
- **Ação**: Substituir por `with sqlite3.connect(...) as conn:` em todos os endpoints

### 2.2 Eliminar código duplicado: `company_analysis_app.py`
- **Arquivos**: `app.py` + `company_analysis_app.py`
- **Problema**: Classe `CompanyAnalyzer` e rotas de análise existem em ambos os arquivos
- **Ação**: Consolidar em `app.py` (ou melhor, em um Blueprint) e remover `company_analysis_app.py`

### 2.3 Sanitizar uso de `innerHTML` nos templates
- **Arquivos**: `dashboard.html`, `company_analysis.html` (~20+ ocorrências)
- **Risco**: XSS se dados da API contiverem HTML malicioso
- **Ação**: Usar `textContent` ou sanitizar dados antes de inserir no DOM

### 2.4 Tratamento inconsistente de erros
- **Arquivo**: `app.py`
- **Problema**: Alguns endpoints retornam `{'error': str(e)}`, outros `{'success': False, 'error': str(e)}`, nem todos com status code correto
- **Ação**: Padronizar formato de resposta de erro em todos os endpoints

---

## Prioridade 3 — Manutenibilidade e Arquitetura

### 3.1 Refatorar `app.py` em Flask Blueprints
- **Problema**: 3.740 linhas e 80 rotas num único arquivo — difícil de navegar e manter
- **Proposta de estrutura**:
  ```
  blueprints/
  ├── wacc.py           # Cálculo WACC (23 rotas)
  ├── companies.py      # Análise de empresas (7 rotas)
  ├── yahoo.py          # Dashboard + drill-down Yahoo (12 rotas)
  ├── historico.py      # Dados históricos (9 rotas)
  ├── sector.py         # Análise por setor (3 rotas)
  ├── export.py         # Exportação de dados (2 rotas)
  └── data_sources.py   # Gestão de fontes (4 rotas)
  ```

### 3.2 Extrair JS/CSS dos templates
- **Problema**: Templates com 1.900 a 2.800 linhas cada, com CSS e JS inline
- **Ação**: Mover para `static/js/` e `static/css/` — bundles por página ou compartilhados
- **Benefício**: Cache do browser, melhor performance, mais fácil de manter

### 3.3 Criar camada de acesso a dados (DAO)
- **Problema**: Queries SQL repetidas em dezenas de endpoints, padrão `connect → query → close` duplicado 36+ vezes
- **Ação**: Criar módulo `db.py` com funções reutilizáveis (ex: `get_companies()`, `get_sectors()`)
- **Benefício**: Eliminação de código duplicado, queries centralizadas, mais fácil de testar

### 3.4 Adicionar testes
- **Problema**: Zero testes para 3.740 linhas de backend + 17k linhas de templates
- **Ação**: Criar `tests/` com:
  - Testes unitários para `wacc_calculator.py`
  - Testes de API para os principais endpoints
  - Testes de integração para ETL scripts

---

## Prioridade 4 — Performance

### 4.1 Implementar cache para endpoints pesados
- **Endpoints**: `/api/yahoo_dashboard_summary`, `/api/historico/consolidated`
- **Problema**: Cada request executa agregações sobre 47k+ registros
- **Ação**: Flask-Caching com TTL de 5-15min para endpoints de dashboard

### 4.2 Otimizar queries N+1
- **Arquivo**: `app.py` (ex: subquery `EXISTS(SELECT 1 FROM company_financials_historical ...)`)
- **Problema**: Subqueries correlacionadas executam para cada linha
- **Ação**: Usar JOINs ou CTEs em vez de EXISTS em subqueries repetitivas

### 4.3 Cache nos extractors com persistência
- **Arquivo**: `data_extractors/base_extractor.py`
- **Problema**: Cache em memória (dict) perdido a cada restart
- **Ação**: Adicionar cache de segundo nível em disco (JSON/SQLite) com TTL

---

## Prioridade 5 — Evolução e Features

### 5.1 Autenticação e autorização
- **Problema**: Zero proteção — qualquer pessoa na rede acessa dados financeiros de 47k empresas
- **Ação**: Flask-Login ou JWT para rotas sensíveis

### 5.2 Rate limiting
- **Problema**: APIs podem ser abusadas com scraping em massa
- **Ação**: Flask-Limiter com limites por IP

### 5.3 Documentação da API
- **Problema**: 80 endpoints sem documentação formal
- **Ação**: Swagger/OpenAPI via Flask-RESTX ou flasgger

### 5.4 Validação de input
- **Problema**: Poucos endpoints validam tipos/limites dos parâmetros de entrada
- **Ação**: Marshmallow ou Pydantic para validação de schemas

### 5.5 Limpar dependências
- **Arquivo**: `requirements.txt`
- **Dependências possivelmente não usadas**:
  - `selenium` — provavelmente só usado em scripts pontuais
  - `wikipedia` — uso questionável em app financeiro
  - `peewee` — ORM instalado mas código usa `sqlite3` direto em todo lugar

### 5.6 Migração de schema gerenciada
- **Problema**: Scripts manuais em `scripts/` sem controle de versão de schema
- **Ação**: Alembic ou sistema simples de migrations versionadas

---

## Resumo Executivo

| Prioridade | Área | Itens | Esforço |
|------------|------|-------|---------|
| **P1** | Segurança | 4 itens | Baixo (horas) |
| **P2** | Estabilidade | 4 itens | Médio (1-2 dias) |
| **P3** | Arquitetura | 4 itens | Alto (1-2 semanas) |
| **P4** | Performance | 3 itens | Médio (2-3 dias) |
| **P5** | Evolução | 6 itens | Alto (contínuo) |

**Recomendação**: Resolver P1 imediatamente (segurança), P2 na próxima sprint (estabilidade), e planejar P3 como refatoração incremental. P4 e P5 conforme necessidade.
