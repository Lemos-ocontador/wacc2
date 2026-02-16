# Análise dos Dados Disponíveis e Sugestões para Conexão com a Calculadora WACC

## 📊 Resumo dos Dados Disponíveis

Baseado na análise das bases de dados, temos os seguintes componentes disponíveis:

### 1. **Taxa Livre de Risco (Risk-Free Rate)**

**Dados Disponíveis:**
- ✅ **US Treasury 10Y**: 4,14% (2024) - Fonte: FRED API
- ✅ **US Treasury 30Y**: Disponível via web scraping
- ✅ **Dados históricos**: Desde 1928

**Sugestões de Implementação:**
```python
# Opções para seleção do usuário:
opcoes_rf = {
    "treasury_10y": "US Treasury 10 anos (padrão)",
    "treasury_30y": "US Treasury 30 anos",
    "custom": "Taxa personalizada"
}
```

**Conexão com a Calculadora:**
- Criar dropdown com opções de prazo (10Y, 30Y)
- Busca automática via FRED API
- Fallback para web scraping se API falhar
- Opção de entrada manual para casos específicos

---

### 2. **Beta (β) - COMPONENTE MAIS COMPLEXO**

**Dados Disponíveis:**
- ✅ **47.698 empresas globais** com dados de beta
- ✅ **Betas por setor**: 15+ setores principais
- ✅ **Dados por país**: Incluindo empresas brasileiras
- ✅ **Betas desalavancados**: Calculáveis com D/E ratio

**Principais Setores Disponíveis:**
- Banks: 881 empresas, Beta médio: 0.293
- Technology: 1.702 empresas, Beta médio: 1.156
- Retail: 2.571 empresas, Beta médio: 0.742
- Oil/Gas: 800 empresas, Beta médio: 0.875
- Real Estate: 1.968 empresas, Beta médio: 0.500

**Sugestões de Implementação:**

#### Opção 1: Seleção por Benchmark (A IMPLEMENTAR)
```python
# Interface proposta:
beta_options = {
    "sector_global": "Beta setorial global (Damodaran)",
    "sector_emkt": "Beta setorial mercados emergentes",
    "benchmark_company": "Benchmark - empresa específica",
    "custom": "Beta personalizado"
}
```

#### Opção 2: Global vs EMKT (DISPONÍVEL AGORA)
```python
# Implementação imediata possível:
def get_sector_beta(sector, region="global"):
    if region == "global":
        # Usar todos os dados da tabela damodaran_global
        query = f"SELECT AVG(beta) FROM damodaran_global WHERE industry LIKE '%{sector}%'"
    elif region == "emkt":
        # Filtrar apenas mercados emergentes
        emerging_markets = ['Brazil', 'Mexico', 'Argentina', 'Chile', 'India', 'China']
        query = f"SELECT AVG(beta) FROM damodaran_global WHERE industry LIKE '%{sector}%' AND country IN ({emerging_markets})"
    
    return execute_query(query)
```

**Conexão com a Calculadora:**
1. **Dropdown de Setor**: Lista dos 15+ setores disponíveis
2. **Radio Button**: Global vs Mercados Emergentes
3. **Cálculo Automático**: Beta desalavancado usando fórmula: `β_unlevered = β_levered / (1 + (1-tax_rate) * D/E)`
4. **Fallback**: Se setor não encontrado, usar beta médio do mercado (1.0)

---

### 3. **Prêmio de Risco de Mercado (Market Risk Premium)**

**Dados Disponíveis:**
- ✅ **ERP US**: 4,61% (média geométrica 1928-2020)
- ✅ **Dados Damodaran**: Atualizados anualmente
- ✅ **Dados históricos**: Desde 1928

**Sugestões de Implementação:**
```python
market_risk_options = {
    "damodaran_us": "ERP US (Damodaran) - 4,61%",
    "damodaran_implied": "ERP Implícito (Damodaran)",
    "custom": "Prêmio personalizado"
}
```

**Conexão com a Calculadora:**
- Valor padrão: 4,61% (dados Damodaran)
- Atualização automática anual
- Opção de entrada manual

---

### 4. **Prêmio de Risco País (Country Risk Premium)**

**Dados Disponíveis:**
- ✅ **192 países** com dados de risco
- ✅ **Brasil**: 7,67% (dados atualizados)
- ✅ **Países principais**: Argentina (20,35%), Venezuela (27,92%), etc.

**Sugestões de Implementação:**
```python
# Implementação direta disponível:
def get_country_risk(country):
    query = f"SELECT risk_premium FROM country_risk WHERE country LIKE '%{country}%'"
    return execute_query(query)

# Principais países para dropdown:
principais_paises = [
    "Brazil", "United States", "Argentina", "Chile", 
    "Colombia", "Mexico", "Peru", "Germany", "China"
]
```

**Conexão com a Calculadora:**
- Dropdown com 192 países disponíveis
- Busca automática por país selecionado
- Valor padrão: Brasil (7,67%)
- Atualização automática dos dados Damodaran

---

### 5. **Outros Componentes WACC**

**Dados Disponíveis:**
- ✅ **Taxa de Imposto**: 34% (padrão Brasil)
- ✅ **Custo da Dívida**: 13,50% (150% da Selic)
- ✅ **Inflação**: US 2,03%, Brasil 3,50%

---

## 🔧 Implementação Sugerida na Calculadora

### Interface do Usuário Proposta:

```html
<!-- Taxa Livre de Risco -->
<div class="form-group">
    <label>Taxa Livre de Risco</label>
    <select id="rf-source">
        <option value="treasury_10y">US Treasury 10Y (Automático)</option>
        <option value="treasury_30y">US Treasury 30Y (Automático)</option>
        <option value="custom">Personalizada</option>
    </select>
    <input type="number" id="rf-custom" style="display:none" placeholder="%">
</div>

<!-- Beta -->
<div class="form-group">
    <label>Beta</label>
    <div>
        <input type="radio" name="beta-type" value="sector" checked> Beta Setorial
        <input type="radio" name="beta-type" value="benchmark"> Benchmark (A implementar)
        <input type="radio" name="beta-type" value="custom"> Personalizado
    </div>
    
    <div id="beta-sector-options">
        <select id="sector">
            <option value="Banks">Bancos</option>
            <option value="Technology">Tecnologia</option>
            <option value="Retail">Varejo</option>
            <!-- ... outros setores -->
        </select>
        
        <div>
            <input type="radio" name="beta-region" value="global" checked> Global
            <input type="radio" name="beta-region" value="emkt"> Mercados Emergentes
        </div>
    </div>
    
    <input type="number" id="beta-custom" style="display:none" placeholder="Beta">
</div>

<!-- Prêmio de Risco País -->
<div class="form-group">
    <label>Prêmio de Risco País</label>
    <select id="country">
        <option value="Brazil">Brasil (7,67%)</option>
        <option value="United States">Estados Unidos</option>
        <option value="Argentina">Argentina (20,35%)</option>
        <!-- ... outros países -->
    </select>
</div>
```

### APIs Necessárias:

```python
# Novas rotas para app.py

@app.route('/api/get_sector_beta/<sector>/<region>')
def get_sector_beta(sector, region):
    # Buscar beta do setor na base de dados
    pass

@app.route('/api/get_country_risk/<country>')
def get_country_risk(country):
    # Buscar risco país na base de dados
    pass

@app.route('/api/get_risk_free_rate/<term>')
def get_risk_free_rate(term):
    # Buscar taxa livre de risco via FRED API
    pass

@app.route('/api/calculate_unlevered_beta')
def calculate_unlevered_beta():
    # Calcular beta desalavancado
    pass
```

---

## 📈 Prioridades de Implementação

### **Fase 1 - Implementação Imediata (Dados Disponíveis)**
1. ✅ **Prêmio de Risco País**: Dropdown com 157 países (principais + outros) — IMPLEMENTADO
2. ✅ **Beta Setorial Global**: 90+ indústrias Damodaran com D/E médio — IMPLEMENTADO
3. ✅ **Taxa Livre de Risco**: Integração com FRED API/Damodaran — IMPLEMENTADO
4. ✅ **Prêmio de Risco de Mercado**: Dados Damodaran ERP — IMPLEMENTADO
5. ✅ **Prêmio de Tamanho (Size Premium)**: 13 decis com faixas de Market Cap — IMPLEMENTADO (Fev/2026)

### **Fase 2 - Melhorias (Médio Prazo)**
1. ✅ **Beta por Mercados Emergentes**: Filtrar dados por região (Global/EMKT) — IMPLEMENTADO
2. ✅ **Beta Desalavancado**: Cálculo automático com D/E ratio — IMPLEMENTADO
3. 🔄 **Histórico de Dados**: Gráficos de evolução temporal
4. 🔄 **Validação de Dados**: Alertas para dados desatualizados
5. 🔄 **Atualização Automática**: Script unificado de atualização (planejado)

### **Fase 3 - Funcionalidades Avançadas (Longo Prazo)**
1. 🆕 **Benchmark por Empresa**: Seleção de empresa específica como benchmark
2. 🆕 **Beta Ajustado**: Ajustes por liquidez, tamanho, etc.
3. 🆕 **Cenários**: Análise de sensibilidade com diferentes parâmetros
4. 🆕 **API Externa**: Integração com Bloomberg, Reuters, etc.
5. 🆕 **GitHub Actions**: Atualização automática de FRED/BCB via workflow CRON

---

## 🎯 Recomendações Finais

1. **Começar com dados disponíveis**: Implementar primeiro os componentes que já temos dados robustos
2. **Interface progressiva**: Começar simples e adicionar complexidade gradualmente
3. **Fallbacks**: Sempre ter valores padrão quando dados não estão disponíveis
4. **Documentação**: Explicar a fonte e metodologia de cada componente
5. **Atualização automática**: Implementar rotinas para manter dados atualizados

A base de dados atual já permite implementar uma calculadora WACC robusta e completa. O foco deve ser na experiência do usuário e na confiabilidade dos cálculos.