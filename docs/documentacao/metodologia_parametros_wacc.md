# Metodologia – Parâmetros do WACC

## Visão Geral

O **WACC (Weighted Average Cost of Capital)** é a taxa de desconto que reflete o custo médio ponderado de todas as fontes de financiamento de uma empresa. É utilizado para descontar os fluxos de caixa projetados no modelo de Fluxo de Caixa Descontado (FCD/DCF).

### Fórmula do WACC

$$WACC = \frac{E}{E+D} \times K_e + \frac{D}{E+D} \times K_d \times (1 - T)$$

Onde:
- **E** = Capital próprio (Equity)
- **D** = Dívida (Debt)
- **T** = Alíquota efetiva de impostos (IR + CSLL)
- **Ke** = Custo do capital próprio (Cost of Equity)
- **Kd** = Custo da dívida bruto (Cost of Debt)

---

## 1. Custo do Capital Próprio (Ke) — CAPM

O custo do capital próprio é estimado pelo modelo **CAPM (Capital Asset Pricing Model)** adaptado para mercados emergentes:

$$K_e = R_f + \beta_L \times ERP + R_p + R_s$$

---

### 1.1 Taxa Livre de Risco (Rf)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | Rf — Risk-Free Rate |
| **Valor BDWACC** | *Carregado dinamicamente de BDWACC.json (campo RF)* |
| **Fonte** | U.S. Treasury Bond 10 anos (T-Bond 10Y) |
| **Provedor** | Federal Reserve Economic Data (FRED) — St. Louis Fed |
| **Frequência** | Diária (média dos últimos 2 anos) |
| **Metodologia** | Média aritmética das taxas de retorno (yield) dos últimos 2 anos do T-Bond 10Y |
| **Justificativa** | O T-Bond de 10 anos é a melhor estimativa para a taxa livre de risco por: (1) ser título de renda fixa de longo prazo cujo rendimento reflete expectativas futuras, (2) não ser utilizado como instrumento de política monetária (diferente da T-Bill), (3) seu rendimento ser menos afetado por mudanças na inflação esperada que o T-Bond de 30 anos |

**Links de Auditoria:**
- 🔗 [FRED — US Treasury 10Y (DGS10)](https://fred.stlouisfed.org/series/DGS10) — Série histórica completa
- 📥 [Download CSV — FRED DGS10](https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10) — Dados para verificação
- 📄 Arquivo local: `static/BDWACC.json` → campo `RF`

---

### 1.2 Prêmio de Risco de Mercado (ERP)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | ERP — Equity Risk Premium (Rm - Rf) |
| **Valor BDWACC** | *Carregado dinamicamente de BDWACC.json (campo RM)* |
| **Fonte** | Retornos históricos S&P 500 vs T-Bonds (1928–atual) |
| **Provedor** | Prof. Aswath Damodaran — NYU Stern |
| **Frequência** | Anual (atualizado em janeiro) |
| **Metodologia** | **Média Geométrica** da diferença (S&P 500 total return − T-Bond return) desde 1928. A média geométrica é preferida pois reflete retornos compostos reais. |

**⚠️ Nota de Auditoria:**
O valor armazenado (ex: 4,61%) é uma **média geométrica calculada** a partir dos dados anuais. A página de auditoria (histretSP.html) mostra os retornos ano a ano — o valor final não aparece diretamente na página HTML, mas pode ser verificado:
1. Baixando a planilha Excel e calculando a média geométrica da coluna "Stocks - T.Bonds" (1928–2020)
2. Consultando o dataset completo no link Excel abaixo

**⚠️ Abordagem Alternativa (ERP Implícito de Damodaran):**
Damodaran também publica um ERP implícito (forward-looking) baseado no fluxo de caixa esperado do S&P 500. Em janeiro/2026, o ERP implícito para mercados maduros é ~4,23%. Nosso sistema usa a abordagem **histórica** (backward-looking), que é a metodologia mais aceita em laudos de avaliação no Brasil.

**Links de Auditoria:**
- 🔗 [Damodaran — Retornos Históricos S&P (HTML)](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/histretSP.html) — Dados anuais 1928–atual
- 📥 [Download Excel — histretSP.xls](https://www.stern.nyu.edu/~adamodar/pc/datasets/histretSP.xls) — **Verificar média geométrica na aba de resumo**
- 🔗 [Damodaran — ERP Implícito (alternativo)](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/implpr.html) — ERP forward-looking
- 📄 Arquivo local: `static/BDWACC.json` → campo `RM`

---

### 1.3 Beta (β)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | β — Beta alavancado (Levered Beta) |
| **Fonte** | Betas setoriais por grupo de indústria |
| **Provedor** | Prof. Aswath Damodaran — NYU Stern |
| **Frequência** | Anual (atualizado em janeiro) |
| **Metodologia** | O beta é calculado em 2 etapas: (1) obtém-se o **beta desalavancado (βU)** mediano do setor da empresa, (2) re-alavanca-se pelo D/E da empresa: $\beta_L = \beta_U \times [1 + (1-T) \times D/E]$ |
| **Justificativa** | Betas setoriais são mais estáveis e confiáveis que betas de ações individuais, especialmente em mercados emergentes com liquidez limitada |

**Links de Auditoria:**
- 🔗 [Damodaran — Betas por Setor (Global)](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/Betas.html) — Todos os setores, todas as regiões
- 📥 [Download Excel — betas.xls](https://www.stern.nyu.edu/~adamodar/pc/datasets/betas.xls)
- 🔗 [Damodaran — Betas Mercados Emergentes](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/betaemerg.html) — Betas específicos para mercados emergentes
- 📥 [Download Excel — betaemerg.xls](https://www.stern.nyu.edu/~adamodar/pc/datasets/betaemerg.xls)
- 📄 Banco de dados local: `data/damodaran_data_new.db` → tabela `damodaran_global`

---

### 1.4 Risco País (Rp)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | Rp — Country Risk Premium (CRP) |
| **Valor BDWACC** | *Carregado dinamicamente de BDWACC.json (campo CR)* |
| **Fonte** | Spread de default soberano + ajuste de volatilidade relativa equity/bond |
| **Provedor** | Prof. Aswath Damodaran — NYU Stern |
| **Frequência** | Anual (atualizado em janeiro) |
| **Metodologia** | Damodaran estima o CRP em 3 etapas: (1) obtém o spread de default soberano via rating Moody's e CDS spreads, (2) ajusta pela volatilidade relativa do mercado acionário vs mercado de títulos de mercados emergentes (σ equity / σ bond), (3) soma o CRP ajustado ao ERP de mercados maduros. Para o Brasil (rating Ba1), o CRP adicional (Country Risk Premium) é o que é adicionado ao ERP maduro. |

**⚠️ Nota de Auditoria:**
Na página ctryprem.html do Damodaran, o Brasil aparece com:
- Rating Moody's: Ba1
- Country Risk Premium: ~3,24% (valor pode variar com atualização)
- Equity Risk Premium total: ~7,47% (= ERP maduro + CRP)

O valor armazenado em BDWACC.json (campo CR) deve corresponder à coluna "Country Risk Premium" da tabela para o Brasil.

**Links de Auditoria:**
- 🔗 [Damodaran — Country Risk Premiums (HTML)](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html) — **Buscar "Brazil" na tabela**
- 📥 [Download Excel — ctryprem.xlsx](https://www.stern.nyu.edu/~adamodar/pc/datasets/ctryprem.xlsx) — Dataset completo com CDS spreads
- 📄 Banco de dados local: `data/damodaran_data_new.db` → tabela `country_risk`
- 📄 Arquivo local: `static/BDWACC.json` → campo `CR`

---

### 1.5 Prêmio por Tamanho (Rs)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | Rs — Size Premium |
| **Fonte** | Retornos excedentes por decil de Market Cap |
| **Provedor** | Kroll / Duff & Phelps |
| **Frequência** | Anual (input manual) |
| **Metodologia** | O prêmio por tamanho reflete o retorno adicional que investidores exigem para empresas de menor capitalização. É baseado na análise de decis de market cap, onde os menores decis historicamente apresentam retornos superiores ao previsto pelo CAPM. O valor é selecionado conforme o decil de market cap da empresa avaliada. |
| **Justificativa** | Empresas menores têm menor liquidez, maior risco operacional e custos de transação relativamente maiores — exigindo um prêmio na taxa de desconto |

**Links de Auditoria:**
- 🔗 [Kroll Cost of Capital](https://www.kroll.com/en/cost-of-capital) — Publicação anual de referência
- 📄 Arquivo local: `static/BDSize.json` → dados por decil
- 📡 API local: `/api/size_premium_data` — consultar dados carregados

---

## 2. Custo da Dívida (Ki / Kd)

### 2.1 Custo Bruto da Dívida (Kd)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | Kd — Custo da dívida antes de impostos |
| **Valor utilizado** | 150% da Taxa Selic (proxy para custo de captação) |
| **Fonte** | Banco Central do Brasil — Sistema Gerenciador de Séries (SGS) |
| **Série BCB** | 432 (Taxa Selic meta diária) |
| **Frequência** | Diária (atualização automática via API BCB) |
| **Metodologia** | Utiliza-se a taxa Selic multiplicada por 150% como proxy para o custo médio de captação da dívida. Este fator reflete o spread bancário médio sobre a Selic para empresas de médio porte. |

**Links de Auditoria:**
- 📡 [API BCB — Selic últimos 10 dias (JSON)](https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/10?formato=json) — **Dados em tempo real**
- 🔗 [BCB — Página da Taxa Selic](https://www.bcb.gov.br/controleinflacao/taxaselic) — Histórico e decisões COPOM
- 📄 Arquivo local (fallback): `static/BDWACC.json` → campo `CT`

### 2.2 Custo Líquido da Dívida (Ki)

$$K_i = K_d \times (1 - T)$$

| Item | Detalhe |
|------|---------|
| **Parâmetro** | T — Alíquota de IR + CSLL |
| **Valor BDWACC** | *Carregado de BDWACC.json (campo IR)* |
| **Metodologia** | Alíquota combinada de IR (25%) + CSLL (9%) = 34%. Para empresas do Lucro Real. |
| **Justificativa** | A despesa financeira gera benefício fiscal (tax shield), reduzindo o custo efetivo da dívida |

**Links de Auditoria:**
- 🔗 [Receita Federal — IR Pessoa Jurídica](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/tributos/irpj) — Base legal do IR e CSLL
- 📄 Arquivo local: `static/BDWACC.json` → campo `IR`

---

## 3. Estrutura de Capital (D/E)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | D/(D+E) e E/(D+E) — Pesos da dívida e capital próprio |
| **Fonte** | Médias setoriais de D/E por indústria |
| **Provedor** | Prof. Aswath Damodaran — NYU Stern |
| **Metodologia** | Por padrão, utiliza-se a relação D/E mediana do setor da empresa (disponível nos datasets de betas). Opcionalmente, pode-se usar a estrutura de capital real da empresa. |

**Links de Auditoria:**
- 🔗 [Damodaran — Betas por Setor (inclui D/E)](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/Betas.html) — Coluna D/E ratio
- 📥 [Download Excel — betas.xls](https://www.stern.nyu.edu/~adamodar/pc/datasets/betas.xls)

---

## 4. Ajustes de Inflação

### 4.1 Inflação Brasileira (IPCA)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | IB — IPCA acumulado 12 meses |
| **Fonte** | Banco Central do Brasil — SGS série 13522 |
| **Frequência** | Mensal (atualização automática via API BCB) |
| **Metodologia** | Para converter o WACC nominal (em USD) para WACC nominal em BRL, aplica-se o diferencial de inflação |

**Links de Auditoria:**
- 📡 [API BCB — IPCA 12m últimos 12 meses (JSON)](https://api.bcb.gov.br/dados/serie/bcdata.sgs.13522/dados/ultimos/12?formato=json) — **Dados em tempo real**
- 🔗 [BCB — Indicadores de Inflação](https://www.bcb.gov.br/controleinflacao/indicadores)
- 📄 Arquivo local (fallback): `static/BDWACC.json` → campo `IB`

### 4.2 Inflação Americana (CPI)

| Item | Detalhe |
|------|---------|
| **Parâmetro** | IA — CPI médio desde 1820 |
| **Valor BDWACC** | *Carregado de BDWACC.json (campo IA)* |
| **Fonte** | Dados históricos de inflação dos EUA |
| **Metodologia** | Utilizada para converter taxas nominais em USD para taxa real, e depois reconverter para taxa nominal em BRL |

**Links de Auditoria:**
- 🔗 [FRED — CPI All Urban Consumers](https://fred.stlouisfed.org/series/CPIAUCSL) — Série histórica CPI
- 📄 Arquivo local: `static/BDWACC.json` → campo `IA`

---

## 5. Resumo das Fontes

| # | Parâmetro | Componente | Provedor | Atualização | Link de Auditoria |
|---|-----------|-----------|----------|-------------|-------------------|
| 1 | Rf (Taxa Livre de Risco) | Ke | FRED / Fed | Diário | [FRED DGS10](https://fred.stlouisfed.org/series/DGS10) |
| 2 | ERP (Prêmio de Mercado) | Ke | Damodaran | Anual | [histretSP.xls](https://www.stern.nyu.edu/~adamodar/pc/datasets/histretSP.xls) |
| 3 | Beta (β) | Ke | Damodaran | Anual | [Betas.html](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/Betas.html) |
| 4 | Risco País (CRP) | Ke | Damodaran | Anual | [ctryprem.html](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html) |
| 5 | Size Premium (Rs) | Ke | Kroll | Anual | [Kroll](https://www.kroll.com/en/cost-of-capital) |
| 6 | Selic → Kd | Ki | BCB (432) | Diário | [API BCB Selic](https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/10?formato=json) |
| 7 | IR + CSLL | Ki | Legislação | Fixo | [Receita Federal](https://www.gov.br/receitafederal/pt-br) |
| 8 | IPCA | Ajuste | BCB (13522) | Mensal | [API BCB IPCA](https://api.bcb.gov.br/dados/serie/bcdata.sgs.13522/dados/ultimos/12?formato=json) |
| 9 | CPI EUA | Ajuste | FRED | Anual | [FRED CPI](https://fred.stlouisfed.org/series/CPIAUCSL) |
| 10 | D/E Setorial | Estrutura | Damodaran | Anual | [Betas.html](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/Betas.html) |

---

## 6. Notas Importantes

### Hierarquia de Dados (Live → Cache → Fallback)
O sistema busca dados na seguinte ordem:
1. **API em tempo real** (BCB para Selic e IPCA)
2. **Cache local** (dados recentes armazenados por 30 minutos)
3. **BDWACC.json** (valores de referência atualizados anualmente)
4. **Fallbacks hardcoded** (valores absolutos de último recurso)

### Verificação de Integridade
- Acesse `/data-updates` para ver o status de todas as fontes
- Acesse `/api/get_wacc_all_live` para obter todos os valores atuais com indicação de fonte
- Acesse `/api/validate_wacc_data` para validar disponibilidade dos dados
- Acesse `/parametros-wacc` para visualizar esta documentação com valores dinâmicos

---

*Documento gerado como referência metodológica. Para valores atualizados em tempo real, consulte a página `/parametros-wacc`.*

*Referência bibliográfica principal: Damodaran, A. "Valuation: Measuring and Managing the Value of Companies". Wiley.*
