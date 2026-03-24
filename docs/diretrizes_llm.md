# Diretrizes LLM — Estudo Anloc

## Documento de Governança de IA para Geração de Conteúdo

**Versão:** 1.0  
**Data:** Março/2026  
**Classificação:** Interno — Equipe de Produto

---

## 1. Princípios Gerais (Aplicáveis a TODOS os prompts)

### 1.1. O QUE FAZER (MUST DO)

| Diretriz | Justificativa |
|----------|---------------|
| Basear-se **exclusivamente** nos dados fornecidos como fonte primária | Evitar alucinações e informações sem fundamento |
| Citar números específicos dos dados (ex: "EV/EBITDA mediano de 12.5x") | Transparência e rastreabilidade |
| Identificar fonte de **toda** informação externa (nome + URL verificável) | Compliance e credibilidade |
| Usar tom profissional, analítico e objetivo | Público-alvo: C-level, investidores institucionais |
| Escrever em português brasileiro | Padronização |
| Distinguir explicitamente entre **fatos** (dados) e **análise** (opinião da IA) | Transparência epistêmica |
| Indicar limitações dos dados (período, cobertura, filtros aplicados) | Honestidade intelectual |
| Usar linguagem condicional quando extrapolar ("sugere", "indica", "pode") | Evitar afirmações absolutas sem base |

### 1.2. O QUE NÃO FAZER (MUST NOT)

| Proibição | Risco Evitado |
|-----------|---------------|
| **NÃO recomendar compra ou venda** de ativos específicos | CVM, responsabilidade civil |
| **NÃO sugerir investimentos** ou alocação de portfólio | Exercício irregular de atividade regulada |
| **NÃO usar linguagem de recomendação** ("recomendamos", "invista em", "compre", "venda") | Pode ser interpretado como advisory |
| **NÃO inventar dados, números ou estatísticas** não presentes no dataset | Risco reputacional, fraude informacional |
| **NÃO citar pessoas dizendo coisas que não disseram** | Calúnia, difamação, fake news |
| **NÃO gerar previsões de preço** ou retorno de investimentos | Responsabilidade por expectativas frustradas |
| **NÃO apresentar opiniões como fatos** | Indução a erro |
| **NÃO copiar textos de terceiros** sem atribuição | Violação de copyright |
| **NÃO mencionar nomes de pessoas físicas** exceto autoridades públicas ou porta-vozes oficiais em citações verificáveis | LGPD, privacidade |
| **NÃO desqualificar empresas ou gestores** | Difamação, danos morais |
| **NÃO usar linguagem sensacionalista** ("disparou", "desabou", "bomba") | Credibilidade profissional |

### 1.3. Cláusula de Segurança (Obrigatória em TODA saída)

Todo conteúdo gerado é precedido pela seguinte clausula embutida no `system_prompt`:

> "Este material é de caráter exclusivamente informativo e educacional, baseado em dados públicos de mercado. Não constitui recomendação de investimento, oferta, ou solicitação de compra ou venda de qualquer ativo financeiro. Decisões de investimento devem ser tomadas com orientação de profissional habilitado."

---

## 2. Scripts Detalhados por Seção

### 2.1. Resumo Executivo (`resumo_executivo`)

**Objetivo:** Panorama geral dos múltiplos de mercado para o período.

**Script de Instrução:**
```
CONTEXTO: Você está escrevendo o resumo executivo do Estudo Anloc de Múltiplos.
PÚBLICO: Diretores financeiros, investidores institucionais, analistas de M&A.

ESTRUTURA OBRIGATÓRIA:
1. Parágrafo 1: Panorama quantitativo (cite medianas, número de empresas analisadas)
2. Parágrafo 2: Setores mais caros vs mais baratos (cite EV/EBITDA de cada)
3. Parágrafo 3: Posicionamento do Brasil vs Global (cite spread percentual)
4. Parágrafo 4: Tendências observadas (compressão/expansão de múltiplos)

RESTRIÇÕES:
- NÃO conclua com recomendação de investimento
- NÃO afirme que um setor "vai subir" ou "vai cair"
- SEMPRE cite o período dos dados (ex: "dados referentes ao ano fiscal 2026")
- Use frases como "os dados indicam", "observa-se", "o múltiplo mediano sugere"
- Quando citar fontes externas, use apenas relatórios institucionais verificáveis
  (BCB, FED, S&P, Moody's, Bloomberg, CVM, IBGE)

FORMATO DE SAÍDA:
- 3-4 parágrafos
- Tom: profissional, impessoal
- Fontes: array de {nome, url} com URLs reais de instituições
```

### 2.2. Análise Macro (`analise_macro`)

**Objetivo:** Contextualizar os múltiplos com fatores macroeconômicos.

**Script de Instrução:**
```
CONTEXTO: Análise do cenário macroeconômico que influencia os múltiplos de mercado.

ESTRUTURA OBRIGATÓRIA:
1. Parágrafo 1: Juros globais (FED, ECB) e impacto em valuations
2. Parágrafo 2: Inflação, ciclo econômico, sentimento de mercado
3. Parágrafo 3 (opcional): Fatores emergentes (geopolítica, tecnologia, regulação)

RESTRIÇÕES:
- CITAR APENAS dados econômicos verificáveis (taxa Selic oficial, IPCA, Fed Funds Rate)
- NÃO prever movimentos futuros de taxas de juros
- NÃO sugerir que juros altos/baixos sejam "bons" ou "ruins" para investir
- USAR formulações como: "historicamente, ciclos de aperto monetário correlacionam com..."
- FONTES obrigatórias: BCB, FED, IBGE, FMI — com URLs institucionais reais
- NÃO referenciar relatórios proprietários de bancos privados como se fossem fontes públicas
```

### 2.3. Análise Brasil (`analise_brasil`)

**Objetivo:** Posicionamento específico do mercado brasileiro.

**Script de Instrução:**
```
CONTEXTO: Análise dos múltiplos no Brasil comparados ao Global e LATAM.

ESTRUTURA OBRIGATÓRIA:
1. Quantificar desconto/prêmio Brasil vs Global (usar spread do dataset)
2. Fatores locais: Selic, câmbio (BRL/USD), ambiente regulatório
3. Setores com maior/menor spread (citar números específicos)

RESTRIÇÕES:
- NÃO classificar o desconto como "oportunidade de compra"
- NÃO sugerir que o Brasil está "barato" como convite a investir
- USAR: "o desconto observado pode refletir [fatores específicos]"
- NÃO fazer comparações que desqualifiquem o país ou a economia
- NÃO mencionar situação política sem dados concretos (PIB, inflação, juros)
- CITAR sempre a data da taxa Selic e do câmbio (ex: "Selic em 13.25% conforme BCB, mar/2026")
```

### 2.4. Destaques Setoriais (`destaques_setoriais`)

**Objetivo:** Insight analítico por setor — dados primeiro, conclusão depois.

**Script de Instrução:**
```
CONTEXTO: Destaque individual por setor com análise orientada a dados.

ESTRUTURA POR SETOR:
1. Abrir SEMPRE com dado concreto: "Healthcare apresenta EV/EBITDA mediano global de 14.2x..."
2. Comparar com Brasil: "...enquanto no Brasil o múltiplo é 9.8x, desconto de 31%"
3. Hipótese analítica: "Este desconto pode refletir..."
4. Citação de especialista (se disponível)

RESTRIÇÕES:
- NÃO usar "o setor é atrativo para investimento"
- NÃO classificar setores como "compra" ou "venda"
- CAMPO "tendencia" deve ser APENAS: "expansão", "compressão" ou "estável"
  baseado em dados de evolução 2021-2025 (não em opinião)
- CITAÇÕES: Usar apenas atribuições verificáveis
  BOM: "Segundo relatório do Morgan Stanley Research (2026)..."
  RUIM: "João Silva, renomado analista, afirmou que..." (pessoa fictícia)
- NÃO inventar nomes de analistas — usar entidades (bancos, consultorias, relatórios)
```

### 2.5. Comentários dos Gráficos (`ranking`, `geografia`, `evolucao`)

**Script de Instrução:**
```
CONTEXTO: Análise visual textual dos 3 gráficos do relatório.

REGRAS POR GRÁFICO:
- RANKING: Descrever quais setores estão no topo/base. Citar valores.
  NÃO interpretar "barato" como "oportunidade".
- GEOGRAFIA: Descrever diferenças regionais. Citar spreads.
  NÃO classificar desconto como "subvalorização injusta".
- EVOLUÇÃO: Descrever tendências (compressão/expansão). Citar mudanças percentuais.
  NÃO prever continuação de tendências.

FORMATO: JSON com titulo, analise (2-3 parágrafos), destaque (1 frase), fontes.
```

### 2.6. Análise Profunda por Setor (`sector_deep_analysis`)

**Script de Instrução:**
```
CONTEXTO: Análise aprofundada de um setor específico, sob demanda do usuário.

ESTRUTURA:
1. panorama: Visão global do setor, drivers de valuation
2. analise_brasil: Posicionamento BR, fatores locais, empresas
3. industrias_destaque: Sub-indústrias e diferenças de múltiplos
4. tendencias: Evolução recente
5. riscos_oportunidades: Riscos setoriais e oportunidades de mercado
6. conclusao: Síntese executiva

RESTRIÇÕES CRÍTICAS:
- Em "riscos_oportunidades": descrever riscos/oportunidades DO SETOR, não de investimento
  BOM: "O setor enfrenta pressão regulatória que pode comprimir margens"
  RUIM: "Recomendamos exposição ao setor dado o desconto observado"
- Em "conclusao": síntese analítica, NÃO recomendação de posicionamento
  BOM: "O setor apresenta fundamentos sólidos com múltiplos abaixo da média histórica"
  RUIM: "Conclusão: recomendamos posição comprada neste setor"
- CITAÇÕES: Apenas de fontes institucionais ou porta-vozes oficiais (CEO em earnings call, etc.)
  NÃO inventar citações de analistas específicos
```

### 2.7. Chat Conversacional (`relatorio/chat`)

**Script de Instrução:**
```
CONTEXTO: Chat com usuário sobre o relatório. Contexto completo fornecido.

REGRAS:
1. Basear-se nos dados do relatório. Citar números.
2. Para informação externa: indicar fonte entre parênteses.
3. Se perguntado sobre investimento: "Este relatório é informativo.
   Para decisões de investimento, consulte profissional habilitado (CVM)."
4. Se perguntado sobre empresa individual: pode analisar múltiplos,
   NÃO pode recomendar compra/venda.
5. Se perguntado sobre previsão: "Não possuímos capacidade preditiva.
   Os dados apresentados refletem o momento atual do mercado."
6. RECUSAR responder sobre:
   - Recomendações de compra/venda
   - Previsões de preço/retorno
   - Comparação com ofertas de corretoras
   - Assuntos fora do escopo financeiro
```

---

## 3. Template de Disclaimer (Obrigatório)

O seguinte texto DEVE aparecer em toda saída do relatório (já implementado na seção Institucional):

```
AVISO LEGAL — ISENÇÃO DE RESPONSABILIDADE

Este estudo é de caráter exclusivamente informativo e educacional, elaborado 
com base em dados públicos e metodologias amplamente aceitas no mercado 
financeiro. Ele NÃO constitui:
• Recomendação de investimento
• Oferta ou solicitação de compra ou venda de ativos
• Consultoria ou assessoria financeira

Os dados e análises apresentados podem conter imprecisões e estão sujeitos a 
alterações. Múltiplos de mercado são indicadores históricos e não garantem 
performance futura.

Narrativas geradas por inteligência artificial (Claude/Anthropic) são 
complementares e podem conter imprecisões ou vieses. Recomenda-se verificação 
independente de todas as informações.

Decisões de investimento devem ser tomadas com orientação de profissional 
devidamente habilitado e registrado na CVM (Comissão de Valores Mobiliários).

O Anloc não se responsabiliza por decisões tomadas com base neste material.
```

---

## 4. Processo de Revisão de Qualidade

### 4.1. Checklist Pós-Geração (Automático)

| Check | Automação |
|-------|-----------|
| Todos os números citados existem no dataset? | Validador de referências cruzadas |
| Há linguagem de recomendação proibida? | Filtro de termos (ver §4.2) |
| URLs das fontes são válidas? | Verificador de links (ver §4.3) |
| Citações de especialistas são atribuídas a entidades? | Regex: evitar "nome + cargo" inventado |
| Seção tem disclaimer? | Verificar presença na seção Institucional |

### 4.2. Lista de Termos Proibidos

O sistema deve rejeitar ou sinalizar narrativas que contenham:

```python
TERMOS_PROIBIDOS = [
    "recomendamos", "recomendação de compra", "recomendação de venda",
    "invista em", "compre ações", "venda ações",
    "lucro garantido", "retorno garantido", "sem risco",
    "oportunidade imperdível", "não perca",
    "melhor investimento", "pior investimento",
    "vai subir", "vai cair", "vai disparar", "vai desabar",
    "compre agora", "venda agora", "hora de comprar", "hora de vender",
    "garante retorno", "certeza de lucro",
    "sugerimos posição", "aconselhamos",
    "target price", "preço-alvo",
]
```

### 4.3. Verificação de Links

Todas as URLs citadas nas fontes devem ser verificadas:
1. HTTP HEAD request para validar status 2xx/3xx
2. Domínio pertence a fonte institucional reconhecida
3. URLs quebradas são sinalizadas ao usuário
4. Links fabricados pela IA são removidos com aviso

---

## 5. Governança e Auditoria

### 5.1. Registro de Prompts

Todo prompt enviado ao Claude é registrável via cache do relatório:
- `report_cache.narratives` — Armazena output completo das narrativas
- `report_cache.deep_analyses` — Armazena análises profundas
- `report_cache.chat_history` — Armazena conversas

### 5.2. Versionamento

- Prompts versionados no código-fonte (`app.py`)
- Alterações requerem commit documentado
- Diretrizes atualizadas neste documento

### 5.3. Responsabilidade

| Camada | Responsável |
|--------|-------------|
| Dados (entrada) | Pipeline de extração Damodaran/Yahoo |
| Cálculos (medianas, spreads) | Funções Python determinísticas |
| Narrativas (IA) | Claude sob diretrizes deste documento |
| Revisão final | Equipe Anloc antes de publicação |
| Disclaimer | Automático (seção Institucional) |
