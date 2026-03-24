# Análise de Riscos Legais e Oportunidades de Blindagem

## Estudo Anloc de Múltiplos de Mercado

**Versão:** 1.0  
**Data:** Março/2026  
**Classificação:** Confidencial — Equipe Jurídica e Produto

---

## 1. Mapa de Riscos Identificados

### 1.1. Risco de Recomendação de Investimento ⚠️ CRÍTICO

| Aspecto | Detalhe |
|---------|---------|
| **Risco** | Linguagem gerada pela IA pode ser interpretada como recomendação de compra/venda, configurando exercício irregular de atividade regulada pela CVM |
| **Fundamento** | ICVM 598/2018, Lei 6.385/76 Art. 27-E |
| **Probabilidade** | ALTA (IA generativa tem viés para linguagem afirmativa) |
| **Impacto** | ALTO (multas CVM, ações civis, responsabilidade solidária) |

**Oportunidades de Melhoria:**

- [x] **IMPLEMENTADO** — Diretrizes de compliance nos 4 prompts LLM proibindo linguagem de recomendação
- [x] **IMPLEMENTADO** — Lista de termos proibidos documentada em `docs/diretrizes_llm.md`
- [ ] **PENDENTE** — Implementar filtro automático pós-geração que verifica presença de termos proibidos antes de exibir ao usuário
- [ ] **PENDENTE** — Adicionar watermark "Uso Informativo — Não Constitui Recomendação" em cada seção do relatório
- [ ] **PENDENTE** — Implementar log de auditoria com hash SHA-256 de cada narrativa gerada

**Ação recomendada:**
```python
# Exemplo de filtro pós-geração a implementar
TERMOS_PROIBIDOS = ["recomendamos", "compre", "venda", "invista em", ...]
def validar_narrativa(texto):
    alertas = [t for t in TERMOS_PROIBIDOS if t.lower() in texto.lower()]
    if alertas:
        logger.warning(f"Termos proibidos detectados: {alertas}")
        # Sinalizar ao usuário ou substituir
```

---

### 1.2. Risco de Alucinação/Dados Falsos ⚠️ CRÍTICO

| Aspecto | Detalhe |
|---------|---------|
| **Risco** | LLM pode gerar dados numéricos, citações ou fatos completamente fabricados |
| **Fundamento** | Código Civil Art. 186 (dano por informação falsa), CDC Art. 37 §1° (publicidade enganosa) |
| **Probabilidade** | ALTA (característica inerente de LLMs) |
| **Impacto** | ALTO (perda de credibilidade, ações indenizatórias) |

**Oportunidades de Melhoria:**

- [x] **IMPLEMENTADO** — Prompt instrui a basear-se nos dados fornecidos
- [x] **IMPLEMENTADO** — Verificador de links para validar URLs citadas
- [ ] **PENDENTE** — Implementar validação cruzada: comparar números citados na narrativa com dados do dataset original
- [ ] **PENDENTE** — Adicionar tag visual para cada afirmação: "📊 Dado do dataset" vs "🤖 Análise IA"
- [ ] **PENDENTE** — Implementar sistema de confiança: % dos dados que podem ser verificados no dataset vs fabricados

**Ação recomendada:**
- Cross-reference automático de todos os números citados (EV/EBITDA, spreads, etc.) contra `report_data`
- Sinalizar visualmente quando IA extrapola além dos dados fornecidos

---

### 1.3. Risco de Calúnia e Difamação ⚠️ ALTO

| Aspecto | Detalhe |
|---------|---------|
| **Risco** | IA pode gerar citações falsas atribuídas a pessoas reais, desqualificar empresas/gestores |
| **Fundamento** | Código Penal Art. 138-140 (calúnia, difamação, injúria), Código Civil Art. 953 |
| **Probabilidade** | MÉDIA (mitigada por diretrizes de não inventar nomes) |
| **Impacto** | ALTO (processos criminais e cíveis) |

**Oportunidades de Melhoria:**

- [x] **IMPLEMENTADO** — Instrução para atribuir citações a entidades, não a pessoas
- [x] **IMPLEMENTADO** — URL obrigatória vinculada a cada citação para rastreabilidade
- [ ] **PENDENTE** — Implementar whitelist de entidades/fontes permitidas (ex: Bloomberg, S&P, BCB, FED, etc.)
- [ ] **PENDENTE** — Bloquear automaticamente citações com nomes de pessoas físicas (regex: padrão "Nome Sobrenome, cargo")
- [ ] **PENDENTE** — Adicionar disclaimer por citação: "Citação verificável na fonte indicada"

---

### 1.4. Risco de Violação de Copyright ⚠️ MÉDIO

| Aspecto | Detalhe |
|---------|---------|
| **Risco** | IA pode reproduzir trechos de relatórios protegidos por copyright (Bloomberg, Morgan Stanley, etc.) |
| **Fundamento** | Lei 9.610/98 (Direitos Autorais), DMCA equivalente |
| **Probabilidade** | MÉDIA |
| **Impacto** | MÉDIO (notificações, take-down, multas) |

**Oportunidades de Melhoria:**

- [ ] **PENDENTE** — Adicionar instrução explícita ao prompt: "Não reproduza trechos literais de relatórios de terceiros"
- [ ] **PENDENTE** — Implementar verificação de similaridade: comparar output com bases conhecidas
- [x] **IMPLEMENTADO** — Atribuição de fontes em cada seção
- [ ] **PENDENTE** — Adicionar clause de "fair use" no disclaimer: informação parafraseada para fins educacionais

---

### 1.5. Risco LGPD ⚠️ MÉDIO

| Aspecto | Detalhe |
|---------|---------|
| **Risco** | Dados pessoais podem ser gerados ou processados indevidamente |
| **Fundamento** | LGPD (Lei 13.709/2018) |
| **Probabilidade** | BAIXA (sistema trabalha com dados de mercado, não pessoais) |
| **Impacto** | ALTO se ocorrer (multas até 2% do faturamento, limitado a R$50M) |

**Oportunidades de Melhoria:**

- [x] **IMPLEMENTADO** — Proibição de mencionar nomes de pessoas físicas nos prompts
- [ ] **PENDENTE** — Documentar política de privacidade para chat conversacional (histórico é armazenado no SQLite)
- [ ] **PENDENTE** — Implementar política de retenção de dados (auto-delete de cache após X meses)
- [ ] **PENDENTE** — Garantir que dados enviados ao Claude/Anthropic não contêm PII (Personally Identifiable Information)
- [ ] **PENDENTE** — Verificar conformidade do Anthropic com LGPD/GDPR (contrato DPA)

---

### 1.6. Risco de URLs Fabricadas/Maliciosas ⚠️ MÉDIO

| Aspecto | Detalhe |
|---------|---------|
| **Risco** | LLM pode gerar URLs que parecem legítimas mas levam a sites diferentes ou inexistentes |
| **Fundamento** | Responsabilidade por redirecionamento, phishing involuntário |
| **Probabilidade** | ALTA (LLMs frequentemente "inventam" URLs com formato correto) |
| **Impacto** | MÉDIO (credibilidade, redirecionamento indesejado) |

**Oportunidades de Melhoria:**

- [x] **IMPLEMENTADO** — Verificador de links com validação HTTP
- [x] **IMPLEMENTADO** — Lista de domínios confiáveis (TRUSTED_DOMAINS)
- [ ] **PENDENTE** — Exibir badge visual "✅ Link Verificado" / "⚠️ Não Verificado" ao lado de cada URL
- [ ] **PENDENTE** — Verificação automática na geração (não apenas sob demanda)
- [ ] **PENDENTE** — Bloquear links para domínios fora da whitelist

---

### 1.7. Risco de Decisão Baseada em Dados Desatualizados ⚠️ MÉDIO

| Aspecto | Detalhe |
|---------|---------|
| **Risco** | Dados Damodaran/Yahoo podem estar desatualizados; IA pode usar conhecimento de treinamento obsoleto |
| **Fundamento** | Responsabilidade por informação desatualizada |
| **Probabilidade** | MÉDIA |
| **Impacto** | MÉDIO (decisões incorretas baseadas em dados velhos) |

**Oportunidades de Melhoria:**

- [x] **IMPLEMENTADO** — Metadados com fiscal_year e data de extração no relatório
- [ ] **PENDENTE** — Exibir idade dos dados de forma proeminente ("Dados de: Jan/2026 — Idade: 3 meses")
- [ ] **PENDENTE** — Alertar quando dados tiverem mais de 6 meses
- [ ] **PENDENTE** — Forçar a IA a declarar a data dos dados no início de cada narrativa

---

## 2. Matriz de Priorização

| Risco | Probabilidade | Impacto | Prioridade | Status |
|-------|:---:|:---:|:---:|:---:|
| Recomendação de investimento | Alta | Alto | **P1** | Parcial |
| Alucinação/dados falsos | Alta | Alto | **P1** | Parcial |
| Calúnia/difamação | Média | Alto | **P2** | Parcial |
| URLs fabricadas | Alta | Médio | **P2** | Parcial |
| Copyright | Média | Médio | **P3** | Parcial |
| LGPD | Baixa | Alto | **P3** | Parcial |
| Dados desatualizados | Média | Médio | **P3** | Parcial |

---

## 3. Plano de Ação por Prioridade

### P1 — Implementar Imediatamente

1. **Filtro pós-geração de termos proibidos**
   - Verificar toda narrativa antes de exibir ao usuário
   - Se detectado: sinalizar com banner de alerta + log

2. **Cross-reference de dados**
   - Comparar cada número citado com os dados originais do `report_data`
   - Marcar discrepâncias visualmente

3. **Watermark anti-recomendação**
   - Adicionar em cada seção narrativa: rodapé "Material informativo — Não constitui recomendação"

### P2 — Implementar a Curto Prazo

4. **Whitelist de entidades citáveis**
   - Manter lista curada de fontes institucionais permitidas
   - Sinalizar citações a entidades fora da lista

5. **Verificação automática de links na geração**
   - Validar URLs antes de salvar no cache
   - Badge visual de status

6. **Bloqueio de PII em citações**
   - Regex para detectar padrão "Nome Sobrenome" em citações
   - Substituir por nome institucional

### P3 — Implementar a Médio Prazo

7. **Política de retenção de dados**
   - Auto-delete de caches com mais de 12 meses
   - Log de acessos ao chat

8. **Verificação de copyright**
   - Instrução adicional nos prompts
   - Disclaimer de fair use

9. **Idade dos dados**
   - Banner visual com idade dos dados
   - Alerta quando obsoletos

---

## 4. Disclaimers Recomendados

### 4.1. Disclaimer Principal (já implementado na seção Institucional)

> Este material é de caráter exclusivamente informativo e educacional [...] Não constitui recomendação de investimento.

### 4.2. Disclaimer de IA (sugestão para adicionar)

> As narrativas e análises textuais deste relatório foram geradas com auxílio de inteligência artificial (Claude/Anthropic) e podem conter imprecisões, vieses ou extrapolações. As informações devem ser verificadas de forma independente antes de qualquer tomada de decisão. O Anloc revisa o conteúdo gerado mas não garante sua exatidão.

### 4.3. Disclaimer por Citação (sugestão)

> Citações atribuídas a entidades e especialistas são baseadas em fontes públicas indicadas. Verifique a fonte original para contexto completo.

### 4.4. Disclaimer de Dados (sugestão)

> Os dados utilizados são públicos, extraídos de Damodaran Online, Yahoo Finance e fontes governamentais. Podem apresentar defasagem temporal. Consulte as fontes primárias para dados atualizados.

---

## 5. Aspectos Regulatórios Aplicáveis

| Regulação | Aplicabilidade | Status |
|-----------|:-:|:-:|
| **CVM/ICVM 598** — Analistas de valores mobiliários | Indireta — se linguagem configurar advisory | ⚠️ Mitigado |
| **LGPD 13.709/2018** — Proteção de dados pessoais | Baixa — dados de mercado | ⚠️ Atenção ao chat |
| **CDC Art. 37** — Publicidade enganosa | Média — se dados forem falsos | ⚠️ Mitigado |
| **Lei 9.610/98** — Direitos autorais | Média — reprodução de trechos | 🔴 Pendente |
| **Marco Civil da Internet** — Responsabilidade por conteúdo | Média — como provedor de conteúdo | ⚠️ Mitigado |
| **Código Penal Art. 138-140** — Crimes contra a honra | Baixa — se citar pessoas | ⚠️ Mitigado |

---

## 6. Conclusão

O sistema possui boa base de proteção já implementada:
- ✅ Diretrizes de compliance em todos os prompts LLM
- ✅ Proibição de recomendações de investimento
- ✅ Verificador de links
- ✅ Citações vinculadas a URLs verificáveis
- ✅ Disclaimer na seção institucional

As principais lacunas são:
- 🔴 Ausência de filtro automatizado pós-geração (defesa em profundidade)
- 🔴 Ausência de cross-reference automático de dados
- 🟡 Falta de política de retenção LGPD
- 🟡 Falta de proteção contra copyright (instrução explícita)

**Recomendação:** Implementar as ações P1 (filtro de termos + cross-reference + watermark) antes de publicar o relatório para uso externo.
