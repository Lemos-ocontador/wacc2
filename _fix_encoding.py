# Script to rewrite estudoanloc_insights.html with correct UTF-8 encoding
import os

path = os.path.join(os.path.dirname(__file__), 'templates', 'estudoanloc_insights.html')

content = r'''<!DOCTYPE html>
<html lang="pt-br">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Insights — Estudo Anloc</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.6/dist/chart.umd.min.js"></script>
    <style>
        :root {
            --bg: #0f1923; --bg2: #152232; --bg3: #1a2d42;
            --accent: #00d4aa; --accent2: #00b894;
            --text: #e8edf2; --text2: #8899aa; --text3: #5a6f82;
            --border: #1e3448; --danger: #e74c3c; --warn: #f39c12;
            --blue: #3498db; --purple: #9b59b6;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; min-height: 100vh; }

        /* Header */
        .header { background: linear-gradient(135deg, var(--bg2), var(--bg3)); border-bottom: 1px solid var(--border); padding: .8rem 1.5rem; display: flex; align-items: center; gap: 1rem; flex-wrap: wrap; }
        .header h1 { font-size: 1.3rem; font-weight: 700; }
        .header h1 span { color: var(--accent); }
        .header-actions { display: flex; gap: .4rem; margin-left: auto; }
        .btn-icon { background: var(--bg); border: 1px solid var(--border); color: var(--text2); width: 36px; height: 36px; border-radius: 8px; cursor: pointer; display: flex; align-items: center; justify-content: center; transition: .2s; font-size: .9rem; }
        .btn-icon:hover, .btn-icon.active { border-color: var(--accent); color: var(--accent); }
        .btn-nav { background: var(--bg); border: 1px solid var(--border); color: var(--text2); padding: .4rem .8rem; border-radius: 6px; text-decoration: none; font-size: .82rem; transition: .2s; }
        .btn-nav:hover { border-color: var(--accent); color: var(--accent); }

        /* Layout */
        .layout { display: flex; min-height: calc(100vh - 52px); }
        .main-area { flex: 1; padding: 1.2rem; max-width: 1600px; overflow-y: auto; transition: margin-right .3s; }
        .main-area.chat-open { margin-right: 380px; }

        /* Panels */
        .panel { background: var(--bg2); border: 1px solid var(--border); border-radius: 10px; padding: 1rem; margin-bottom: 1rem; }
        .panel-title { font-size: .9rem; font-weight: 600; color: var(--accent); margin-bottom: .7rem; display: flex; align-items: center; gap: .5rem; }

        /* Config Panel */
        .config-panel { max-height: 0; overflow: hidden; transition: max-height .3s ease, padding .3s; padding: 0 1rem; margin-bottom: 0; }
        .config-panel.open { max-height: 300px; padding: 1rem; margin-bottom: 1rem; }
        .config-grid { display: grid; grid-template-columns: 180px 1fr 120px; gap: .8rem; align-items: end; }
        .config-row2 { margin-top: .7rem; }
        .config-row2 textarea { width: 100%; background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: .5rem; border-radius: 6px; font-size: .82rem; resize: vertical; min-height: 50px; }
        .config-row2 textarea:focus { border-color: var(--accent); outline: none; }
        .key-status { font-size: .7rem; padding: 2px 8px; border-radius: 10px; }
        .key-ok { background: rgba(0,212,170,.15); color: var(--accent); }
        .key-missing { background: rgba(231,76,60,.15); color: var(--danger); }
        .key-env { background: rgba(52,152,219,.15); color: var(--blue); }

        /* Filters */
        .filters-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr)); gap: .7rem; }
        .filter-group label { display: block; font-size: .7rem; color: var(--text2); margin-bottom: .2rem; text-transform: uppercase; letter-spacing: .5px; }
        .filter-group select, .filter-group input { width: 100%; background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: .45rem .6rem; border-radius: 6px; font-size: .82rem; }
        .filter-group select:focus, .filter-group input:focus { border-color: var(--accent); outline: none; }
        .btn-calc { background: linear-gradient(135deg, var(--accent), var(--accent2)); color: #000; border: none; padding: .55rem 1.3rem; border-radius: 8px; font-weight: 600; font-size: .85rem; cursor: pointer; transition: .2s; }
        .btn-calc:hover { opacity: .9; transform: translateY(-1px); }
        .btn-calc:disabled { opacity: .5; cursor: not-allowed; transform: none; }

        /* KPIs */
        .kpi-row { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: .8rem; margin-bottom: 1rem; }
        .kpi-card { background: var(--bg2); border: 1px solid var(--border); border-radius: 10px; padding: .8rem; text-align: center; transition: .2s; }
        .kpi-card:hover { border-color: var(--accent); transform: translateY(-2px); }
        .kpi-label { font-size: .65rem; color: var(--text2); text-transform: uppercase; letter-spacing: .5px; }
        .kpi-value { font-size: 1.4rem; font-weight: 700; color: var(--accent); margin: .2rem 0; }
        .kpi-sub { font-size: .7rem; color: var(--text3); }

        /* Tabs */
        .tab-bar { display: flex; gap: .2rem; border-bottom: 1px solid var(--border); flex-wrap: wrap; margin-bottom: .8rem; }
        .tab-btn { background: none; border: none; color: var(--text2); padding: .5rem 1rem; font-size: .8rem; cursor: pointer; border-bottom: 2px solid transparent; transition: .2s; white-space: nowrap; }
        .tab-btn.active { color: var(--accent); border-bottom-color: var(--accent); }
        .tab-content { display: none; }
        .tab-content.active { display: block; }

        /* Insight cards */
        .insight-card { background: var(--bg3); border-radius: 8px; padding: .8rem 1rem; margin-bottom: .5rem; transition: .2s; }
        .insight-card:hover { transform: translateX(3px); }
        .insight-card.severity-danger { border-left: 3px solid var(--danger); }
        .insight-card.severity-warning { border-left: 3px solid var(--warn); }
        .insight-card.severity-info { border-left: 3px solid var(--accent); }
        .insight-header { display: flex; align-items: center; gap: .5rem; margin-bottom: .2rem; }
        .insight-title { font-weight: 600; font-size: .82rem; }
        .insight-text { color: var(--text2); font-size: .78rem; line-height: 1.5; }
        .insight-badge { font-size: .6rem; padding: 2px 7px; border-radius: 10px; margin-left: auto; }

        /* LLM sections */
        .llm-section { background: var(--bg3); border: 1px solid var(--border); border-radius: 10px; padding: 1rem; margin-bottom: .8rem; }
        .llm-section h4 { color: var(--accent); font-size: .85rem; margin-bottom: .5rem; display: flex; align-items: center; gap: .4rem; }
        .llm-text { color: var(--text); font-size: .82rem; line-height: 1.7; }
        .llm-text p { margin-bottom: .5rem; }
        .llm-highlight { background: var(--bg2); border-radius: 8px; padding: .7rem .9rem; margin: .5rem 0; }
        .llm-highlight.tipo-positivo { border-left: 3px solid var(--accent); }
        .llm-highlight.tipo-negativo { border-left: 3px solid var(--danger); }
        .llm-highlight.tipo-neutro { border-left: 3px solid var(--blue); }
        .llm-highlight.tipo-risco { border-left: 3px solid var(--danger); }
        .llm-highlight.tipo-oportunidade { border-left: 3px solid var(--accent); }
        .llm-highlight strong { color: var(--text); }
        .llm-highlight p, .llm-highlight span { color: var(--text2); font-size: .8rem; }

        /* Charts */
        .charts-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 1rem; margin-bottom: 1rem; }
        .chart-panel { background: var(--bg2); border: 1px solid var(--border); border-radius: 10px; padding: .8rem; }
        .chart-title { font-size: .82rem; font-weight: 600; color: var(--text2); margin-bottom: .5rem; display: flex; align-items: center; gap: .4rem; }
        .chart-container { position: relative; height: 280px; }

        /* Tables */
        .data-table { width: 100%; border-collapse: collapse; font-size: .78rem; }
        .data-table th { background: var(--bg3); color: var(--accent); padding: .4rem .6rem; text-align: center; font-weight: 600; border-bottom: 2px solid var(--border); position: sticky; top: 0; }
        .data-table th:first-child { text-align: left; }
        .data-table td { padding: .35rem .6rem; border-bottom: 1px solid var(--border); text-align: center; }
        .data-table td:first-child { text-align: left; font-weight: 500; }
        .data-table tr:hover { background: rgba(0,212,170,.04); }

        /* Evolution specific */
        .trend-badge { font-size: .7rem; padding: 2px 8px; border-radius: 10px; font-weight: 600; }
        .trend-up { background: rgba(0,212,170,.15); color: var(--accent); }
        .trend-down { background: rgba(231,76,60,.15); color: var(--danger); }
        .trend-stable { background: rgba(52,152,219,.15); color: var(--blue); }

        /* Chat Drawer */
        .chat-drawer { position: fixed; right: -380px; top: 52px; width: 380px; height: calc(100vh - 52px); background: var(--bg2); border-left: 1px solid var(--border); display: flex; flex-direction: column; transition: right .3s; z-index: 100; }
        .chat-drawer.open { right: 0; }
        .chat-header { padding: .7rem 1rem; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: .5rem; }
        .chat-header h3 { font-size: .9rem; color: var(--accent); flex: 1; }
        .chat-messages { flex: 1; overflow-y: auto; padding: .8rem; display: flex; flex-direction: column; gap: .6rem; }
        .chat-bubble { max-width: 90%; padding: .6rem .9rem; border-radius: 12px; font-size: .82rem; line-height: 1.5; animation: fadeIn .2s; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(5px); } to { opacity: 1; transform: none; } }
        .chat-bubble.user { background: var(--accent); color: #000; align-self: flex-end; border-bottom-right-radius: 4px; }
        .chat-bubble.assistant { background: var(--bg3); color: var(--text); align-self: flex-start; border-bottom-left-radius: 4px; }
        .chat-bubble.assistant p { margin-bottom: .4rem; }
        .chat-bubble.system { background: rgba(52,152,219,.1); color: var(--blue); align-self: center; font-size: .75rem; text-align: center; }
        .chat-suggestions { padding: .5rem .8rem; display: flex; flex-wrap: wrap; gap: .3rem; border-top: 1px solid var(--border); }
        .suggestion-chip { background: var(--bg3); border: 1px solid var(--border); color: var(--text2); padding: .3rem .6rem; border-radius: 15px; font-size: .7rem; cursor: pointer; transition: .2s; }
        .suggestion-chip:hover { border-color: var(--accent); color: var(--accent); }
        .chat-input-area { padding: .6rem; border-top: 1px solid var(--border); display: flex; gap: .4rem; }
        .chat-input { flex: 1; background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: .5rem .7rem; border-radius: 8px; font-size: .82rem; resize: none; }
        .chat-input:focus { border-color: var(--accent); outline: none; }
        .chat-send { background: var(--accent); color: #000; border: none; width: 36px; height: 36px; border-radius: 8px; cursor: pointer; font-size: .9rem; transition: .2s; }
        .chat-send:hover { opacity: .85; }
        .chat-send:disabled { opacity: .4; cursor: not-allowed; }

        /* Loading */
        .loading { text-align: center; padding: 2.5rem; color: var(--text2); }
        .spinner { width: 36px; height: 36px; border: 3px solid var(--border); border-top-color: var(--accent); border-radius: 50%; animation: spin .8s linear infinite; margin: 0 auto; }
        @keyframes spin { to { transform: rotate(360deg); } }
        .empty-state { text-align: center; padding: 2rem; color: var(--text3); font-size: .85rem; }
        .disclaimer { background: rgba(255,193,7,.08); border: 1px solid rgba(255,193,7,.25); border-radius: 8px; padding: .6rem .9rem; margin-bottom: .8rem; font-size: .75rem; color: #ffc107; }
        .typing-dots { display: inline-flex; gap: 3px; } .typing-dots span { width: 6px; height: 6px; background: var(--text3); border-radius: 50%; animation: blink 1.4s infinite; } .typing-dots span:nth-child(2) { animation-delay: .2s; } .typing-dots span:nth-child(3) { animation-delay: .4s; }
        @keyframes blink { 0%,80%,100% { opacity: .3; } 40% { opacity: 1; } }

        @media (max-width: 768px) {
            .kpi-row { grid-template-columns: repeat(2, 1fr); }
            .charts-grid { grid-template-columns: 1fr; }
            .filters-grid { grid-template-columns: repeat(2, 1fr); }
            .config-grid { grid-template-columns: 1fr; }
            .chat-drawer { width: 100%; right: -100%; }
            .main-area.chat-open { margin-right: 0; }
        }
    </style>
</head>
<body>
    <!-- Header -->
    <div class="header">
        <h1><i class="fas fa-lightbulb" style="color:var(--accent);"></i> <span>Insights</span> &mdash; Estudo Anloc</h1>
        <div class="header-actions">
            <button class="btn-icon" onclick="toggleConfig()" title="Configura&#231;&#245;es LLM" id="btnConfig"><i class="fas fa-cog"></i></button>
            <button class="btn-icon" onclick="toggleChat()" title="Chat IA" id="btnChat"><i class="fas fa-comment-dots"></i></button>
            <a href="/estudoanloc" class="btn-nav"><i class="fas fa-table"></i> Estudo</a>
            <a href="/" class="btn-nav"><i class="fas fa-home"></i></a>
        </div>
    </div>

    <div class="layout">
        <div class="main-area" id="mainArea">
            <!-- Config Panel (collapsible) -->
            <div class="panel config-panel" id="configPanel">
                <div class="panel-title"><i class="fas fa-cog"></i> Configura&#231;&#227;o da IA</div>
                <div class="config-grid">
                    <div class="filter-group">
                        <label>Provedor LLM</label>
                        <select id="llmProvider" onchange="onProviderChange()">
                            <option value="gemini">Google Gemini</option>
                            <option value="openai">OpenAI (GPT-4o)</option>
                            <option value="anthropic">Anthropic (Claude)</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>API Key <span id="keyStatus" class="key-status key-missing">N&#227;o configurada</span></label>
                        <input type="password" id="apiKeyInput" placeholder="Cole sua API Key aqui (ou use env var)" oninput="onKeyChange()">
                    </div>
                    <div class="filter-group" style="display:flex;align-items:flex-end;">
                        <button class="btn-calc" onclick="testLlmConnection()" style="font-size:.78rem;padding:.45rem .8rem;"><i class="fas fa-plug"></i> Testar</button>
                    </div>
                </div>
                <div class="config-row2">
                    <label style="font-size:.7rem;color:var(--text2);text-transform:uppercase;letter-spacing:.5px;">Diretrizes adicionais (instru&#231;&#245;es para a IA)</label>
                    <textarea id="customInstructions" placeholder="Ex: Foque na compara&#231;&#227;o Brasil vs EUA. Destaque tend&#234;ncias de compress&#227;o de m&#250;ltiplos. Analise riscos macroecon&#244;micos."></textarea>
                </div>
            </div>

            <!-- Filters -->
            <div class="panel">
                <div class="panel-title"><i class="fas fa-filter"></i> Filtros de An&#225;lise</div>
                <div class="filters-grid">
                    <div class="filter-group">
                        <label>Setor *</label>
                        <select id="sectorSelect" style="border-color:var(--accent);"><option value="">Carregando...</option></select>
                    </div>
                    <div class="filter-group">
                        <label>Ind&#250;stria</label>
                        <select id="industrySelect"><option value="">Todas</option></select>
                    </div>
                    <div class="filter-group">
                        <label>Ano Fiscal</label>
                        <select id="yearSelect"></select>
                    </div>
                    <div class="filter-group">
                        <label>Regi&#227;o</label>
                        <select id="regionSelect">
                            <option value="">Global</option>
                            <option value="Brazil">Brasil</option>
                            <option value="LATAM">LATAM</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Pa&#237;s</label>
                        <select id="countrySelect"><option value="">Todos</option></select>
                    </div>
                    <div class="filter-group">
                        <label>EV M&#237;n (USD)</label>
                        <select id="minEvSelect">
                            <option value="0">Sem filtro</option>
                            <option value="50000000">$50M</option>
                            <option value="100000000" selected>$100M</option>
                            <option value="500000000">$500M</option>
                            <option value="1000000000">$1B</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Teto EV/EBITDA</label>
                        <select id="maxEvEbitdaSelect">
                            <option value="40">40x</option>
                            <option value="60" selected>60x</option>
                            <option value="100">100x</option>
                            <option value="0">Sem teto</option>
                        </select>
                    </div>
                    <div class="filter-group" style="display:flex;align-items:flex-end;">
                        <button class="btn-calc" id="analyzeBtn" onclick="runAnalysis()">
                            <i class="fas fa-brain"></i> Analisar
                        </button>
                    </div>
                </div>
            </div>

            <!-- Loading -->
            <div id="loadingArea" style="display:none;">
                <div class="loading">
                    <div class="spinner"></div>
                    <p style="margin-top:.6rem;">Analisando dados e gerando insights...</p>
                </div>
            </div>

            <!-- Results -->
            <div id="resultsArea" style="display:none;">
                <div class="disclaimer">
                    <i class="fas fa-info-circle"></i> Insights gerados por an&#225;lise heur&#237;stica e intelig&#234;ncia artificial. N&#227;o constitui recomenda&#231;&#227;o de investimento.
                </div>

                <div id="kpiRow" class="kpi-row"></div>

                <div class="panel" style="padding:.6rem 1rem 1rem;">
                    <div class="tab-bar">
                        <button class="tab-btn active" onclick="switchTab(this,'overview')"><i class="fas fa-lightbulb"></i> Insights</button>
                        <button class="tab-btn" onclick="switchTab(this,'charts')"><i class="fas fa-chart-bar"></i> Gr&#225;ficos</button>
                        <button class="tab-btn" onclick="switchTab(this,'evolution')"><i class="fas fa-chart-line"></i> Evolu&#231;&#227;o</button>
                        <button class="tab-btn" onclick="switchTab(this,'industries')"><i class="fas fa-industry"></i> Ind&#250;strias</button>
                        <button class="tab-btn" onclick="switchTab(this,'geo')"><i class="fas fa-globe-americas"></i> Geografia</button>
                        <button class="tab-btn" onclick="switchTab(this,'companies')"><i class="fas fa-building"></i> Empresas</button>
                        <button class="tab-btn" onclick="switchTab(this,'llm')" id="tabLlm" style="display:none;"><i class="fas fa-brain"></i> An&#225;lise IA</button>
                    </div>
                    <div id="tab-overview" class="tab-content active"></div>
                    <div id="tab-charts" class="tab-content"></div>
                    <div id="tab-evolution" class="tab-content"></div>
                    <div id="tab-industries" class="tab-content"></div>
                    <div id="tab-geo" class="tab-content"></div>
                    <div id="tab-companies" class="tab-content"></div>
                    <div id="tab-llm" class="tab-content"></div>
                </div>
            </div>
        </div>

        <!-- Chat Drawer -->
        <div class="chat-drawer" id="chatDrawer">
            <div class="chat-header">
                <i class="fas fa-brain" style="color:var(--accent);"></i>
                <h3>Chat IA</h3>
                <span id="chatProviderBadge" style="font-size:.65rem;padding:2px 6px;background:rgba(155,89,182,.15);color:var(--purple);border-radius:8px;">Gemini</span>
                <button class="btn-icon" onclick="toggleChat()" style="width:28px;height:28px;font-size:.75rem;"><i class="fas fa-times"></i></button>
            </div>
            <div class="chat-messages" id="chatMessages">
                <div class="chat-bubble system">Selecione um setor e clique em "Analisar" para habilitar o chat com contexto dos dados.</div>
            </div>
            <div class="chat-suggestions" id="chatSuggestions">
                <div class="suggestion-chip" onclick="sendSuggestion(this)">Resuma os principais destaques</div>
                <div class="suggestion-chip" onclick="sendSuggestion(this)">Compare Brasil vs EUA</div>
                <div class="suggestion-chip" onclick="sendSuggestion(this)">Quais ind&#250;strias est&#227;o caras?</div>
                <div class="suggestion-chip" onclick="sendSuggestion(this)">Identifique tend&#234;ncias de evolu&#231;&#227;o</div>
            </div>
            <div class="chat-input-area">
                <textarea class="chat-input" id="chatInput" rows="1" placeholder="Pergunte sobre os dados..." onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();sendChat();}"></textarea>
                <button class="chat-send" onclick="sendChat()" id="chatSendBtn"><i class="fas fa-paper-plane"></i></button>
            </div>
        </div>
    </div>

    <script>
    // ====================================
    // STATE
    // ====================================
    let analysisData = null;
    let evolutionData = null;
    let chartInstances = {};
    let chatHistory = [];
    let dataContextStr = '';

    // ====================================
    // CONFIG / LLM
    // ====================================
    function getLlmConfig() {
        return {
            provider: document.getElementById('llmProvider').value,
            api_key: document.getElementById('apiKeyInput').value,
            custom_instructions: document.getElementById('customInstructions').value
        };
    }
    function saveLlmConfig() {
        const cfg = getLlmConfig();
        localStorage.setItem('anloc_llm_provider', cfg.provider);
        localStorage.setItem('anloc_llm_key_' + cfg.provider, cfg.api_key);
        localStorage.setItem('anloc_llm_instructions', cfg.custom_instructions);
    }
    function loadLlmConfig() {
        const prov = localStorage.getItem('anloc_llm_provider') || 'gemini';
        document.getElementById('llmProvider').value = prov;
        const key = localStorage.getItem('anloc_llm_key_' + prov) || '';
        document.getElementById('apiKeyInput').value = key;
        document.getElementById('customInstructions').value = localStorage.getItem('anloc_llm_instructions') || '';
        onProviderChange();
        onKeyChange();
    }
    function onProviderChange() {
        const prov = document.getElementById('llmProvider').value;
        const key = localStorage.getItem('anloc_llm_key_' + prov) || '';
        document.getElementById('apiKeyInput').value = key;
        document.getElementById('chatProviderBadge').textContent = {gemini:'Gemini',openai:'GPT-4o',anthropic:'Claude'}[prov];
        onKeyChange();
    }
    function onKeyChange() {
        const key = document.getElementById('apiKeyInput').value;
        const el = document.getElementById('keyStatus');
        if (key) { el.textContent = 'Configurada'; el.className = 'key-status key-ok'; }
        else { el.textContent = 'Env var (fallback)'; el.className = 'key-status key-env'; }
        saveLlmConfig();
    }
    async function testLlmConnection() {
        const cfg = getLlmConfig();
        try {
            const r = await fetch('/api/estudoanloc/chat', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({provider: cfg.provider, api_key: cfg.api_key,
                    messages: [{role:'user',content:'Responda apenas: OK'}], data_context: ''})
            });
            const d = await r.json();
            alert(d.success ? 'Conex\u00e3o OK com ' + cfg.provider : 'Erro: ' + d.error);
        } catch(e) { alert('Erro de conex\u00e3o: ' + e.message); }
    }
    function toggleConfig() {
        const p = document.getElementById('configPanel');
        const b = document.getElementById('btnConfig');
        p.classList.toggle('open'); b.classList.toggle('active');
    }
    function toggleChat() {
        const d = document.getElementById('chatDrawer');
        const m = document.getElementById('mainArea');
        const b = document.getElementById('btnChat');
        d.classList.toggle('open'); m.classList.toggle('chat-open'); b.classList.toggle('active');
    }

    // ====================================
    // FILTERS
    // ====================================
    document.addEventListener('DOMContentLoaded', () => { loadFilters(); loadLlmConfig(); });

    async function loadFilters() {
        try {
            const r = await fetch('/api/estudoanloc/filters');
            const d = await r.json();
            if (!d.success) return;
            const sel = document.getElementById('sectorSelect');
            sel.innerHTML = '<option value="">\u2014 Selecione setor \u2014</option>' + d.sectors.map(s => `<option>${s}</option>`).join('');
            sel.addEventListener('change', loadIndustries);
            const ySel = document.getElementById('yearSelect');
            const yrs = d.years || [];
            ySel.innerHTML = yrs.map(y => `<option value="${y}"${y===Math.max(...yrs)?' selected':''}>${y}</option>`).join('');
            if (d.regions) {
                const rSel = document.getElementById('regionSelect');
                d.regions.filter(r => !['Brazil','LATAM'].includes(r)).forEach(r => {
                    const o = document.createElement('option'); o.value = r; o.textContent = r; rSel.appendChild(o);
                });
            }
        } catch(e) { console.error('Erro filtros:', e); }
    }
    async function loadIndustries() {
        const sector = document.getElementById('sectorSelect').value;
        const iSel = document.getElementById('industrySelect');
        if (!sector) { iSel.innerHTML = '<option value="">Todas</option>'; return; }
        iSel.innerHTML = '<option value="">Carregando...</option>';
        try {
            const r = await fetch(`/api/estudoanloc/industries?sector=${encodeURIComponent(sector)}`);
            const d = await r.json();
            iSel.innerHTML = '<option value="">Todas as ind\u00fastrias</option>' +
                (d.success && d.industries ? d.industries.map(i => `<option>${i}</option>`).join('') : '');
        } catch(e) { iSel.innerHTML = '<option value="">Todas</option>'; }
    }

    // ====================================
    // ANALYSIS
    // ====================================
    async function runAnalysis() {
        const sector = document.getElementById('sectorSelect').value;
        if (!sector) { alert('Selecione um setor'); return; }
        const btn = document.getElementById('analyzeBtn');
        btn.disabled = true;
        document.getElementById('loadingArea').style.display = 'block';
        document.getElementById('resultsArea').style.display = 'none';
        const cfg = getLlmConfig();
        const industry = document.getElementById('industrySelect').value;
        const payload = {
            sector, fiscal_year: parseInt(document.getElementById('yearSelect').value),
            region: document.getElementById('regionSelect').value,
            country: document.getElementById('countrySelect').value,
            industries: industry ? [industry] : [],
            filters: {
                min_ev_usd: parseInt(document.getElementById('minEvSelect').value) || 0,
                max_ev_ebitda: parseInt(document.getElementById('maxEvEbitdaSelect').value) || 0
            },
            llm_provider: cfg.provider, api_key: cfg.api_key
        };
        try {
            const [analysisResp, evoResp] = await Promise.all([
                fetch('/api/estudoanloc/insights_llm', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}),
                fetch('/api/estudoanloc/evolution_data', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
                    sector, industry, region: payload.region, country: payload.country,
                    min_ev_usd: payload.filters.min_ev_usd, max_ev_ebitda: payload.filters.max_ev_ebitda,
                    year_start: 2021, year_end: parseInt(document.getElementById('yearSelect').value)
                })})
            ]);
            const data = await analysisResp.json();
            const evo = await evoResp.json();
            if (!data.success) { alert('Erro: ' + data.error); return; }
            analysisData = data;
            evolutionData = evo.success ? evo : null;
            buildDataContext(data, evo);
            renderAll(data, evo);
            document.getElementById('resultsArea').style.display = 'block';
            // Reset chat with context
            chatHistory = [];
            document.getElementById('chatMessages').innerHTML = '<div class="chat-bubble system">Dados carregados! Pergunte qualquer coisa sobre o setor <strong>' + sector + '</strong>.</div>';
            updateSuggestions(sector, industry);
        } catch(e) { alert('Erro: ' + e.message); }
        finally { btn.disabled = false; document.getElementById('loadingArea').style.display = 'none'; }
    }

    function buildDataContext(data, evo) {
        const m = data.metadata || {};
        const s = data.stats || {};
        let ctx = `Setor: ${m.sector} | Ano: ${m.fiscal_year} | Empresas: ${m.total_companies}\n`;
        for (const k of ['ev_ebitda','ev_revenue','fcf_revenue','fcf_ebitda']) {
            const v = s[k]; if (!v) continue;
            ctx += `${k}: mediana=${v.median?.toFixed(1)}x p25=${v.p25?.toFixed(1)}x p75=${v.p75?.toFixed(1)}x (n=${v.n})\n`;
        }
        if (data.by_industry) { ctx += '\nPor ind\u00fastria:\n'; data.by_industry.slice(0,10).forEach(i => { const e = i.ev_ebitda; if(e) ctx += `  ${i.label}: ${e.median?.toFixed(1)}x (n=${i.n})\n`; }); }
        if (data.by_region) { ctx += '\nPor regi\u00e3o:\n'; data.by_region.forEach(r => { const e = r.ev_ebitda; if(e) ctx += `  ${r.label}: ${e.median?.toFixed(1)}x (n=${r.n})\n`; }); }
        if (data.top_companies) { ctx += '\nTop empresas:\n'; data.top_companies.slice(0,5).forEach(c => ctx += `  ${c.ticker} (${c.country}): EV/EBITDA=${c.ev_ebitda?.toFixed(1)}x\n`); }
        if (evo && evo.evolution) {
            ctx += '\nEVOLU\u00c7\u00c3O TEMPORAL:\n';
            evo.evolution.filter(y=>y.n>0).forEach(y => {
                const e = y.ev_ebitda; if(e) ctx += `  ${y.year}: EV/EBITDA med=${e.median}x (n=${e.n})\n`;
            });
            if (evo.trends) { ctx += 'Tend\u00eancias:\n'; for(const[k,v] of Object.entries(evo.trends)) ctx += `  ${k}: ${v.start_val}x\u2192${v.end_val}x (${v.change_pct>0?'+':''}${v.change_pct}%)\n`; }
        }
        dataContextStr = ctx;
    }

    function updateSuggestions(sector, industry) {
        const chips = [
            `Resuma o panorama de ${sector}`,
            industry ? `Analise ${industry} em detalhe` : 'Quais ind\u00fastrias est\u00e3o mais caras?',
            'Compare Brasil vs mercado global',
            'Identifique tend\u00eancias dos \u00faltimos anos',
            'Quais empresas merecem aten\u00e7\u00e3o?',
            'Analise riscos e oportunidades'
        ];
        document.getElementById('chatSuggestions').innerHTML = chips.map(c => `<div class="suggestion-chip" onclick="sendSuggestion(this)">${c}</div>`).join('');
    }

    // ====================================
    // RENDER ALL
    // ====================================
    function renderAll(data, evo) {
        renderKPIs(data);
        renderOverviewTab(data);
        renderChartsTab(data);
        renderEvolutionTab(evo);
        renderIndustriesTab(data);
        renderGeoTab(data);
        renderCompaniesTab(data);
        renderLlmTab(data);
    }

    // Formatters
    function fmtX(v,d=1){return v!=null&&isFinite(v)?v.toFixed(d)+'x':'\u2014';}
    function fmtPct(v){return v!=null&&isFinite(v)?v.toFixed(1)+'%':'\u2014';}
    function sg(o,m,s){try{return o[m][s];}catch(e){return null;}}

    // ====================================
    // KPIs
    // ====================================
    function renderKPIs(data) {
        const s=data.stats||{}, m=data.metadata||{};
        const trend = evolutionData?.trends?.ev_ebitda;
        let trendHtml = '';
        if (trend) {
            const cls = trend.direction==='up'?'trend-up':trend.direction==='down'?'trend-down':'trend-stable';
            const icon = trend.direction==='up'?'fa-arrow-up':trend.direction==='down'?'fa-arrow-down':'fa-minus';
            trendHtml = `<span class="trend-badge ${cls}"><i class="fas ${icon}"></i> ${trend.change_pct>0?'+':''}${trend.change_pct}% (${trend.start_year}-${trend.end_year})</span>`;
        }
        document.getElementById('kpiRow').innerHTML = `
            <div class="kpi-card"><div class="kpi-label">Empresas</div><div class="kpi-value">${m.total_companies||0}</div><div class="kpi-sub">${m.sector} \u00b7 ${m.fiscal_year}</div></div>
            <div class="kpi-card"><div class="kpi-label">EV/EBITDA Med</div><div class="kpi-value">${fmtX(sg(s,'ev_ebitda','median'))}</div><div class="kpi-sub">P25: ${fmtX(sg(s,'ev_ebitda','p25'))} \u00b7 P75: ${fmtX(sg(s,'ev_ebitda','p75'))} ${trendHtml}</div></div>
            <div class="kpi-card"><div class="kpi-label">EV/Vendas Med</div><div class="kpi-value">${fmtX(sg(s,'ev_revenue','median'))}</div><div class="kpi-sub">P25: ${fmtX(sg(s,'ev_revenue','p25'))} \u00b7 P75: ${fmtX(sg(s,'ev_revenue','p75'))}</div></div>
            <div class="kpi-card"><div class="kpi-label">FCF/Vendas Med</div><div class="kpi-value">${fmtX(sg(s,'fcf_revenue','median'),2)}</div><div class="kpi-sub">P25: ${fmtX(sg(s,'fcf_revenue','p25'),2)} \u00b7 P75: ${fmtX(sg(s,'fcf_revenue','p75'),2)}</div></div>
            <div class="kpi-card"><div class="kpi-label">Insights</div><div class="kpi-value">${(data.heuristic_insights||[]).length}</div><div class="kpi-sub">${data.llm_analysis?'+ An\u00e1lise IA':'Heur\u00edsticos'}</div></div>
        `;
    }

    // ====================================
    // OVERVIEW (Heuristic insights)
    // ====================================
    function renderOverviewTab(data) {
        const c = document.getElementById('tab-overview');
        const ins = data.heuristic_insights || [];
        if (!ins.length) { c.innerHTML='<div class="empty-state">Nenhum insight</div>'; return; }
        const sevC={danger:'#ff4d6a',warning:'#ffb347',info:'#00d4aa'}, sevL={danger:'Alerta',warning:'Aten\u00e7\u00e3o',info:'Info'};
        const catL={contexto:'Contexto Setorial',geo:'Compara\u00e7\u00e3o Geogr\u00e1fica',anomalia:'Anomalia',dispersao:'Dispers\u00e3o',empresa:'Empresa Destaque',evolucao:'Evolu\u00e7\u00e3o'};
        const groups={}; ins.forEach(i=>{const cat=i.category||'outros';if(!groups[cat])groups[cat]=[];groups[cat].push(i);});
        let h=''; for(const cat of['contexto','anomalia','empresa','geo','evolucao','dispersao']){
            if(!groups[cat])continue;
            h+=`<h4 style="color:var(--text2);font-size:.75rem;text-transform:uppercase;letter-spacing:.05em;margin:.8rem 0 .3rem;border-bottom:1px solid var(--border);padding-bottom:.2rem;">${catL[cat]||cat}</h4>`;
            groups[cat].forEach(i=>{const cl=sevC[i.severity]||sevC.info;const bl=sevL[i.severity]||'Info';
                h+=`<div class="insight-card severity-${i.severity}"><div class="insight-header"><i class="fas ${i.icon||'fa-lightbulb'}" style="color:${cl};font-size:.85rem;"></i><span class="insight-title">${i.title}</span><span class="insight-badge" style="background:${cl}22;color:${cl};">${bl}</span></div><div class="insight-text">${i.text}</div></div>`;
            });
        } c.innerHTML=h;
    }

    // ====================================
    // CHARTS
    // ====================================
    function renderChartsTab(data) {
        const c=document.getElementById('tab-charts'), cd=data.chart_data||{};
        c.innerHTML=`<div class="charts-grid">
            <div class="chart-panel"><div class="chart-title"><i class="fas fa-industry"></i> EV/EBITDA por Ind\u00fastria</div><div class="chart-container"><canvas id="cInd"></canvas></div></div>
            <div class="chart-panel"><div class="chart-title"><i class="fas fa-globe"></i> EV/EBITDA por Regi\u00e3o</div><div class="chart-container"><canvas id="cReg"></canvas></div></div>
            <div class="chart-panel"><div class="chart-title"><i class="fas fa-flag"></i> EV/EBITDA por Pa\u00eds</div><div class="chart-container"><canvas id="cCtry"></canvas></div></div>
            <div class="chart-panel"><div class="chart-title"><i class="fas fa-chart-area"></i> Distribui\u00e7\u00e3o EV/EBITDA</div><div class="chart-container"><canvas id="cDist"></canvas></div></div>
        </div>`;
        Object.values(chartInstances).forEach(ci=>{try{ci.destroy();}catch(e){}});chartInstances={};
        const opts=(lbl)=>({indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{grid:{color:'#1e3448'},ticks:{color:'#8899aa'}},y:{grid:{display:false},ticks:{color:'#e8edf2',font:{size:10}}}}});
        const ind=(cd.industry_chart||[]).filter(x=>x.median!=null);
        if(ind.length)chartInstances.ind=new Chart(document.getElementById('cInd'),{type:'bar',data:{labels:ind.map(x=>x.label),datasets:[{label:'Mediana',data:ind.map(x=>x.median),backgroundColor:'rgba(0,212,170,.5)',borderColor:'#00d4aa',borderWidth:1}]},options:opts()});
        const reg=(cd.region_chart||[]).filter(x=>x.median!=null);
        if(reg.length)chartInstances.reg=new Chart(document.getElementById('cReg'),{type:'bar',data:{labels:reg.map(x=>`${x.label} (${x.n})`),datasets:[{label:'Mediana',data:reg.map(x=>x.median),backgroundColor:'rgba(52,152,219,.5)',borderColor:'#3498db',borderWidth:1}]},options:opts()});
        const ctry=(cd.country_chart||[]).filter(x=>x.median!=null);
        if(ctry.length)chartInstances.ctry=new Chart(document.getElementById('cCtry'),{type:'bar',data:{labels:ctry.map(x=>`${x.label} (${x.n})`),datasets:[{label:'Mediana',data:ctry.map(x=>x.median),backgroundColor:'rgba(155,89,182,.5)',borderColor:'#9b59b6',borderWidth:1}]},options:opts()});
        const dist=cd.distribution||[];
        if(dist.length>5){const b=buildHist(dist,20);chartInstances.dist=new Chart(document.getElementById('cDist'),{type:'bar',data:{labels:b.labels,datasets:[{label:'Empresas',data:b.counts,backgroundColor:'rgba(0,212,170,.35)',borderColor:'#00d4aa',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{grid:{color:'#1e3448'},ticks:{color:'#8899aa',font:{size:9}}},y:{grid:{color:'#1e3448'},ticks:{color:'#8899aa'}}}}});}
    }
    function buildHist(vals,n){const s=vals.filter(v=>isFinite(v)).sort((a,b)=>a-b),p5=s[Math.floor(s.length*.02)],p95=s[Math.floor(s.length*.98)],f=s.filter(v=>v>=p5&&v<=p95),mn=f[0],mx=f[f.length-1],st=(mx-mn)/n,l=[],c=[];for(let i=0;i<n;i++){const lo=mn+i*st,hi=lo+st;l.push(lo.toFixed(1)+'x');c.push(f.filter(v=>v>=lo&&(i===n-1?v<=hi:v<hi)).length);}return{labels:l,counts:c};}

    // ====================================
    // EVOLUTION TAB
    // ====================================
    function renderEvolutionTab(evo) {
        const c = document.getElementById('tab-evolution');
        if (!evo || !evo.success || !evo.evolution) { c.innerHTML='<div class="empty-state">Sem dados de evolu\u00e7\u00e3o</div>'; return; }
        const years = evo.evolution.filter(y=>y.n>0);
        if(years.length<2){c.innerHTML='<div class="empty-state">Dados insuficientes</div>';return;}
        // Trend badges
        let trendsHtml = '<div style="display:flex;flex-wrap:wrap;gap:.6rem;margin-bottom:1rem;">';
        const tLabels = {ev_ebitda:'EV/EBITDA',ev_revenue:'EV/Vendas',fcf_revenue:'FCF/Vendas',fcf_ebitda:'FCF/EBITDA'};
        for(const[k,v] of Object.entries(evo.trends||{})){
            const cls=v.direction==='up'?'trend-up':v.direction==='down'?'trend-down':'trend-stable';
            const icon=v.direction==='up'?'fa-arrow-up':v.direction==='down'?'fa-arrow-down':'fa-minus';
            trendsHtml+=`<div style="background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:.6rem .9rem;min-width:180px;"><div style="font-size:.7rem;color:var(--text2);text-transform:uppercase;">${tLabels[k]||k}</div><div style="font-size:1.1rem;font-weight:700;color:var(--text);margin:.2rem 0;">${v.start_val}x \u2192 ${v.end_val}x</div><span class="trend-badge ${cls}"><i class="fas ${icon}"></i> ${v.change_pct>0?'+':''}${v.change_pct}% (${v.start_year}\u2013${v.end_year})</span></div>`;
        }
        trendsHtml+='</div>';
        c.innerHTML = trendsHtml + `
            <div class="charts-grid">
                <div class="chart-panel"><div class="chart-title"><i class="fas fa-chart-line"></i> EV/EBITDA \u2014 Evolu\u00e7\u00e3o</div><div class="chart-container"><canvas id="evoEvEbitda"></canvas></div></div>
                <div class="chart-panel"><div class="chart-title"><i class="fas fa-chart-line"></i> EV/Vendas \u2014 Evolu\u00e7\u00e3o</div><div class="chart-container"><canvas id="evoEvRev"></canvas></div></div>
                <div class="chart-panel"><div class="chart-title"><i class="fas fa-chart-line"></i> FCF/Vendas \u2014 Evolu\u00e7\u00e3o</div><div class="chart-container"><canvas id="evoFcfRev"></canvas></div></div>
                <div class="chart-panel"><div class="chart-title"><i class="fas fa-chart-line"></i> FCF/EBITDA \u2014 Evolu\u00e7\u00e3o</div><div class="chart-container"><canvas id="evoFcfEbitda"></canvas></div></div>
            </div>
            <div style="margin-top:.8rem;overflow-x:auto;"><table class="data-table"><thead><tr><th>Ano</th><th>N</th><th>EV/EBITDA Med</th><th>P25</th><th>P75</th><th>EV/Vendas Med</th><th>FCF/Vendas Med</th><th>FCF/EBITDA Med</th></tr></thead><tbody id="evoTable"></tbody></table></div>`;
        const labels = years.map(y=>y.year);
        const mkDataset = (metric, color) => ({
            labels,
            datasets: [
                {label:'Mediana',data:years.map(y=>y[metric]?.median??null),borderColor:color,backgroundColor:color+'33',fill:false,tension:.3,pointRadius:4,borderWidth:2},
                {label:'P25',data:years.map(y=>y[metric]?.p25??null),borderColor:color+'66',borderDash:[5,5],fill:false,tension:.3,pointRadius:2,borderWidth:1},
                {label:'P75',data:years.map(y=>y[metric]?.p75??null),borderColor:color+'66',borderDash:[5,5],fill:'-1',backgroundColor:color+'11',tension:.3,pointRadius:2,borderWidth:1}
            ]
        });
        const lineOpts = {responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#8899aa',font:{size:10}}}},scales:{x:{grid:{color:'#1e3448'},ticks:{color:'#e8edf2'}},y:{grid:{color:'#1e3448'},ticks:{color:'#8899aa'}}}};
        ['evoEvEbitda','evoEvRev','evoFcfRev','evoFcfEbitda'].forEach(id=>{if(chartInstances[id])try{chartInstances[id].destroy();}catch(e){}});
        chartInstances.evoEvEbitda = new Chart(document.getElementById('evoEvEbitda'),{type:'line',data:mkDataset('ev_ebitda','#00d4aa'),options:lineOpts});
        chartInstances.evoEvRev = new Chart(document.getElementById('evoEvRev'),{type:'line',data:mkDataset('ev_revenue','#3498db'),options:lineOpts});
        chartInstances.evoFcfRev = new Chart(document.getElementById('evoFcfRev'),{type:'line',data:mkDataset('fcf_revenue','#9b59b6'),options:lineOpts});
        chartInstances.evoFcfEbitda = new Chart(document.getElementById('evoFcfEbitda'),{type:'line',data:mkDataset('fcf_ebitda','#f39c12'),options:lineOpts});
        // Table
        document.getElementById('evoTable').innerHTML = years.map(y=>`<tr><td><strong>${y.year}</strong></td><td>${y.n}</td><td>${fmtX(y.ev_ebitda?.median)}</td><td>${fmtX(y.ev_ebitda?.p25)}</td><td>${fmtX(y.ev_ebitda?.p75)}</td><td>${fmtX(y.ev_revenue?.median)}</td><td>${fmtX(y.fcf_revenue?.median,2)}</td><td>${fmtX(y.fcf_ebitda?.median,2)}</td></tr>`).join('');
    }

    // ====================================
    // INDUSTRIES TAB
    // ====================================
    function renderIndustriesTab(data) {
        const c=document.getElementById('tab-industries'),ind=data.by_industry||[];
        if(!ind.length){c.innerHTML='<div class="empty-state">Sem dados</div>';return;}
        c.innerHTML=`<div style="overflow-x:auto;"><table class="data-table"><thead><tr><th>Ind\u00fastria</th><th>N</th><th>EV/EBITDA</th><th>P25</th><th>P75</th><th>EV/Vendas</th><th>FCF/Vendas</th><th>FCF/EBITDA</th></tr></thead><tbody>`+
        ind.map(i=>`<tr><td>${i.label}</td><td>${i.n}</td><td>${fmtX(sg(i,'ev_ebitda','median'))}</td><td>${fmtX(sg(i,'ev_ebitda','p25'))}</td><td>${fmtX(sg(i,'ev_ebitda','p75'))}</td><td>${fmtX(sg(i,'ev_revenue','median'))}</td><td>${fmtX(sg(i,'fcf_revenue','median'),2)}</td><td>${fmtX(sg(i,'fcf_ebitda','median'),2)}</td></tr>`).join('')+
        '</tbody></table></div>';
    }

    // ====================================
    // GEO TAB
    // ====================================
    function renderGeoTab(data) {
        const c=document.getElementById('tab-geo');let h='';
        const reg=data.by_region||[];
        if(reg.length){h+=`<h4 style="color:var(--accent);font-size:.85rem;margin-bottom:.5rem;"><i class="fas fa-globe"></i> Por Regi\u00e3o</h4><div style="overflow-x:auto;margin-bottom:1rem;"><table class="data-table"><thead><tr><th>Regi\u00e3o</th><th>N</th><th>EV/EBITDA</th><th>EV/Vendas</th><th>FCF/Vendas</th></tr></thead><tbody>`+reg.map(r=>`<tr><td>${r.label}</td><td>${r.n}</td><td>${fmtX(sg(r,'ev_ebitda','median'))}</td><td>${fmtX(sg(r,'ev_revenue','median'))}</td><td>${fmtX(sg(r,'fcf_revenue','median'),2)}</td></tr>`).join('')+'</tbody></table></div>';}
        const ctry=data.by_country||[];
        if(ctry.length){h+=`<h4 style="color:var(--accent);font-size:.85rem;margin-bottom:.5rem;"><i class="fas fa-flag"></i> Por Pa\u00eds (top 15)</h4><div style="overflow-x:auto;"><table class="data-table"><thead><tr><th>Pa\u00eds</th><th>N</th><th>EV/EBITDA</th><th>P25</th><th>P75</th></tr></thead><tbody>`+ctry.map(cc=>`<tr><td>${cc.label}</td><td>${cc.n}</td><td>${fmtX(sg(cc,'ev_ebitda','median'))}</td><td>${fmtX(sg(cc,'ev_ebitda','p25'))}</td><td>${fmtX(sg(cc,'ev_ebitda','p75'))}</td></tr>`).join('')+'</tbody></table></div>';}
        c.innerHTML=h||'<div class="empty-state">Sem dados geogr\u00e1ficos</div>';
    }

    // ====================================
    // COMPANIES TAB
    // ====================================
    function renderCompaniesTab(data) {
        const c=document.getElementById('tab-companies'),top=data.top_companies||[],bot=data.bottom_companies||[];let h='';
        const tbl=(arr)=>`<div style="overflow-x:auto;margin-bottom:.6rem;"><table class="data-table"><thead><tr><th>Ticker</th><th>Empresa</th><th>Ind\u00fastria</th><th>Pa\u00eds</th><th>EV/EBITDA</th><th>EV/Vendas</th><th>Margem</th></tr></thead><tbody>`+arr.map(cc=>`<tr><td><strong>${cc.ticker||'?'}</strong></td><td>${cc.company_name||'?'}</td><td>${cc.yahoo_industry||'?'}</td><td>${cc.country||'?'}</td><td>${fmtX(cc.ev_ebitda)}</td><td>${fmtX(cc.ev_revenue)}</td><td>${fmtPct(cc.ebitda_margin)}</td></tr>`).join('')+'</tbody></table></div>';
        if(top.length)h+=`<h4 style="color:var(--accent);font-size:.85rem;margin-bottom:.4rem;"><i class="fas fa-arrow-up"></i> Top 10 \u2014 Maior EV/EBITDA</h4>`+tbl(top);
        if(bot.length)h+=`<h4 style="color:var(--blue);font-size:.85rem;margin:.6rem 0 .4rem;"><i class="fas fa-arrow-down"></i> Bottom 10 \u2014 Menor EV/EBITDA</h4>`+tbl(bot);
        c.innerHTML=h||'<div class="empty-state">Sem dados</div>';
    }

    // ====================================
    // LLM TAB
    // ====================================
    function renderLlmTab(data) {
        const c=document.getElementById('tab-llm'),llm=data.llm_analysis;
        if(!llm){c.innerHTML=`<div class="empty-state"><i class="fas fa-brain" style="font-size:1.8rem;color:var(--text3);"></i><br><p>An\u00e1lise IA n\u00e3o gerada.</p><small style="color:var(--text3);">Configure uma API Key nas defini\u00e7\u00f5es (\u2699) ou via vari\u00e1vel de ambiente.</small></div>`;document.getElementById('tabLlm').style.display='none';return;}
        document.getElementById('tabLlm').style.display='';
        let h=`<div style="display:flex;align-items:center;gap:.5rem;margin-bottom:.8rem;"><h3 style="color:var(--accent);font-size:.95rem;margin:0;"><i class="fas fa-brain"></i> An\u00e1lise IA</h3><span style="font-size:.65rem;padding:2px 6px;background:rgba(155,89,182,.15);color:var(--purple);border-radius:8px;">${data.llm_provider||'IA'}</span></div>`;
        if(llm.resumo_executivo)h+=`<div class="llm-section"><h4><i class="fas fa-file-alt"></i> Resumo Executivo</h4><div class="llm-text">${safeHtml(llm.resumo_executivo)}</div></div>`;
        if(llm.destaques?.length){h+=`<div class="llm-section"><h4><i class="fas fa-star"></i> Destaques</h4>`;llm.destaques.forEach(d=>{const t=d.tipo||'neutro',ic=t==='positivo'?'fa-check-circle':t==='negativo'?'fa-times-circle':'fa-info-circle',cl=t==='positivo'?'var(--accent)':t==='negativo'?'var(--danger)':'var(--blue)';h+=`<div class="llm-highlight tipo-${t}"><strong><i class="fas ${ic}" style="color:${cl};"></i> ${safeText(d.titulo)}</strong><p>${safeText(d.texto)}</p></div>`;});h+='</div>';}
        if(llm.analise_industrias)h+=`<div class="llm-section"><h4><i class="fas fa-industry"></i> Ind\u00fastrias</h4><div class="llm-text">${safeHtml(llm.analise_industrias)}</div></div>`;
        if(llm.analise_geografica)h+=`<div class="llm-section"><h4><i class="fas fa-globe-americas"></i> Geografia</h4><div class="llm-text">${safeHtml(llm.analise_geografica)}</div></div>`;
        if(llm.empresas_destaque)h+=`<div class="llm-section"><h4><i class="fas fa-building"></i> Empresas</h4><div class="llm-text">${safeHtml(llm.empresas_destaque)}</div></div>`;
        if(llm.riscos_oportunidades?.length){h+=`<div class="llm-section"><h4><i class="fas fa-balance-scale"></i> Riscos e Oportunidades</h4>`;llm.riscos_oportunidades.forEach(ro=>{const is_r=ro.tipo==='risco';h+=`<div class="llm-highlight tipo-${ro.tipo}"><span><i class="fas ${is_r?'fa-exclamation-triangle':'fa-rocket'}" style="color:${is_r?'var(--danger)':'var(--accent)'};"></i> <strong>${is_r?'Risco':'Oportunidade'}:</strong> ${safeText(ro.texto)}</span></div>`;});h+='</div>';}
        if(llm.conclusao)h+=`<div class="llm-section"><h4><i class="fas fa-flag-checkered"></i> Conclus\u00e3o</h4><div class="llm-text">${safeHtml(llm.conclusao)}</div></div>`;
        c.innerHTML=h;
    }
    function safeText(t){if(!t)return '';const d=document.createElement('div');d.textContent=t;return d.innerHTML;}
    function safeHtml(t){return safeText(t).split('\n').filter(p=>p.trim()).map(p=>`<p>${p}</p>`).join('');}

    // ====================================
    // TABS
    // ====================================
    function switchTab(btn,name){document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active'));document.getElementById('tab-'+name).classList.add('active');btn.classList.add('active');}

    // ====================================
    // CHAT
    // ====================================
    function sendSuggestion(el) { document.getElementById('chatInput').value = el.textContent; sendChat(); }

    async function sendChat() {
        const input = document.getElementById('chatInput');
        const text = input.value.trim();
        if (!text) return;
        input.value = '';

        // Add user bubble
        chatHistory.push({role: 'user', content: text});
        addBubble('user', text);

        // Show typing
        const typingId = 'typing-' + Date.now();
        addBubble('assistant', '<div class="typing-dots"><span></span><span></span><span></span></div>', typingId);

        const cfg = getLlmConfig();
        document.getElementById('chatSendBtn').disabled = true;

        try {
            const r = await fetch('/api/estudoanloc/chat', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    provider: cfg.provider, api_key: cfg.api_key,
                    messages: chatHistory, data_context: dataContextStr,
                    custom_instructions: cfg.custom_instructions
                })
            });
            const d = await r.json();
            // Remove typing
            const te = document.getElementById(typingId);
            if (te) te.remove();

            if (d.success) {
                chatHistory.push({role: 'assistant', content: d.message});
                addBubble('assistant', formatChatMessage(d.message));
            } else {
                addBubble('system', 'Erro: ' + d.error);
            }
        } catch(e) {
            const te = document.getElementById(typingId);
            if(te) te.remove();
            addBubble('system', 'Erro de conex\u00e3o: ' + e.message);
        } finally {
            document.getElementById('chatSendBtn').disabled = false;
        }
    }

    function addBubble(role, html, id) {
        const container = document.getElementById('chatMessages');
        const div = document.createElement('div');
        div.className = 'chat-bubble ' + role;
        if (id) div.id = id;
        div.innerHTML = html;
        container.appendChild(div);
        container.scrollTop = container.scrollHeight;
    }

    function formatChatMessage(text) {
        // Sanitize then format: bold, paragraphs
        const safe = safeText(text);
        return safe.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
                   .split('\n').filter(p=>p.trim()).map(p=>`<p>${p}</p>`).join('');
    }
    </script>
</body>
</html>'''

with open(path, 'w', encoding='utf-8', newline='\n') as f:
    f.write(content)

# Verify
with open(path, 'rb') as f:
    raw = f.read(5)
    print('No BOM:', raw[:3] != b'\xef\xbb\xbf')

with open(path, 'r', encoding='utf-8') as f:
    text = f.read()
    for word in ['Configuração', 'Evolução', 'Gráficos', 'Indústria', 'Análise', 'País', 'Região']:
        # These will be in HTML entity form, check source
        pass
    # Check for double-encoding artifacts
    import re
    bad = re.findall(r'├|Ôò|ÔÇ', text)
    print(f'Double-encoding artifacts found: {len(bad)}')
    print(f'File size: {len(text)} chars')
    print('OK: File rewritten successfully')
