// WACC Calculator - Custom JavaScript

// Global variables
let currentCalculation = null;
let chartInstances = {};
let loadingStates = {};

// Initialize application when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

// Main initialization function
function initializeApp() {
    // Initialize tooltips
    initializeTooltips();
    
    // Initialize loading states
    initializeLoadingStates();
    
    // Initialize form validation
    initializeFormValidation();
    
    // Initialize event listeners
    initializeEventListeners();
    
    // Initialize page-specific functionality
    const currentPage = getCurrentPage();
    switch(currentPage) {
        case 'index':
            initializeHomePage();
            break;
        case 'calculator':
            initializeCalculatorPage();
            break;
        case 'dashboard':
            initializeDashboardPage();
            break;
        case 'history':
            initializeHistoryPage();
            break;
    }
}

// Get current page name
function getCurrentPage() {
    const path = window.location.pathname;
    if (path === '/' || path === '/index') return 'index';
    if (path.includes('calculator')) return 'calculator';
    if (path.includes('dashboard')) return 'dashboard';
    if (path.includes('history')) return 'history';
    return 'unknown';
}

// Initialize tooltips
function initializeTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

// Initialize loading states
function initializeLoadingStates() {
    loadingStates = {
        calculation: false,
        marketData: false,
        sectors: false,
        history: false
    };
}

// Initialize form validation
function initializeFormValidation() {
    const forms = document.querySelectorAll('.needs-validation');
    Array.prototype.slice.call(forms).forEach(function(form) {
        form.addEventListener('submit', function(event) {
            if (!form.checkValidity()) {
                event.preventDefault();
                event.stopPropagation();
            }
            form.classList.add('was-validated');
        }, false);
    });
}

// Initialize global event listeners
function initializeEventListeners() {
    // Handle navigation active states
    updateActiveNavigation();
    
    // Handle responsive tables
    handleResponsiveTables();
    
    // Handle keyboard shortcuts
    document.addEventListener('keydown', handleKeyboardShortcuts);
}

// Update active navigation
function updateActiveNavigation() {
    const currentPath = window.location.pathname;
    const navLinks = document.querySelectorAll('.navbar-nav .nav-link');
    
    navLinks.forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('href') === currentPath) {
            link.classList.add('active');
        }
    });
}

// Handle responsive tables
function handleResponsiveTables() {
    const tables = document.querySelectorAll('.table-responsive');
    tables.forEach(table => {
        if (table.scrollWidth > table.clientWidth) {
            table.classList.add('table-scroll-indicator');
        }
    });
}

// Handle keyboard shortcuts
function handleKeyboardShortcuts(event) {
    // Ctrl/Cmd + K for search
    if ((event.ctrlKey || event.metaKey) && event.key === 'k') {
        event.preventDefault();
        const searchInput = document.querySelector('input[type="search"]');
        if (searchInput) {
            searchInput.focus();
        }
    }
    
    // Escape to close modals
    if (event.key === 'Escape') {
        const openModals = document.querySelectorAll('.modal.show');
        openModals.forEach(modal => {
            const modalInstance = bootstrap.Modal.getInstance(modal);
            if (modalInstance) {
                modalInstance.hide();
            }
        });
    }
}

// Home page initialization
function initializeHomePage() {
    loadSystemStatus();
    animateCounters();
}

// Load system status
function loadSystemStatus() {
    showLoading('system-status');
    
    fetch('/api/health')
        .then(response => response.json())
        .then(data => {
            updateSystemStatus(data);
        })
        .catch(error => {
            console.error('Error loading system status:', error);
            showError('system-status', 'Erro ao carregar status do sistema');
        })
        .finally(() => {
            hideLoading('system-status');
        });
}

// Update system status display
function updateSystemStatus(data) {
    const statusContainer = document.getElementById('system-status');
    if (!statusContainer) return;
    
    const statusHtml = `
        <div class="row">
            <div class="col-md-3 mb-3">
                <div class="card text-center ${data.status === 'healthy' ? 'bg-success' : 'bg-warning'} text-white">
                    <div class="card-body">
                        <i class="fas fa-heartbeat fa-2x mb-2"></i>
                        <h5>Sistema</h5>
                        <p class="mb-0">${data.status === 'healthy' ? 'Saudável' : 'Atenção'}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3 mb-3">
                <div class="card text-center ${data.data_sources?.fred ? 'bg-success' : 'bg-danger'} text-white">
                    <div class="card-body">
                        <i class="fas fa-database fa-2x mb-2"></i>
                        <h5>FRED</h5>
                        <p class="mb-0">${data.data_sources?.fred ? 'Online' : 'Offline'}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3 mb-3">
                <div class="card text-center ${data.data_sources?.bcb ? 'bg-success' : 'bg-danger'} text-white">
                    <div class="card-body">
                        <i class="fas fa-chart-line fa-2x mb-2"></i>
                        <h5>BCB</h5>
                        <p class="mb-0">${data.data_sources?.bcb ? 'Online' : 'Offline'}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3 mb-3">
                <div class="card text-center ${data.data_sources?.damodaran ? 'bg-success' : 'bg-danger'} text-white">
                    <div class="card-body">
                        <i class="fas fa-university fa-2x mb-2"></i>
                        <h5>Damodaran</h5>
                        <p class="mb-0">${data.data_sources?.damodaran ? 'Online' : 'Offline'}</p>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    statusContainer.innerHTML = statusHtml;
}

// Animate counters
function animateCounters() {
    const counters = document.querySelectorAll('.counter');
    counters.forEach(counter => {
        const target = parseInt(counter.getAttribute('data-target'));
        const duration = 2000;
        const step = target / (duration / 16);
        let current = 0;
        
        const timer = setInterval(() => {
            current += step;
            if (current >= target) {
                current = target;
                clearInterval(timer);
            }
            counter.textContent = Math.floor(current).toLocaleString();
        }, 16);
    });
}

// Calculator page initialization
function initializeCalculatorPage() {
    loadSectors();
    loadCountries();
    initializeCalculatorForm();
    initializeCalculatorEvents();
}

// Load sectors for calculator
function loadSectors() {
    if (loadingStates.sectors) return;
    
    loadingStates.sectors = true;
    const sectorSelect = document.getElementById('sector');
    
    if (!sectorSelect) {
        loadingStates.sectors = false;
        return;
    }
    
    fetch('/api/get_sectors')
        .then(response => response.json())
        .then(data => {
            populateSectorSelect(sectorSelect, data.sectors || []);
        })
        .catch(error => {
            console.error('Error loading sectors:', error);
            showToast('Erro ao carregar setores', 'error');
        })
        .finally(() => {
            loadingStates.sectors = false;
        });
}

// Load countries for calculator
function loadCountries() {
    const countrySelect = document.getElementById('country');
    
    if (!countrySelect) {
        return;
    }
    
    fetch('/api/get_countries')
        .then(response => response.json())
        .then(data => {
            populateCountrySelect(countrySelect, data.countries || []);
        })
        .catch(error => {
            console.error('Error loading countries:', error);
            showToast('Erro ao carregar países', 'error');
        });
}

// Populate sector select
function populateSectorSelect(selectElement, sectors) {
    selectElement.innerHTML = '<option value="">Selecione um setor...</option>';
    
    sectors.forEach(sector => {
        const option = document.createElement('option');
        option.value = sector;
        option.textContent = sector;
        selectElement.appendChild(option);
    });
}

// Populate country select
function populateCountrySelect(selectElement, countries) {
    selectElement.innerHTML = '<option value="">Selecione um país...</option>';
    
    countries.forEach(country => {
        const option = document.createElement('option');
        option.value = country;
        option.textContent = country;
        selectElement.appendChild(option);
    });
}

// Initialize calculator form
function initializeCalculatorForm() {
    const form = document.getElementById('wacc-form');
    if (!form) return;
    
    // Sync debt/equity ratios
    const debtRatioInput = document.getElementById('debt_ratio');
    const equityRatioDisplay = document.getElementById('equity-ratio-display');
    
    if (debtRatioInput && equityRatioDisplay) {
        debtRatioInput.addEventListener('input', function() {
            const debtRatio = parseFloat(this.value) || 0;
            const equityRatio = Math.max(0, 1 - debtRatio);
            equityRatioDisplay.textContent = (equityRatio * 100).toFixed(1) + '%';
        });
    }
    
    // Auto-fill toggle
    const autoFillToggle = document.getElementById('auto-fill-toggle');
    if (autoFillToggle) {
        autoFillToggle.addEventListener('change', function() {
            toggleAutoFill(this.checked);
        });
    }
}

// Initialize calculator events
function initializeCalculatorEvents() {
    const form = document.getElementById('wacc-form');
    if (!form) return;
    
    form.addEventListener('submit', handleCalculatorSubmit);
    
    // Reset button
    const resetBtn = document.getElementById('reset-btn');
    if (resetBtn) {
        resetBtn.addEventListener('click', resetCalculatorForm);
    }
    
    // Example data button
    const exampleBtn = document.getElementById('example-btn');
    if (exampleBtn) {
        exampleBtn.addEventListener('click', loadExampleData);
    }
}

// Handle calculator form submission
function handleCalculatorSubmit(event) {
    event.preventDefault();
    
    if (loadingStates.calculation) return;
    
    const formData = new FormData(event.target);
    const data = Object.fromEntries(formData.entries());
    
    calculateWACC(data);
}

// Calculate WACC
function calculateWACC(data) {
    loadingStates.calculation = true;
    showLoading('calculation-results');
    
    fetch('/api/calculate_wacc', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data)
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            displayCalculationResults(result.data);
            currentCalculation = result.data;
        } else {
            showError('calculation-results', result.error || 'Erro no cálculo');
        }
    })
    .catch(error => {
        console.error('Error calculating WACC:', error);
        showError('calculation-results', 'Erro ao calcular WACC');
    })
    .finally(() => {
        loadingStates.calculation = false;
        hideLoading('calculation-results');
    });
}

// Display calculation results
function displayCalculationResults(data) {
    const resultsContainer = document.getElementById('calculation-results');
    if (!resultsContainer) return;
    
    const wacc = data.wacc || 0;
    const components = data.components || {};
    
    const resultsHtml = `
        <div class="card">
            <div class="card-header">
                <h5 class="mb-0"><i class="fas fa-calculator me-2"></i>Resultado do Cálculo</h5>
            </div>
            <div class="card-body">
                <div class="row">
                    <div class="col-md-6">
                        <div class="text-center p-4 bg-primary text-white rounded">
                            <h2 class="display-4 mb-0">${(wacc * 100).toFixed(2)}%</h2>
                            <p class="mb-0">WACC Calculado</p>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <h6>Componentes Utilizados:</h6>
                        <ul class="list-unstyled">
                            <li><strong>Taxa Livre de Risco:</strong> ${(components.risk_free_rate * 100 || 0).toFixed(2)}%</li>
                            <li><strong>Beta:</strong> ${components.beta || 'N/A'}</li>
                            <li><strong>Prêmio de Risco de Mercado:</strong> ${(components.market_risk_premium * 100 || 0).toFixed(2)}%</li>
                            <li><strong>Prêmio de Risco País:</strong> ${(components.country_risk_premium * 100 || 0).toFixed(2)}%</li>
                            <li><strong>Custo da Dívida:</strong> ${(components.cost_of_debt * 100 || 0).toFixed(2)}%</li>
                            <li><strong>Taxa de Imposto:</strong> ${(components.tax_rate * 100 || 0).toFixed(2)}%</li>
                            <li><strong>Proporção da Dívida:</strong> ${(components.debt_ratio * 100 || 0).toFixed(1)}%</li>
                        </ul>
                    </div>
                </div>
                <div class="mt-3">
                    <button class="btn btn-success me-2" onclick="saveCalculation()"><i class="fas fa-save me-1"></i>Salvar</button>
                    <button class="btn btn-info me-2" onclick="exportCalculation()"><i class="fas fa-download me-1"></i>Exportar</button>
                    <button class="btn btn-warning" onclick="shareCalculation()"><i class="fas fa-share me-1"></i>Compartilhar</button>
                </div>
            </div>
        </div>
    `;
    
    resultsContainer.innerHTML = resultsHtml;
    resultsContainer.scrollIntoView({ behavior: 'smooth' });
}

// Toggle auto-fill functionality
function toggleAutoFill(enabled) {
    const inputs = document.querySelectorAll('#wacc-form input[type="number"]');
    inputs.forEach(input => {
        if (enabled) {
            input.setAttribute('readonly', 'readonly');
            input.classList.add('bg-light');
        } else {
            input.removeAttribute('readonly');
            input.classList.remove('bg-light');
        }
    });
    
    if (enabled) {
        loadMarketData();
    }
}

// Load market data for auto-fill
function loadMarketData() {
    if (loadingStates.marketData) return;
    
    loadingStates.marketData = true;
    
    fetch('/api/get_market_data')
        .then(response => response.json())
        .then(data => {
            fillMarketData(data);
        })
        .catch(error => {
            console.error('Error loading market data:', error);
            showToast('Erro ao carregar dados de mercado', 'error');
        })
        .finally(() => {
            loadingStates.marketData = false;
        });
}

// Fill form with market data
function fillMarketData(data) {
    const fields = {
        'risk_free_rate': data.risk_free_rate,
        'market_risk_premium': data.market_risk_premium,
        'country_risk_premium': data.country_risk_premium,
        'cost_of_debt': data.cost_of_debt,
        'tax_rate': data.tax_rate
    };
    
    Object.entries(fields).forEach(([fieldId, value]) => {
        const input = document.getElementById(fieldId);
        if (input && value !== undefined) {
            input.value = (value * 100).toFixed(2);
        }
    });
}

// Reset calculator form
function resetCalculatorForm() {
    const form = document.getElementById('wacc-form');
    if (form) {
        form.reset();
        form.classList.remove('was-validated');
    }
    
    const resultsContainer = document.getElementById('calculation-results');
    if (resultsContainer) {
        resultsContainer.innerHTML = '';
    }
    
    currentCalculation = null;
}

// Load example data
function loadExampleData() {
    const exampleData = {
        sector: 'technology',
        country: 'BR',
        risk_free_rate: 10.75,
        beta: 1.2,
        market_risk_premium: 6.0,
        country_risk_premium: 2.5,
        size_premium: 1.0,
        cost_of_debt: 12.0,
        tax_rate: 34.0,
        debt_ratio: 30.0
    };
    
    Object.entries(exampleData).forEach(([fieldId, value]) => {
        const input = document.getElementById(fieldId);
        if (input) {
            input.value = value;
        }
    });
    
    showToast('Dados de exemplo carregados', 'success');
}

// Dashboard page initialization
function initializeDashboardPage() {
    loadDashboardData();
    initializeCharts();
}

// Load dashboard data
function loadDashboardData() {
    loadMarketDataCards();
    loadDataSourceStatus();
    loadRecentCalculations();
}

// Load market data cards
function loadMarketDataCards() {
    fetch('/api/get_market_data')
        .then(response => response.json())
        .then(data => {
            updateMarketDataCards(data);
        })
        .catch(error => {
            console.error('Error loading market data cards:', error);
        });
}

// Update market data cards
function updateMarketDataCards(data) {
    const cards = {
        'risk-free-rate': { value: data.risk_free_rate, suffix: '%' },
        'selic-rate': { value: data.selic_rate, suffix: '%' },
        'market-risk-premium': { value: data.market_risk_premium, suffix: '%' },
        'country-risk': { value: data.country_risk_premium, suffix: '%' }
    };
    
    Object.entries(cards).forEach(([cardId, cardData]) => {
        const card = document.getElementById(cardId);
        if (card && cardData.value !== undefined) {
            const valueElement = card.querySelector('.card-text');
            if (valueElement) {
                valueElement.textContent = (cardData.value * 100).toFixed(2) + cardData.suffix;
            }
        }
    });
}

// Initialize charts
function initializeCharts() {
    initializeHistoricalRatesChart();
    initializeSectorBetasChart();
    initializeWACCCompositionChart();
    initializeWACCBySectorChart();
}

// Initialize historical rates chart
function initializeHistoricalRatesChart() {
    const ctx = document.getElementById('historical-rates-chart');
    if (!ctx) return;
    
    // Sample data - replace with actual API call
    const data = {
        labels: ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun'],
        datasets: [{
            label: 'Taxa Selic',
            data: [10.75, 10.75, 11.25, 11.25, 12.25, 12.75],
            borderColor: 'rgb(75, 192, 192)',
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            tension: 0.1
        }, {
            label: 'Taxa Livre de Risco',
            data: [10.5, 10.5, 11.0, 11.0, 12.0, 12.5],
            borderColor: 'rgb(255, 99, 132)',
            backgroundColor: 'rgba(255, 99, 132, 0.2)',
            tension: 0.1
        }]
    };
    
    chartInstances.historicalRates = new Chart(ctx, {
        type: 'line',
        data: data,
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Evolução das Taxas Históricas'
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    ticks: {
                        callback: function(value) {
                            return value + '%';
                        }
                    }
                }
            }
        }
    });
}

// Initialize sector betas chart
function initializeSectorBetasChart() {
    const ctx = document.getElementById('sector-betas-chart');
    if (!ctx) return;
    
    // Sample data - replace with actual API call
    const data = {
        labels: ['Tecnologia', 'Financeiro', 'Saúde', 'Energia', 'Varejo'],
        datasets: [{
            label: 'Beta',
            data: [1.2, 0.8, 0.9, 1.1, 1.0],
            backgroundColor: [
                'rgba(255, 99, 132, 0.8)',
                'rgba(54, 162, 235, 0.8)',
                'rgba(255, 205, 86, 0.8)',
                'rgba(75, 192, 192, 0.8)',
                'rgba(153, 102, 255, 0.8)'
            ]
        }]
    };
    
    chartInstances.sectorBetas = new Chart(ctx, {
        type: 'bar',
        data: data,
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Beta por Setor'
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Initialize WACC composition chart
function initializeWACCCompositionChart() {
    const ctx = document.getElementById('wacc-composition-chart');
    if (!ctx) return;
    
    // Sample data - replace with actual calculation
    const data = {
        labels: ['Custo do Capital Próprio', 'Custo da Dívida (após IR)'],
        datasets: [{
            data: [70, 30],
            backgroundColor: [
                'rgba(54, 162, 235, 0.8)',
                'rgba(255, 99, 132, 0.8)'
            ]
        }]
    };
    
    chartInstances.waccComposition = new Chart(ctx, {
        type: 'doughnut',
        data: data,
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Composição do WACC'
                },
                legend: {
                    position: 'bottom'
                }
            }
        }
    });
}

// Initialize WACC by sector chart
function initializeWACCBySectorChart() {
    const ctx = document.getElementById('wacc-by-sector-chart');
    if (!ctx) return;
    
    // Sample data - replace with actual API call
    const data = {
        labels: ['Tecnologia', 'Financeiro', 'Saúde', 'Energia', 'Varejo'],
        datasets: [{
            label: 'WACC (%)',
            data: [16.5, 14.2, 15.1, 17.8, 15.9],
            backgroundColor: 'rgba(75, 192, 192, 0.8)',
            borderColor: 'rgba(75, 192, 192, 1)',
            borderWidth: 1
        }]
    };
    
    chartInstances.waccBySector = new Chart(ctx, {
        type: 'bar',
        data: data,
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'WACC Médio por Setor'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return value + '%';
                        }
                    }
                }
            }
        }
    });
}

// History page initialization
function initializeHistoryPage() {
    loadCalculationHistory();
    initializeHistoryFilters();
}

// Load calculation history
function loadCalculationHistory() {
    if (loadingStates.history) return;
    
    loadingStates.history = true;
    showLoading('history-table');
    
    fetch('/api/get_history')
        .then(response => response.json())
        .then(data => {
            displayCalculationHistory(data.calculations || []);
            updateHistoryStatistics(data.statistics || {});
        })
        .catch(error => {
            console.error('Error loading history:', error);
            showError('history-table', 'Erro ao carregar histórico');
        })
        .finally(() => {
            loadingStates.history = false;
            hideLoading('history-table');
        });
}

// Display calculation history
function displayCalculationHistory(calculations) {
    const tableBody = document.querySelector('#history-table tbody');
    if (!tableBody) return;
    
    if (calculations.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="7" class="text-center">Nenhum cálculo encontrado</td></tr>';
        return;
    }
    
    const rows = calculations.map(calc => `
        <tr>
            <td>${new Date(calc.timestamp).toLocaleDateString('pt-BR')}</td>
            <td>${calc.sector || 'N/A'}</td>
            <td>${calc.country || 'N/A'}</td>
            <td>${(calc.wacc * 100).toFixed(2)}%</td>
            <td><span class="badge bg-${calc.status === 'completed' ? 'success' : 'warning'}">${calc.status}</span></td>
            <td>
                <button class="btn btn-sm btn-outline-primary" onclick="viewCalculation('${calc.id}')"><i class="fas fa-eye"></i></button>
                <button class="btn btn-sm btn-outline-secondary" onclick="duplicateCalculation('${calc.id}')"><i class="fas fa-copy"></i></button>
                <button class="btn btn-sm btn-outline-danger" onclick="deleteCalculation('${calc.id}')"><i class="fas fa-trash"></i></button>
            </td>
        </tr>
    `).join('');
    
    tableBody.innerHTML = rows;
}

// Initialize history filters
function initializeHistoryFilters() {
    const filterForm = document.getElementById('history-filters');
    if (!filterForm) return;
    
    filterForm.addEventListener('submit', function(event) {
        event.preventDefault();
        applyHistoryFilters();
    });
    
    const clearFiltersBtn = document.getElementById('clear-filters');
    if (clearFiltersBtn) {
        clearFiltersBtn.addEventListener('click', clearHistoryFilters);
    }
}

// Apply history filters
function applyHistoryFilters() {
    const formData = new FormData(document.getElementById('history-filters'));
    const filters = Object.fromEntries(formData.entries());
    
    // Apply filters to the table
    const rows = document.querySelectorAll('#history-table tbody tr');
    rows.forEach(row => {
        let visible = true;
        
        // Apply sector filter
        if (filters.sector && filters.sector !== '') {
            const sectorCell = row.cells[1].textContent;
            if (!sectorCell.toLowerCase().includes(filters.sector.toLowerCase())) {
                visible = false;
            }
        }
        
        // Apply country filter
        if (filters.country && filters.country !== '') {
            const countryCell = row.cells[2].textContent;
            if (!countryCell.toLowerCase().includes(filters.country.toLowerCase())) {
                visible = false;
            }
        }
        
        row.style.display = visible ? '' : 'none';
    });
}

// Clear history filters
function clearHistoryFilters() {
    const filterForm = document.getElementById('history-filters');
    if (filterForm) {
        filterForm.reset();
    }
    
    // Show all rows
    const rows = document.querySelectorAll('#history-table tbody tr');
    rows.forEach(row => {
        row.style.display = '';
    });
}

// Utility functions

// Show loading indicator
function showLoading(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    const loadingHtml = `
        <div class="text-center p-4">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Carregando...</span>
            </div>
            <p class="mt-2 text-muted">Carregando...</p>
        </div>
    `;
    
    container.innerHTML = loadingHtml;
}

// Hide loading indicator
function hideLoading(containerId) {
    // Loading will be replaced by actual content
}

// Show error message
function showError(containerId, message) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    const errorHtml = `
        <div class="alert alert-danger" role="alert">
            <i class="fas fa-exclamation-triangle me-2"></i>
            ${message}
        </div>
    `;
    
    container.innerHTML = errorHtml;
}

// Show toast notification
function showToast(message, type = 'info') {
    const toastContainer = document.getElementById('toast-container') || createToastContainer();
    
    const toastId = 'toast-' + Date.now();
    const toastHtml = `
        <div id="${toastId}" class="toast align-items-center text-white bg-${type === 'error' ? 'danger' : type}" role="alert">
            <div class="d-flex">
                <div class="toast-body">
                    <i class="fas fa-${getToastIcon(type)} me-2"></i>
                    ${message}
                </div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
            </div>
        </div>
    `;
    
    toastContainer.insertAdjacentHTML('beforeend', toastHtml);
    
    const toastElement = document.getElementById(toastId);
    const toast = new bootstrap.Toast(toastElement, { delay: 5000 });
    toast.show();
    
    // Remove toast element after it's hidden
    toastElement.addEventListener('hidden.bs.toast', function() {
        toastElement.remove();
    });
}

// Create toast container if it doesn't exist
function createToastContainer() {
    const container = document.createElement('div');
    container.id = 'toast-container';
    container.className = 'toast-container position-fixed top-0 end-0 p-3';
    container.style.zIndex = '1055';
    document.body.appendChild(container);
    return container;
}

// Get toast icon based on type
function getToastIcon(type) {
    const icons = {
        success: 'check-circle',
        error: 'exclamation-triangle',
        warning: 'exclamation-triangle',
        info: 'info-circle'
    };
    return icons[type] || 'info-circle';
}

// Format number with locale
function formatNumber(number, decimals = 2) {
    return new Intl.NumberFormat('pt-BR', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(number);
}

// Format percentage
function formatPercentage(number, decimals = 2) {
    return formatNumber(number * 100, decimals) + '%';
}

// Debounce function
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Export functions for global access
window.WACCApp = {
    showLoading,
    hideLoading,
    showError,
    showToast,
    formatNumber,
    formatPercentage,
    calculateWACC,
    loadSectors,
    resetCalculatorForm,
    loadExampleData
};

// Calculation action functions (called from HTML)
function saveCalculation() {
    if (!currentCalculation) {
        showToast('Nenhum cálculo para salvar', 'warning');
        return;
    }
    
    fetch('/api/save-calculation', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(currentCalculation)
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            showToast('Cálculo salvo com sucesso', 'success');
        } else {
            showToast('Erro ao salvar cálculo', 'error');
        }
    })
    .catch(error => {
        console.error('Error saving calculation:', error);
        showToast('Erro ao salvar cálculo', 'error');
    });
}

function exportCalculation() {
    if (!currentCalculation) {
        showToast('Nenhum cálculo para exportar', 'warning');
        return;
    }
    
    const dataStr = JSON.stringify(currentCalculation, null, 2);
    const dataBlob = new Blob([dataStr], { type: 'application/json' });
    const url = URL.createObjectURL(dataBlob);
    
    const link = document.createElement('a');
    link.href = url;
    link.download = `wacc_calculation_${new Date().toISOString().split('T')[0]}.json`;
    link.click();
    
    URL.revokeObjectURL(url);
    showToast('Cálculo exportado com sucesso', 'success');
}

function shareCalculation() {
    if (!currentCalculation) {
        showToast('Nenhum cálculo para compartilhar', 'warning');
        return;
    }
    
    const shareData = {
        title: 'Cálculo WACC',
        text: `WACC calculado: ${(currentCalculation.wacc * 100).toFixed(2)}%`,
        url: window.location.href
    };
    
    if (navigator.share) {
        navigator.share(shareData)
            .then(() => showToast('Cálculo compartilhado', 'success'))
            .catch(error => console.error('Error sharing:', error));
    } else {
        // Fallback: copy to clipboard
        const shareText = `${shareData.title}: ${shareData.text} - ${shareData.url}`;
        navigator.clipboard.writeText(shareText)
            .then(() => showToast('Link copiado para a área de transferência', 'success'))
            .catch(() => showToast('Erro ao copiar link', 'error'));
    }
}

// History action functions
function viewCalculation(id) {
    // Implementation for viewing calculation details
    showToast('Funcionalidade em desenvolvimento', 'info');
}

function duplicateCalculation(id) {
    // Implementation for duplicating calculation
    showToast('Funcionalidade em desenvolvimento', 'info');
}

function deleteCalculation(id) {
    if (confirm('Tem certeza que deseja excluir este cálculo?')) {
        // Implementation for deleting calculation
        showToast('Funcionalidade em desenvolvimento', 'info');
    }
}