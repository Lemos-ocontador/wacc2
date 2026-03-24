#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface Web para Calculadora WACC Automatizada

Esta aplicação Flask fornece uma interface web moderna para:
- Visualizar componentes do WACC em tempo real
- Configurar parâmetros personalizados
- Executar cálculos automatizados
- Visualizar histórico de cálculos

Autor: Sistema Automatizado WACC
Data: 2025-09-24
"""

from flask import Flask, render_template, request, jsonify, send_file, send_from_directory, Response, stream_with_context
import os
import sqlite3

# Carregar variáveis de ambiente do .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Fallback: ler .env manualmente se python-dotenv não estiver instalado
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(_env_path):
        with open(_env_path) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith('#') and '=' in _line:
                    _k, _v = _line.split('=', 1)
                    os.environ.setdefault(_k.strip(), _v.strip())
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from typing import Dict, Any, List
import json as json_module

from wacc_calculator import WACCCalculator, WACCComponents
from data_extractors import WACCDataManager
from wacc_data_connector import WACCDataConnector
from field_categories_manager import FieldCategoriesManager
from data_source_manager import DataSourceManager
from geographic_mappings import GEOGRAPHIC_MAPPING, get_country_region

# Configurar aplicação Flask
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())
app.config['JSON_AS_ASCII'] = False

# Detectar ambiente: Google App Engine vs Local
IS_GAE = os.environ.get('GAE_ENV', '').startswith('standard')

# API Key Anthropic (Claude) - configurar via .env ou variável de ambiente
# Exemplo: ANTHROPIC_API_KEY=sk-ant-... no arquivo .env

# Path centralizado do banco de dados
DB_PATH = os.environ.get('DB_PATH', 'data/damodaran_data_new.db')

def get_db(db_path=None):
    """Abre conexão SQLite. No GAE usa modo immutable para evitar journal no filesystem read-only."""
    path = db_path or DB_PATH
    if IS_GAE:
        abs_path = os.path.abspath(path)
        uri = 'file:' + abs_path + '?immutable=1'
        return sqlite3.connect(uri, uri=True)
    return sqlite3.connect(path)

# No GAE, cache vai para /tmp (filesystem efêmero mas gravável)
if IS_GAE:
    CACHE_DIR = Path("/tmp/cache")
else:
    CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

# DB separado para report_cache (gravável no GAE via /tmp)
CACHE_DB_PATH = '/tmp/report_cache.db' if IS_GAE else DB_PATH

def get_cache_db():
    """Conexão SQLite gravável para operações de cache de relatórios."""
    return sqlite3.connect(CACHE_DB_PATH)

# Configurar logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if IS_GAE:
    logger.info("Rodando no Google App Engine")
else:
    logger.info("Rodando em ambiente local")

# Inicializar calculadora WACC
calculator = WACCCalculator(cache_dir=str(CACHE_DIR))
data_manager = WACCDataManager(cache_dir=str(CACHE_DIR))
wacc_connector = WACCDataConnector()
field_manager = FieldCategoriesManager()
data_source_mgr = DataSourceManager()

# Classe para análise de empresas
class CompanyAnalyzer:
    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
    
    def get_connection(self):
        return get_db(self.db_path)
    def get_companies_data(self, filters=None):
        """Obtém dados das empresas com filtros aplicados"""
        conn = self.get_connection()
        
        query = """
        SELECT 
            dg.*,
            cbd.about
        FROM damodaran_global dg
        LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
        WHERE 1=1
        """
        
        params = []
        
        if filters:
            # Filtros geográficos hierárquicos
            if 'countries' in filters:
                placeholders = ','.join(['?' for _ in filters['countries']])
                query += f" AND dg.country IN ({placeholders})"
                params.extend(filters['countries'])
            elif 'subregions' in filters:
                placeholders = ','.join(['?' for _ in filters['subregions']])
                query += f" AND dg.sub_group IN ({placeholders})"
                params.extend(filters['subregions'])
            elif 'regions' in filters:
                placeholders = ','.join(['?' for _ in filters['regions']])
                query += f" AND dg.broad_group IN ({placeholders})"
                params.extend(filters['regions'])
            
            # Filtros de setor hierárquicos
            if 'industries' in filters:
                placeholders = ','.join(['?' for _ in filters['industries']])
                query += f" AND dg.industry IN ({placeholders})"
                params.extend(filters['industries'])
            elif 'subsectors' in filters:
                placeholders = ','.join(['?' for _ in filters['subsectors']])
                query += f" AND dg.industry_group IN ({placeholders})"
                params.extend(filters['subsectors'])
            elif 'sectors' in filters:
                placeholders = ','.join(['?' for _ in filters['sectors']])
                query += f" AND dg.primary_sector IN ({placeholders})"
                params.extend(filters['sectors'])
            
            # Filtros de market cap
            if 'min_market_cap' in filters:
                query += " AND dg.market_cap >= ?"
                params.append(float(filters['min_market_cap']))
            if 'max_market_cap' in filters:
                query += " AND dg.market_cap <= ?"
                params.append(float(filters['max_market_cap']))
        
        query += " ORDER BY dg.market_cap DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    
    def calculate_benchmarks(self, df, group_by='industry'):
        """Calcula benchmarks estatísticos"""
        if df.empty:
            return []
        
        numeric_columns = ['market_cap', 'revenue', 'net_income', 'roe', 'roa', 'pe_ratio', 'operating_margin', 'beta']
        
        benchmarks = []
        for group_name, group_df in df.groupby(group_by):
            benchmark = {'name': group_name, 'company_count': len(group_df)}
            
            for col in numeric_columns:
                if col in group_df.columns:
                    values = pd.to_numeric(group_df[col], errors='coerce').dropna()
                    if len(values) > 0:
                        benchmark[col] = {
                            'mean': float(values.mean()),
                            'median': float(values.median()),
                            'q1': float(values.quantile(0.25)),
                            'q3': float(values.quantile(0.75)),
                            'min': float(values.min()),
                            'max': float(values.max())
                        }
            
            benchmarks.append(benchmark)
        
        return benchmarks

# Inicializar analisador de empresas
company_analyzer = CompanyAnalyzer()


@app.route('/')
def index():
    """Página inicial da aplicação - Dashboard principal"""
    return render_template('main_dashboard.html')


@app.route('/wacc')
def wacc_page():
    """Página do WACC Calculator"""
    return render_template('wacc_interface.html')


@app.route('/company-analysis')
def company_analysis_page():
    """Página de Análise de Empresas/Benchmark"""
    return render_template('company_analysis.html')

@app.route('/test_filter_debug.html')
def test_filter_debug():
    """Página de teste de debug dos filtros"""
    return send_from_directory('.', 'test_filter_debug.html')


@app.route('/dashboard')
def dashboard():
    """Dashboard principal com visão geral dos componentes WACC."""
    try:
        # Obter status dos extratores
        health_status = data_manager.get_health_status()
        
        # Obter dados mais recentes
        recent_data = data_manager.extract_all_wacc_components()
        
        # Calcular WACC padrão
        default_wacc = calculator.calculate_wacc()
        
        return render_template('dashboard.html', 
                             health_status=health_status,
                             recent_data=recent_data,
                             default_wacc=default_wacc)
    except Exception as e:
        logger.error(f"Erro no dashboard: {e}")
        return render_template('error.html', error=str(e))


@app.route('/calculator')
def calculator_page():
    """Página da calculadora interativa."""
    return render_template('calculator.html')


@app.route('/wacc_interface')
def wacc_interface_page():
    """Página da interface WACC aprimorada."""
    return render_template('wacc_interface.html')


@app.route('/data-updates')
def data_updates_page():
    """Dashboard de atualização de bases de dados."""
    return render_template('data_updates_dashboard.html')


@app.route('/parametros-wacc')
def wacc_parameters_page():
    """Página de documentação dos parâmetros WACC com valores dinâmicos e links de auditoria."""
    return render_template('wacc_parameters.html')


# ===== ROTAS PARA GESTÃO DE FONTES DE DADOS =====

@app.route('/api/data_sources_status', methods=['GET'])
def api_data_sources_status():
    """Retorna status de todas as fontes de dados."""
    try:
        sources = data_source_mgr.get_all_sources_status()
        return jsonify({'success': True, 'sources': sources})
    except Exception as e:
        logger.error(f'Erro ao obter status das fontes: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/update_data_source/<source_id>', methods=['POST'])
def api_update_data_source(source_id):
    """Atualiza uma fonte de dados específica."""
    try:
        result = data_source_mgr.update_source(source_id)
        return jsonify(result)
    except Exception as e:
        logger.error(f'Erro ao atualizar fonte {source_id}: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/update_all_sources', methods=['GET'])
def api_update_all_sources_sse():
    """SSE endpoint — atualiza todas as fontes com progresso em tempo real."""
    def generate():
        for event in data_source_mgr.update_all_sources():
            yield f"data: {json_module.dumps(event, ensure_ascii=False)}\n\n"
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    )


@app.route('/api/data_update_history', methods=['GET'])
def api_data_update_history():
    """Retorna histórico de atualizações."""
    try:
        source_id = request.args.get('source_id')
        limit = int(request.args.get('limit', 20))
        history = data_source_mgr.get_update_history(source_id, limit)
        return jsonify({'success': True, 'history': history})
    except Exception as e:
        logger.error(f'Erro ao obter histórico: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/size_premium_data', methods=['GET'])
def api_size_premium_data():
    """Retorna dados do BDSize.json para auditoria do Size Premium."""
    try:
        import json as _json
        path = Path("static/BDSize.json")
        if not path.exists():
            return jsonify({'success': False, 'error': 'BDSize.json não encontrado'}), 404
        with open(path, 'r', encoding='utf-8') as f:
            data = _json.load(f)
        return jsonify({
            'success': True,
            'fonte': 'Kroll / Duff & Phelps Cost of Capital Navigator',
            'nota': 'Dados atualizados anualmente. Requer input manual do relatório Kroll.',
            'total_decis': len(data),
            'ano_referencia': data[0].get('[ANO_REFER]') if data else None,
            'decis': data,
        })
    except Exception as e:
        logger.error(f'Erro ao carregar BDSize.json: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/calculate_wacc', methods=['POST'])
def api_calculate_wacc():
    """API endpoint para calcular WACC."""
    try:
        data = request.get_json()
        
        # Extrair parâmetros
        sector = data.get('sector')
        country = data.get('country', 'Brazil')
        market_value_equity = data.get('market_value_equity')
        market_value_debt = data.get('market_value_debt')
        custom_components = data.get('custom_components', {})
        
        # Converter valores de string para float se necessário
        if market_value_equity:
            market_value_equity = float(market_value_equity)
        if market_value_debt:
            market_value_debt = float(market_value_debt)
        
        # Converter componentes customizados
        for key, value in custom_components.items():
            if value is not None and value != '':
                custom_components[key] = float(value)
            else:
                custom_components.pop(key, None)
        
        # Calcular WACC
        components = calculator.calculate_wacc(
            sector=sector,
            country=country,
            market_value_equity=market_value_equity,
            market_value_debt=market_value_debt,
            custom_components=custom_components
        )
        
        # Salvar cálculo
        filename = calculator.save_calculation(components)
        
        # Preparar resposta
        result = {
            'success': True,
            'wacc': components.wacc,
            'wacc_percentage': components.wacc * 100,
            'components': {
                'risk_free_rate': components.risk_free_rate,
                'market_risk_premium': components.market_risk_premium,
                'country_risk_premium': components.country_risk_premium,
                'beta': components.beta,
                'size_premium': components.size_premium,
                'cost_of_debt': components.cost_of_debt,
                'tax_rate': components.tax_rate,
                'debt_to_equity': components.debt_to_equity,
                'cost_of_equity': components.cost_of_equity,
                'weight_equity': components.weight_equity,
                'weight_debt': components.weight_debt
            },
            'data_sources': components.data_sources,
            'calculation_date': components.calculation_date,
            'filename': filename,
            'summary': calculator.get_calculation_summary(components)
        }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erro no cálculo WACC: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/get_market_data')
def api_get_market_data():
    """API endpoint para obter dados de mercado em tempo real."""
    try:
        # Extrair componentes atuais
        components = data_manager.extract_all_wacc_components()
        
        # Processar dados para o frontend
        market_data = {}
        for component, data in components.items():
            if isinstance(data, dict):
                market_data[component] = {
                    'value': data.get('data', {}).get('value', 0) if isinstance(data.get('data'), dict) else data.get('data', 0),
                    'source': data.get('source', 'Unknown'),
                    'timestamp': data.get('timestamp', datetime.now().isoformat()),
                    'status': 'success' if data.get('success', False) else 'error'
                }
            else:
                market_data[component] = {
                    'value': data,
                    'source': 'Default',
                    'timestamp': datetime.now().isoformat(),
                    'status': 'default'
                }
        
        return jsonify({
            'success': True,
            'data': market_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter dados de mercado: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/get_sectors')
def api_get_sectors():
    """API endpoint para obter lista de setores disponíveis."""
    try:
        # Lista de setores baseada nos dados do Damodaran
        sectors = [
            "Advertising", "Aerospace/Defense", "Air Transport", "Apparel", 
            "Auto & Truck", "Auto Parts", "Bank (Money Center)", "Banks (Regional)",
            "Beverage (Alcoholic)", "Beverage (Soft)", "Broadcasting", "Brokerage & Investment Banking",
            "Building Materials", "Business & Consumer Services", "Cable TV", "Chemical (Basic)",
            "Chemical (Diversified)", "Chemical (Specialty)", "Coal & Related Energy", "Computer Services",
            "Computers/Peripherals", "Construction Supplies", "Diversified", "Drugs (Biotechnology)",
            "Drugs (Pharmaceutical)", "Education", "Electrical Equipment", "Electronics (Consumer & Office)",
            "Electronics (General)", "Engineering/Construction", "Entertainment", "Environmental & Waste Services",
            "Farming/Agriculture", "Financial Svcs. (Non-bank & Insurance)", "Food Processing", "Food Wholesalers",
            "Furniture/Home Furnishings", "Green & Renewable Energy", "Healthcare Products", "Healthcare Support Services",
            "Homebuilding", "Hospitals/Healthcare Facilities", "Hotel/Gaming", "Household Products",
            "Information Services", "Insurance (General)", "Insurance (Life)", "Insurance (Prop/Cas.)",
            "Investments & Asset Management", "Iron/Steel", "Machinery", "Metals & Mining",
            "Oil/Gas (Integrated)", "Oil/Gas (Production and Exploration)", "Oil/Gas Distribution", "Oilfield Svcs/Equip.",
            "Paper/Forest Products", "Power", "Precious Metals", "Publishing & Newspapers",
            "R.E.I.T.", "Railroad", "Real Estate (Development)", "Real Estate (General/Diversified)",
            "Real Estate (Operations & Services)", "Recreation", "Reinsurance", "Restaurant/Dining",
            "Retail (Automotive)", "Retail (Building Supply)", "Retail (Distributors)", "Retail (General)",
            "Retail (Grocery and Food)", "Retail (Online)", "Retail (Special Lines)", "Rubber& Tires",
            "Semiconductor", "Semiconductor Equip", "Shipbuilding & Marine", "Shoe",
            "Software (Entertainment)", "Software (Internet)", "Software (System & Application)", "Steel",
            "Telecom (Wireless)", "Telecom. Equipment", "Telecom. Services", "Tobacco",
            "Transportation", "Transportation (Railroads)", "Trucking", "Utility (General)",
            "Utility (Water)", "Total Market"
        ]
        
        return jsonify({
            'success': True,
            'sectors': sorted(sectors)
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter setores: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/get_countries')
def api_get_countries():
    """API endpoint para obter lista de países disponíveis."""
    try:
        # Lista de países principais
        countries = [
            "Brazil", "United States", "Argentina", "Chile", "Colombia", "Mexico", "Peru",
            "Canada", "United Kingdom", "Germany", "France", "Italy", "Spain", "China",
            "Japan", "India", "Australia", "South Korea", "Russia", "South Africa"
        ]
        
        return jsonify({
            'success': True,
            'countries': sorted(countries)
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter países: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/history')
def history_page():
    """Página de histórico de cálculos."""
    try:
        # Listar arquivos de cálculo salvos
        calculation_files = list(CACHE_DIR.glob("wacc_calculation_*.json"))
        calculation_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        calculations = []
        for file_path in calculation_files[:20]:  # Últimos 20 cálculos
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                calculations.append({
                    'filename': file_path.name,
                    'date': data.get('calculation_date', ''),
                    'wacc': data.get('calculated_values', {}).get('wacc', 0),
                    'wacc_percentage': data.get('wacc_percentage', 0),
                    'components': data.get('components', {})
                })
            except Exception as e:
                logger.warning(f"Erro ao ler arquivo {file_path}: {e}")
        
        return render_template('history.html', calculations=calculations)
        
    except Exception as e:
        logger.error(f"Erro na página de histórico: {e}")
        return render_template('error.html', error=str(e))


@app.route('/api/get_history')
def api_get_history():
    """API endpoint para obter histórico de cálculos."""
    try:
        # Listar arquivos de cache
        cache_files = list(CACHE_DIR.glob('wacc_calculation_*.json'))
        calculations = []
        
        for file_path in cache_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Extrair informações do cálculo
                calc_info = {
                    'id': file_path.stem,
                    'timestamp': data.get('timestamp', ''),
                    'sector': data.get('sector', ''),
                    'country': data.get('country', ''),
                    'wacc': data.get('wacc', 0),
                    'status': 'completed'
                }
                calculations.append(calc_info)
                
            except Exception as e:
                logger.warning(f"Erro ao ler arquivo {file_path}: {e}")
                continue
        
        # Ordenar por timestamp (mais recente primeiro)
        calculations.sort(key=lambda x: x['timestamp'], reverse=True)
        
        # Calcular estatísticas
        statistics = {
            'total_calculations': len(calculations),
            'average_wacc': sum(c['wacc'] for c in calculations) / len(calculations) if calculations else 0,
            'most_common_sector': 'N/A',
            'last_calculation': calculations[0]['timestamp'] if calculations else 'N/A'
        }
        
        return jsonify({
            'success': True,
            'calculations': calculations,
            'statistics': statistics
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter histórico: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/download_calculation/<filename>')
def api_download_calculation(filename):
    """API endpoint para download de cálculo específico."""
    try:
        file_path = CACHE_DIR / filename
        if file_path.exists():
            return send_file(file_path, as_attachment=True)
        else:
            return jsonify({'error': 'Arquivo não encontrado'}), 404
            
    except Exception as e:
        logger.error(f"Erro no download: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/health')
def api_health():
    """API endpoint para verificar saúde da aplicação."""
    try:
        health_status = data_manager.get_health_status()
        
        return jsonify({
            'success': True,
            'status': 'healthy',
            'extractors': health_status,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro no health check: {e}")
        return jsonify({
            'success': False,
            'status': 'unhealthy',
            'error': str(e)
        }), 500


# ===== NOVAS ROTAS WACC =====

@app.route('/api/get_risk_free_options', methods=['GET'])
def get_risk_free_options():
    """
    Obter opções disponíveis para taxa livre de risco.
    
    Returns:
        JSON com opções de taxa livre de risco (10Y, 30Y, custom)
    """
    try:
        result = wacc_connector.get_risk_free_rate_options()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter opções de taxa livre de risco: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/get_risk_free_rate', methods=['GET'])
def get_risk_free_rate():
    """
    Obter taxa livre de risco específica.
    
    Query Parameters:
        term (str): Prazo da taxa (10y, 30y). Default: 10y
    
    Returns:
        JSON com taxa livre de risco
    """
    try:
        term = request.args.get('term', '10y')
        result = wacc_connector.get_risk_free_rate(term)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter taxa livre de risco: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/get_beta_sectors', methods=['GET'])
def get_beta_sectors():
    """
    Obter setores disponíveis para cálculo de beta.
    
    Returns:
        JSON com lista de setores e suas estatísticas
    """
    try:
        result = wacc_connector.get_available_sectors()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter setores: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/get_sector_beta', methods=['GET'])
def get_sector_beta():
    """
    Obter beta de um setor específico.
    
    Query Parameters:
        sector (str): Nome do setor (obrigatório)
        region (str): Região (global, emkt). Default: global
    
    Returns:
        JSON com beta alavancado e desalavancado do setor
    """
    try:
        sector = request.args.get('sector')
        region = request.args.get('region', 'global')
        
        if not sector:
            return jsonify({
                'success': False,
                'error': 'Parâmetro sector é obrigatório'
            }), 400
        
        result = wacc_connector.get_sector_beta(sector, region)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter beta do setor: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/benchmark_companies', methods=['GET'])
def get_benchmark_companies():
    """
    Retorna empresas para seleção de benchmark com beta e D/E.
    Filtros: industry, region, country, min_market_cap, max_market_cap, search
    """
    try:
        industry = request.args.get('industry')
        if not industry:
            return jsonify({'success': False, 'error': 'Parâmetro industry é obrigatório'}), 400
        
        region = request.args.get('region', 'global')
        countries = request.args.getlist('country')
        min_mc = request.args.get('min_market_cap', type=float)
        max_mc = request.args.get('max_market_cap', type=float)
        search = request.args.get('search', '').strip()
        
        conn = get_db()
        
        query = """
        SELECT dg.company_name, dg.ticker, dg.exchange, dg.country, dg.broad_group,
               CAST(dg.beta AS REAL) as beta,
               CAST(dg.debt_equity AS REAL) as debt_equity,
               CAST(dg.market_cap AS REAL) as market_cap,
               CAST(dg.operating_margin AS REAL) as operating_margin,
               CAST(dg.revenue AS REAL) as revenue,
               CAST(dg.ev_ebitda AS REAL) as ev_ebitda,
               CAST(dg.ev_ebit AS REAL) as ev_ebit,
               CAST(dg.ev_revenue AS REAL) as ev_sales,
               CAST(dg.effective_tax_rate AS REAL) as effective_tax_rate,
               CAST(dg.marginal_tax_rate AS REAL) as marginal_tax_rate,
               CAST(dg.cash_firm_value AS REAL) as cash_firm_value,
               dg.bottom_up_beta_for_sector,
               dg.sic_desc,
               dg.sic_round,
               dg.atividade_anloc,
               cbd.about
        FROM damodaran_global dg
        LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
        WHERE dg.industry = ?
          AND dg.beta IS NOT NULL AND dg.beta != ''
        """
        params = [industry]
        
        if region == 'emkt':
            query += " AND dg.broad_group = 'Emerging Markets'"
        
        if countries:
            placeholders = ','.join(['?' for _ in countries])
            query += f" AND dg.country IN ({placeholders})"
            params.extend(countries)
        
        if min_mc is not None:
            query += " AND CAST(dg.market_cap AS REAL) >= ?"
            params.append(min_mc)
        if max_mc is not None:
            query += " AND CAST(dg.market_cap AS REAL) <= ?"
            params.append(max_mc)
        
        if search:
            query += " AND (dg.company_name LIKE ? OR dg.ticker LIKE ?)"
            params.extend([f'%{search}%', f'%{search}%'])
        
        query += " ORDER BY CAST(dg.market_cap AS REAL) DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if df.empty:
            return jsonify({'success': True, 'companies': [], 'stats': {'total': 0}})
        
        # Calcular beta desalavancado para cada empresa (metodologia Damodaran)
        # Usa marginal_tax_rate de cada empresa (como Damodaran faz)
        df['tax_for_calc'] = df['marginal_tax_rate'].fillna(df['effective_tax_rate']).fillna(0.20)
        df['unlevered_beta'] = df.apply(
            lambda r: round(r['beta'] / (1 + (1 - r['tax_for_calc']) * r['debt_equity']), 4)
            if pd.notna(r['debt_equity']) and r['debt_equity'] >= 0
            else round(r['beta'], 4), axis=1
        )
        
        df = df.replace({np.nan: None})
        
        companies = df.to_dict('records')
        
        # Estatísticas gerais
        valid_betas = df[df['beta'] > 0]['beta']
        valid_de = df[df['debt_equity'].notna() & (df['debt_equity'] >= 0)]['debt_equity']
        valid_bu = df[df['unlevered_beta'] > 0]['unlevered_beta']
        
        stats = {
            'total': len(df),
            'avg_beta': round(valid_betas.mean(), 4) if len(valid_betas) > 0 else None,
            'median_beta': round(valid_betas.median(), 4) if len(valid_betas) > 0 else None,
            'avg_de': round(valid_de.mean(), 4) if len(valid_de) > 0 else None,
            'avg_unlevered_beta': round(valid_bu.mean(), 4) if len(valid_bu) > 0 else None,
        }
        
        return jsonify({'success': True, 'companies': companies, 'stats': stats})
    except Exception as e:
        logger.error(f"Erro ao obter empresas benchmark: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/benchmark_calculate', methods=['POST'])
def calculate_benchmark():
    """
    Calcula βU e D/E médios a partir de empresas selecionadas.
    Body JSON: { tickers: [...], method: 'simple'|'weighted', tax_rate: 0.34 }
    """
    try:
        data = request.get_json()
        if not data or 'tickers' not in data:
            return jsonify({'success': False, 'error': 'tickers é obrigatório'}), 400
        
        tickers = data['tickers']
        method = data.get('method', 'simple')
        # tax_rate agora é por empresa (effective/marginal), não fixo
        
        if len(tickers) < 1:
            return jsonify({'success': False, 'error': 'Selecione pelo menos 1 empresa'}), 400
        
        conn = get_db()
        placeholders = ','.join(['?' for _ in tickers])
        query = f"""
        SELECT dg.company_name, dg.ticker, dg.exchange, dg.country,
               CAST(dg.beta AS REAL) as beta,
               CAST(dg.debt_equity AS REAL) as debt_equity,
               CAST(dg.market_cap AS REAL) as market_cap,
               CAST(dg.operating_margin AS REAL) as operating_margin,
               CAST(dg.ev_ebitda AS REAL) as ev_ebitda,
               CAST(dg.ev_ebit AS REAL) as ev_ebit,
               CAST(dg.ev_revenue AS REAL) as ev_sales,
               CAST(dg.effective_tax_rate AS REAL) as effective_tax_rate,
               CAST(dg.marginal_tax_rate AS REAL) as marginal_tax_rate,
               CAST(dg.cash_firm_value AS REAL) as cash_firm_value,
               dg.bottom_up_beta_for_sector,
               dg.sic_desc,
               dg.sic_round,
               dg.atividade_anloc,
               cbd.about
        FROM damodaran_global dg
        LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
        WHERE dg.ticker IN ({placeholders})
          AND dg.beta IS NOT NULL
        """
        df = pd.read_sql_query(query, conn, params=tickers)
        conn.close()
        
        if df.empty:
            return jsonify({'success': False, 'error': 'Nenhuma empresa encontrada'})
        
        # Calcular beta desalavancado (metodologia Damodaran: marginal_tax_rate por empresa)
        df['debt_equity'] = df['debt_equity'].fillna(0).clip(lower=0)
        df['tax_for_calc'] = df['marginal_tax_rate'].fillna(df['effective_tax_rate']).fillna(0.20)
        df['unlevered_beta'] = df['beta'] / (1 + (1 - df['tax_for_calc']) * df['debt_equity'])
        
        if method == 'weighted' and df['market_cap'].sum() > 0:
            total_mc = df['market_cap'].sum()
            df['weight'] = df['market_cap'] / total_mc
            avg_bu = (df['unlevered_beta'] * df['weight']).sum()
            avg_de = (df['debt_equity'] * df['weight']).sum()
            avg_bl = (df['beta'] * df['weight']).sum()
            avg_tax = (df['tax_for_calc'] * df['weight']).sum()
        else:
            df['weight'] = 1.0 / len(df)
            avg_bu = df['unlevered_beta'].mean()
            avg_de = df['debt_equity'].mean()
            avg_bl = df['beta'].mean()
            avg_tax = df['tax_for_calc'].mean()
        
        # Converter NaN para None (evita NaN no JSON que quebra JSON.parse do browser)
        df = df.replace({np.nan: None})
        
        companies_detail = []
        for _, row in df.iterrows():
            companies_detail.append({
                'company_name': row['company_name'],
                'ticker': row['ticker'],
                'exchange': row.get('exchange'),
                'country': row.get('country'),
                'beta': round(row['beta'], 4),
                'debt_equity': round(row['debt_equity'], 4),
                'unlevered_beta': round(row['unlevered_beta'], 4),
                'market_cap': row['market_cap'],
                'operating_margin': row.get('operating_margin'),
                'ev_ebitda': row.get('ev_ebitda'),
                'ev_ebit': row.get('ev_ebit'),
                'ev_sales': row.get('ev_sales'),
                'about': row.get('about'),
                'sic_desc': row.get('sic_desc'),
                'sic_round': row.get('sic_round'),
                'atividade_anloc': row.get('atividade_anloc'),
                'weight': round(row['weight'], 4),
            })
        
        return jsonify({
            'success': True,
            'benchmark': {
                'unlevered_beta': round(avg_bu, 4),
                'levered_beta_avg': round(avg_bl, 4),
                'debt_equity_avg': round(avg_de, 4),
                'effective_tax_rate': round(avg_tax, 4),
                'companies_used': len(df),
                'method': method,
                'companies': companies_detail
            }
        })
    except Exception as e:
        logger.error(f"Erro ao calcular benchmark: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_country_risk_options', methods=['GET'])
def get_country_risk_options():
    """
    Obter países disponíveis para prêmio de risco.
    
    Returns:
        JSON com lista de países e seus prêmios de risco
    """
    try:
        result = wacc_connector.get_available_countries()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter países: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/get_country_risk', methods=['GET'])
def get_country_risk():
    """
    Obter prêmio de risco de um país específico.
    
    Query Parameters:
        country (str): Nome do país (obrigatório)
    
    Returns:
        JSON com prêmio de risco do país
    """
    try:
        country = request.args.get('country')
        
        if not country:
            return jsonify({
                'success': False,
                'error': 'Parâmetro country é obrigatório'
            }), 400
        
        result = wacc_connector.get_country_risk(country)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter risco do país: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/get_market_risk_premium', methods=['GET'])
def get_market_risk_premium():
    """
    Obter prêmio de risco de mercado.
    
    Returns:
        JSON com prêmio de risco de mercado (ERP)
    """
    try:
        result = wacc_connector.get_market_risk_premium()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter prêmio de risco de mercado: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/get_kd_selic', methods=['GET'])
def get_kd_selic():
    """
    Obter Selic ao vivo da API BCB → Kd (150% Selic).
    Fallback para BDWACC.json.
    """
    try:
        result = wacc_connector.get_selic_live()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter Selic/Kd: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_ipca', methods=['GET'])
def get_ipca():
    """
    Obter IPCA 12m ao vivo da API BCB.
    Fallback para BDWACC.json.
    """
    try:
        result = wacc_connector.get_ipca_live()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter IPCA: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_wacc_all_live', methods=['GET'])
def get_wacc_all_live():
    """
    Retorna TODOS os componentes WACC com status de fonte (live vs fallback).
    Usado para diagnóstico e verificação.
    """
    try:
        wacc_data = wacc_connector._load_wacc_components()
        selic = wacc_connector.get_selic_live()
        ipca = wacc_connector.get_ipca_live()
        
        from wacc_data_connector import (
            FALLBACK_RF, FALLBACK_RM, FALLBACK_CT, FALLBACK_IR,
            FALLBACK_IB, FALLBACK_IA, FALLBACK_CR
        )
        
        components = {
            'RF': {
                'value': wacc_data.get('RF'),
                'raw': wacc_data.get('RF_raw'),
                'ano': wacc_data.get('RF_ano'),
                'source': 'BDWACC.json',
                'is_fallback': wacc_data.get('RF') == FALLBACK_RF,
            },
            'RM': {
                'value': wacc_data.get('RM'),
                'raw': wacc_data.get('RM_raw'),
                'ano': wacc_data.get('RM_ano'),
                'source': 'BDWACC.json',
                'is_fallback': wacc_data.get('RM') == FALLBACK_RM,
            },
            'CR': {
                'value': wacc_data.get('CR'),
                'raw': wacc_data.get('CR_raw'),
                'ano': wacc_data.get('CR_ano'),
                'source': 'BDWACC.json (referência)',
                'is_fallback': wacc_data.get('CR') == FALLBACK_CR,
            },
            'CT_Selic': {
                'selic': selic.get('selic_percentage'),
                'kd': selic.get('kd_percentage'),
                'source': selic.get('source'),
                'date': selic.get('date'),
                'is_live': selic.get('is_live', False),
                'is_fallback': not selic.get('is_live', False) and selic.get('kd_percentage') == FALLBACK_CT,
            },
            'IR': {
                'value': wacc_data.get('IR'),
                'raw': wacc_data.get('IR_raw'),
                'source': 'BDWACC.json',
                'is_fallback': wacc_data.get('IR') == FALLBACK_IR,
            },
            'IB_IPCA': {
                'ipca_12m': ipca.get('ipca_percentage'),
                'source': ipca.get('source'),
                'date': ipca.get('date'),
                'is_live': ipca.get('is_live', False),
                'bdwacc_value': wacc_data.get('IB'),
                'is_fallback': not ipca.get('is_live', False) and wacc_data.get('IB') == FALLBACK_IB,
            },
            'IA': {
                'value': wacc_data.get('IA'),
                'raw': wacc_data.get('IA_raw'),
                'source': 'BDWACC.json',
                'is_fallback': wacc_data.get('IA') == FALLBACK_IA,
            },
        }
        
        any_fallback = any(c.get('is_fallback') for c in components.values())
        
        return jsonify({
            'success': True,
            'components': components,
            'has_fallbacks': any_fallback,
            'summary': {k: '⚠️ FALLBACK' if v.get('is_fallback') else '✅ OK' for k, v in components.items()},
        })
    except Exception as e:
        logger.error(f"Erro get_wacc_all_live: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== ROTAS PARA SIZE PREMIUM =====

@app.route('/api/get_size_premium', methods=['GET'])
def get_size_premium():
    """
    Obter prêmio de tamanho baseado no valor de mercado.
    
    Query Parameters:
        market_cap (float, optional): Valor de mercado da empresa em reais
    
    Returns:
        JSON com prêmio de tamanho aplicável ou todos os decis disponíveis
    """
    try:
        market_cap = request.args.get('market_cap')
        
        if market_cap:
            try:
                market_cap = float(market_cap)
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': 'Valor de mercado deve ser um número válido'
                }), 400
        
        result = wacc_connector.get_size_premium(market_cap)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erro ao obter size premium: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/get_size_deciles', methods=['GET'])
def get_size_deciles():
    """
    Obter todos os decis de tamanho disponíveis.
    
    Returns:
        JSON com todos os decis de size premium
    """
    try:
        result = wacc_connector.get_size_premium()
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erro ao obter decis de tamanho: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/get_wacc_components', methods=['GET'])
def get_wacc_components():
    """
    Obter todos os componentes WACC de uma vez.
    
    Query Parameters:
        sector (str): Setor da empresa (obrigatório)
        country (str): País da empresa. Default: Brazil
        region (str): Região para beta (global, emkt). Default: global
    
    Returns:
        JSON com todos os componentes WACC
    """
    try:
        sector = request.args.get('sector')
        country = request.args.get('country', 'Brazil')
        region = request.args.get('region', 'global')
        
        if not sector:
            return jsonify({
                'success': False,
                'error': 'Parâmetro sector é obrigatório'
            }), 400
        
        result = wacc_connector.get_wacc_components(sector, country, region)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao obter componentes WACC: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/calculate_unlevered_beta', methods=['POST'])
def calculate_unlevered_beta():
    """
    Calcular beta desalavancado a partir de beta alavancado.
    
    JSON Body:
        levered_beta (float): Beta alavancado
        debt_equity_ratio (float): Relação Dívida/Patrimônio
        tax_rate (float): Taxa de imposto. Default: 0.34
    
    Returns:
        JSON com beta desalavancado
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'JSON body é obrigatório'
            }), 400
        
        levered_beta = data.get('levered_beta')
        debt_equity = data.get('debt_equity_ratio')
        tax_rate = data.get('tax_rate', 0.34)
        
        if levered_beta is None or debt_equity is None:
            return jsonify({
                'success': False,
                'error': 'levered_beta e debt_equity_ratio são obrigatórios'
            }), 400
        
        # Fórmula: βU = βL / [1 + (1 - T) × (D/E)]
        unlevered_beta = levered_beta / (1 + (1 - tax_rate) * debt_equity)
        
        return jsonify({
            'success': True,
            'levered_beta': levered_beta,
            'unlevered_beta': round(unlevered_beta, 4),
            'debt_equity_ratio': debt_equity,
            'tax_rate': tax_rate,
            'formula': 'βU = βL / [1 + (1 - T) × (D/E)]'
        })
        
    except Exception as e:
        logger.error(f"Erro ao calcular beta desalavancado: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500


@app.route('/api/validate_wacc_data', methods=['GET'])
def validate_wacc_data():
    """
    Validar disponibilidade e qualidade dos dados WACC.
    
    Returns:
        JSON com status de validação dos dados
    """
    try:
        # Testar cada componente
        validation = {
            'success': True,
            'timestamp': pd.Timestamp.now().isoformat(),
            'components': {}
        }
        
        # 1. Testar taxa livre de risco
        rf_test = wacc_connector.get_risk_free_rate('10y')
        validation['components']['risk_free_rate'] = {
            'available': rf_test['success'],
            'current_value': rf_test.get('rate_percentage', 'N/A'),
            'source': 'FRED/Damodaran'
        }
        
        # 2. Testar setores
        sectors_test = wacc_connector.get_available_sectors()
        validation['components']['sectors'] = {
            'available': sectors_test['success'],
            'count': sectors_test.get('total_sectors', 0),
            'source': 'Damodaran Global'
        }
        
        # 3. Testar países
        countries_test = wacc_connector.get_available_countries()
        validation['components']['countries'] = {
            'available': countries_test['success'],
            'count': countries_test.get('total_countries', 0),
            'source': 'Damodaran Country Risk'
        }
        
        # 4. Testar prêmio de mercado
        mrp_test = wacc_connector.get_market_risk_premium()
        validation['components']['market_risk_premium'] = {
            'available': mrp_test['success'],
            'current_value': mrp_test.get('market_risk_premium_percentage', 'N/A'),
            'source': 'Damodaran ERP'
        }
        
        # Status geral
        all_available = all([
            validation['components']['risk_free_rate']['available'],
            validation['components']['sectors']['available'],
            validation['components']['countries']['available'],
            validation['components']['market_risk_premium']['available']
        ])
        
        validation['overall_status'] = 'healthy' if all_available else 'degraded'
        validation['success'] = all_available
        
        return jsonify(validation)
        
    except Exception as e:
        logger.error(f"Erro na validação dos dados: {e}")
        return jsonify({
            'success': False,
            'overall_status': 'error',
            'error': 'Erro interno do servidor'
        }), 500


# ===== NOVAS ROTAS PARA SUBDIVISÕES NATIVAS =====

@app.route('/api/get_broad_groups', methods=['GET'])
def get_broad_groups():
    """Obter grupos geográficos amplos (Broad Groups) disponíveis."""
    try:
        import sqlite3
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                broad_group,
                COUNT(*) as company_count,
                COUNT(DISTINCT country) as country_count
            FROM damodaran_global 
            WHERE broad_group IS NOT NULL AND broad_group != ''
            GROUP BY broad_group
            ORDER BY company_count DESC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        broad_groups = []
        for row in results:
            broad_groups.append({
                'name': row[0],
                'company_count': row[1],
                'country_count': row[2]
            })
        
        return jsonify({
            'success': True,
            'broad_groups': broad_groups,
            'total': len(broad_groups)
        })
        
    except Exception as e:
        logger.error(f'Erro ao obter broad groups: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_industry_groups', methods=['GET'])
def get_industry_groups():
    """Obter grupos de indústria disponíveis."""
    try:
        import sqlite3
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                industry_group,
                primary_sector,
                COUNT(*) as company_count,
                AVG(CASE WHEN beta IS NOT NULL AND beta != '' 
                    THEN CAST(beta as REAL) ELSE NULL END) as avg_beta
            FROM damodaran_global 
            WHERE industry_group IS NOT NULL AND industry_group != ''
            GROUP BY industry_group, primary_sector
            ORDER BY company_count DESC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        industry_groups = []
        for row in results:
            industry_groups.append({
                'industry_group': row[0],
                'primary_sector': row[1],
                'company_count': row[2],
                'avg_beta': round(row[3], 3) if row[3] else None
            })
        
        return jsonify({
            'success': True,
            'industry_groups': industry_groups,
            'total': len(industry_groups)
        })
        
    except Exception as e:
        logger.error(f'Erro ao obter industry groups: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_sub_groups', methods=['GET'])
def get_sub_groups():
    """Obter subgrupos regionais disponíveis."""
    try:
        import sqlite3
        broad_group = request.args.get('broad_group')
        
        conn = get_db()
        cursor = conn.cursor()
        
        if broad_group:
            cursor.execute('''
                SELECT 
                    sub_group,
                    COUNT(*) as company_count,
                    COUNT(DISTINCT country) as country_count
                FROM damodaran_global 
                WHERE sub_group IS NOT NULL AND sub_group != ''
                    AND broad_group = ?
                GROUP BY sub_group
                ORDER BY company_count DESC
            ''', (broad_group,))
        else:
            cursor.execute('''
                SELECT 
                    sub_group,
                    broad_group,
                    COUNT(*) as company_count,
                    COUNT(DISTINCT country) as country_count
                FROM damodaran_global 
                WHERE sub_group IS NOT NULL AND sub_group != ''
                GROUP BY sub_group, broad_group
                ORDER BY company_count DESC
            ''')
        
        results = cursor.fetchall()
        conn.close()
        
        sub_groups = []
        for row in results:
            if broad_group:
                sub_groups.append({
                    'name': row[0],
                    'company_count': row[1],
                    'country_count': row[2]
                })
            else:
                sub_groups.append({
                    'name': row[0],
                    'broad_group': row[1],
                    'company_count': row[2],
                    'country_count': row[3]
                })
        
        return jsonify({
            'success': True,
            'sub_groups': sub_groups,
            'total': len(sub_groups),
            'filtered_by': broad_group if broad_group else None
        })
        
    except Exception as e:
        logger.error(f'Erro ao obter sub groups: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_primary_sectors', methods=['GET'])
def get_primary_sectors():
    """Obter setores primários disponíveis."""
    try:
        import sqlite3
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                primary_sector,
                COUNT(*) as company_count,
                COUNT(DISTINCT industry_group) as industry_group_count,
                AVG(CASE WHEN beta IS NOT NULL AND beta != '' 
                    THEN CAST(beta as REAL) ELSE NULL END) as avg_beta
            FROM damodaran_global 
            WHERE primary_sector IS NOT NULL AND primary_sector != ''
            GROUP BY primary_sector
            ORDER BY company_count DESC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        primary_sectors = []
        for row in results:
            primary_sectors.append({
                'name': row[0],
                'company_count': row[1],
                'industry_group_count': row[2],
                'avg_beta': round(row[3], 3) if row[3] else None
            })
        
        return jsonify({
            'success': True,
            'primary_sectors': primary_sectors,
            'total': len(primary_sectors)
        })
        
    except Exception as e:
        logger.error(f'Erro ao obter primary sectors: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_subdivision_hierarchy', methods=['GET'])
def get_subdivision_hierarchy():
    """Obter hierarquia completa de subdivisões."""
    try:
        import sqlite3
        conn = get_db()
        cursor = conn.cursor()
        
        # Hierarquia geográfica
        cursor.execute('''
            SELECT 
                broad_group,
                sub_group,
                country,
                COUNT(*) as company_count
            FROM damodaran_global 
            WHERE broad_group IS NOT NULL AND broad_group != ''
                AND sub_group IS NOT NULL AND sub_group != ''
                AND country IS NOT NULL AND country != ''
            GROUP BY broad_group, sub_group, country
            ORDER BY broad_group, sub_group, company_count DESC
        ''')
        
        geo_results = cursor.fetchall()
        
        # Hierarquia setorial
        cursor.execute('''
            SELECT 
                primary_sector,
                industry_group,
                industry,
                COUNT(*) as company_count,
                AVG(CASE WHEN beta IS NOT NULL AND beta != '' 
                    THEN CAST(beta as REAL) ELSE NULL END) as avg_beta
            FROM damodaran_global 
            WHERE primary_sector IS NOT NULL AND primary_sector != ''
                AND industry_group IS NOT NULL AND industry_group != ''
                AND industry IS NOT NULL AND industry != ''
            GROUP BY primary_sector, industry_group, industry
            ORDER BY primary_sector, industry_group, company_count DESC
        ''')
        
        sector_results = cursor.fetchall()
        conn.close()
        
        # Organizar hierarquia geográfica
        geo_hierarchy = {}
        for row in geo_results:
            broad_group, sub_group, country, count = row
            if broad_group not in geo_hierarchy:
                geo_hierarchy[broad_group] = {}
            if sub_group not in geo_hierarchy[broad_group]:
                geo_hierarchy[broad_group][sub_group] = {}
            geo_hierarchy[broad_group][sub_group][country] = count
        
        # Organizar hierarquia setorial
        sector_hierarchy = {}
        for row in sector_results:
            primary_sector, industry_group, industry, count, beta = row
            if primary_sector not in sector_hierarchy:
                sector_hierarchy[primary_sector] = {}
            if industry_group not in sector_hierarchy[primary_sector]:
                sector_hierarchy[primary_sector][industry_group] = {}
            sector_hierarchy[primary_sector][industry_group][industry] = {
                'company_count': count,
                'avg_beta': round(beta, 3) if beta else None
            }
        
        return jsonify({
            'success': True,
            'geographic_hierarchy': geo_hierarchy,
            'sector_hierarchy': sector_hierarchy,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f'Erro ao obter hierarquia: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


# APIs para análise de empresas
@app.route('/api/hierarchy')
def get_hierarchy():
    """API para obter hierarquia de filtros para o frontend"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Hierarquia geográfica
        cursor.execute('''
            SELECT 
                broad_group,
                sub_group,
                country,
                COUNT(*) as company_count
            FROM damodaran_global 
            WHERE broad_group IS NOT NULL AND broad_group != ''
                AND sub_group IS NOT NULL AND sub_group != ''
                AND country IS NOT NULL AND country != ''
            GROUP BY broad_group, sub_group, country
            ORDER BY broad_group, sub_group, company_count DESC
        ''')
        
        geo_results = cursor.fetchall()
        
        # Hierarquia setorial
        cursor.execute('''
            SELECT 
                primary_sector,
                industry_group,
                industry,
                COUNT(*) as company_count
            FROM damodaran_global 
            WHERE primary_sector IS NOT NULL AND primary_sector != ''
                AND industry_group IS NOT NULL AND industry_group != ''
                AND industry IS NOT NULL AND industry != ''
            GROUP BY primary_sector, industry_group, industry
            ORDER BY primary_sector, industry_group, company_count DESC
        ''')
        
        sector_results = cursor.fetchall()
        conn.close()
        
        # Organizar hierarquia geográfica
        geo_hierarchy = {}
        for row in geo_results:
            broad_group, sub_group, country, count = row
            if broad_group not in geo_hierarchy:
                geo_hierarchy[broad_group] = {}
            if sub_group not in geo_hierarchy[broad_group]:
                geo_hierarchy[broad_group][sub_group] = []
            geo_hierarchy[broad_group][sub_group].append(country)
        
        # Organizar hierarquia setorial
        industry_hierarchy = {}
        for row in sector_results:
            primary_sector, industry_group, industry, count = row
            if primary_sector not in industry_hierarchy:
                industry_hierarchy[primary_sector] = {}
            if industry_group not in industry_hierarchy[primary_sector]:
                industry_hierarchy[primary_sector][industry_group] = []
            industry_hierarchy[primary_sector][industry_group].append(industry)
        
        return jsonify({
            'geo_hierarchy': geo_hierarchy,
            'industry_hierarchy': industry_hierarchy
        })
        
    except Exception as e:
        logger.error(f'Erro ao obter hierarquia: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/filters')
def get_filters():
    """API para obter opções de filtros"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Obter países únicos
        cursor.execute("SELECT DISTINCT country FROM damodaran_global WHERE country IS NOT NULL ORDER BY country")
        countries = [row[0] for row in cursor.fetchall()]
        
        # Obter indústrias únicas
        cursor.execute("SELECT DISTINCT industry FROM damodaran_global WHERE industry IS NOT NULL ORDER BY industry")
        industries = [row[0] for row in cursor.fetchall()]
        
        # Criar hierarquia geográfica
        cursor.execute("""
            SELECT DISTINCT broad_group, sub_group, country
            FROM damodaran_global 
            WHERE broad_group IS NOT NULL AND sub_group IS NOT NULL AND country IS NOT NULL
            ORDER BY broad_group, sub_group, country
        """)
        geo_results = cursor.fetchall()
        
        geographic_hierarchy = {}
        for broad_group, sub_group, country in geo_results:
            if broad_group not in geographic_hierarchy:
                geographic_hierarchy[broad_group] = {}
            if sub_group not in geographic_hierarchy[broad_group]:
                geographic_hierarchy[broad_group][sub_group] = []
            if country not in geographic_hierarchy[broad_group][sub_group]:
                geographic_hierarchy[broad_group][sub_group].append(country)
        
        # Criar hierarquia de indústrias
        cursor.execute("""
            SELECT DISTINCT primary_sector, industry_group, industry
            FROM damodaran_global 
            WHERE primary_sector IS NOT NULL AND industry_group IS NOT NULL AND industry IS NOT NULL
            ORDER BY primary_sector, industry_group, industry
        """)
        industry_results = cursor.fetchall()
        
        industry_hierarchy = {}
        for primary_sector, industry_group, industry in industry_results:
            if primary_sector not in industry_hierarchy:
                industry_hierarchy[primary_sector] = {}
            if industry_group not in industry_hierarchy[primary_sector]:
                industry_hierarchy[primary_sector][industry_group] = []
            if industry not in industry_hierarchy[primary_sector][industry_group]:
                industry_hierarchy[primary_sector][industry_group].append(industry)
        
        # Atividades Anloc
        cursor.execute("SELECT DISTINCT atividade_anloc FROM damodaran_global WHERE atividade_anloc IS NOT NULL AND atividade_anloc != '' ORDER BY atividade_anloc")
        atividades_anloc = [row[0] for row in cursor.fetchall()]

        conn.close()
        
        return jsonify({
            'success': True,
            'countries': countries,
            'industries': industries,
            'geographic_hierarchy': geographic_hierarchy,
            'industry_hierarchy': industry_hierarchy,
            'atividades_anloc': atividades_anloc
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/companies')
def get_companies():
    """Endpoint para obter dados das empresas com filtros"""
    try:
        # DEBUG: Log dos parâmetros recebidos
        print("=== DEBUG API /api/companies ===")
        print("Todos os parâmetros da requisição:", dict(request.args))
        print("Parâmetros únicos:", {k: v for k, v in request.args.items()})
        print("Parâmetros como lista:", {k: request.args.getlist(k) for k in request.args.keys()})
        
        filters = {}
        
        # Filtros geográficos hierárquicos
        if request.args.getlist('country'):
            filters['countries'] = request.args.getlist('country')
            print("Filtro de países aplicado:", filters['countries'])
        elif request.args.getlist('subregion'):
            filters['subregions'] = request.args.getlist('subregion')
            print("Filtro de sub-regiões aplicado:", filters['subregions'])
        elif request.args.getlist('region'):
            filters['regions'] = request.args.getlist('region')
            print("Filtro de regiões aplicado:", filters['regions'])
        
        # Filtros de setor hierárquicos
        if request.args.getlist('industry'):
            filters['industries'] = request.args.getlist('industry')
            print("Filtro de indústrias aplicado:", filters['industries'])
        elif request.args.getlist('subsector'):
            filters['subsectors'] = request.args.getlist('subsector')
            print("Filtro de subsetores aplicado:", filters['subsectors'])
        elif request.args.getlist('sector'):
            filters['sectors'] = request.args.getlist('sector')
            print("Filtro de setores aplicado:", filters['sectors'])
        
        # Filtros de market cap
        if request.args.get('min_market_cap'):
            filters['min_market_cap'] = request.args.get('min_market_cap')
            print("Filtro de market cap mínimo:", filters['min_market_cap'])
        if request.args.get('max_market_cap'):
            filters['max_market_cap'] = request.args.get('max_market_cap')
            print("Filtro de market cap máximo:", filters['max_market_cap'])
        
        print("Filtros finais enviados para get_companies_data:", filters)
        
        df = company_analyzer.get_companies_data(filters)
        
        print("Resultado do get_companies_data:")
        print(f"  - Tipo: {type(df)}")
        print(f"  - Vazio: {df.empty if hasattr(df, 'empty') else 'N/A'}")
        print(f"  - Tamanho: {len(df) if hasattr(df, '__len__') else 'N/A'}")
        if hasattr(df, 'columns'):
            print(f"  - Colunas: {list(df.columns)}")
        
        if df.empty:
            print("DataFrame vazio - retornando lista vazia")
            return jsonify([])
        
        # Substituir valores NaN por None para serialização JSON válida
        df = df.replace({np.nan: None})
        
        result = df.to_dict('records')
        print(f"Retornando {len(result)} empresas")
        
        return jsonify(result)
        
    except Exception as e:
        print(f"ERRO no endpoint /api/companies: {e}")
        import traceback
        print("Traceback completo:")
        traceback.print_exc()
        logger.error(f"Erro no endpoint /api/companies: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/benchmarks')
def get_benchmarks():
    """API para obter benchmarks por setor ou país"""
    try:
        group_by = request.args.get('group_by', 'industry')
        
        filters = {}
        if request.args.get('country'):
            filters['country'] = request.args.get('country')
        if request.args.get('industry'):
            filters['industry'] = request.args.get('industry')
        
        df = company_analyzer.get_companies_data(filters)
        benchmarks = company_analyzer.calculate_benchmarks(df, group_by)
        
        return jsonify({
            'success': True,
            'benchmarks': benchmarks,
            'group_by': group_by
        })
    
    except Exception as e:
        logger.error(f"Erro no endpoint /api/benchmarks: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/company/<company_name>/analysis')
def get_company_analysis(company_name):
    """API para análise detalhada de uma empresa específica"""
    try:
        # Busca dados da empresa
        df = company_analyzer.get_companies_data()
        company_data = df[df['company_name'] == company_name]
        
        if company_data.empty:
            return jsonify({'success': False, 'error': 'Empresa não encontrada'})
        
        # Substituir valores NaN por None para serialização JSON válida
        company_data = company_data.replace({np.nan: None})
        company = company_data.iloc[0].to_dict()
        
        return jsonify({
            'success': True,
            'company': company
        })
    
    except Exception as e:
        logger.error(f"Erro na análise da empresa {company_name}: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/company_search')
def api_company_search():
    """Busca empresas por ticker ou nome (autocomplete)."""
    try:
        q = request.args.get('q', '').strip()
        if len(q) < 2:
            return jsonify({'success': True, 'results': []})

        conn = get_db()
        rows = conn.execute("""
            SELECT yahoo_code, company_name, yahoo_sector, yahoo_industry, yahoo_country,
                   market_cap, currency
            FROM company_basic_data
            WHERE yahoo_code LIKE ? OR company_name LIKE ?
            ORDER BY
                CASE WHEN yahoo_code LIKE ? THEN 0 ELSE 1 END,
                market_cap DESC NULLS LAST
            LIMIT 15
        """, (f'{q}%', f'%{q}%', f'{q}%')).fetchall()
        conn.close()

        results = []
        for r in rows:
            results.append({
                'code': r[0], 'name': r[1], 'sector': r[2],
                'industry': r[3], 'country': r[4],
                'market_cap': r[5], 'currency': r[6],
            })

        return jsonify({'success': True, 'results': results})
    except Exception as e:
        logger.error(f"Erro na busca de empresas: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/company_profile')
def api_company_profile():
    """API unificada: retorna TODOS os dados de uma empresa (basic, damodaran, histórico)."""
    try:
        code = request.args.get('code', '').strip()
        if not code:
            return jsonify({'success': False, 'error': 'Parâmetro code obrigatório'}), 400

        conn = get_db()

        # 1) company_basic_data — tenta yahoo_code, google_finance_code, ou company_name contendo o code
        row = None
        cols = None
        for sql in [
            "SELECT * FROM company_basic_data WHERE yahoo_code = ?",
            "SELECT * FROM company_basic_data WHERE google_finance_code = ?",
            "SELECT * FROM company_basic_data WHERE company_name LIKE ?",
        ]:
            param = code if 'LIKE' not in sql else f'%{code}%'
            cur = conn.execute(sql, [param])
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            if row:
                break
        basic = dict(zip(cols, row)) if row else {}
        # Resolve yahoo_code for historical queries
        yahoo_code = basic.get('yahoo_code', code)

        # 2) damodaran_global (pode ter por company_name ou ticker)
        dam = {}
        if basic:
            cur2 = conn.execute(
                "SELECT * FROM damodaran_global WHERE company_name = ? ORDER BY year DESC LIMIT 1",
                [basic.get('company_name', '')])
            cols2 = [d[0] for d in cur2.description]
            row2 = cur2.fetchone()
            if row2:
                dam = dict(zip(cols2, row2))
        if not dam:
            # Fallback: buscar pelo code no company_name do damodaran
            cur2b = conn.execute(
                "SELECT * FROM damodaran_global WHERE company_name LIKE ? ORDER BY year DESC LIMIT 1",
                [f'%{code}%'])
            cols2b = [d[0] for d in cur2b.description]
            row2b = cur2b.fetchone()
            if row2b:
                dam = dict(zip(cols2b, row2b))

        # 3) company_financials_historical (annual)
        cur3 = conn.execute(
            "SELECT * FROM company_financials_historical WHERE yahoo_code = ? AND period_type = 'annual' ORDER BY period_date DESC",
            [yahoo_code])
        cols3 = [d[0] for d in cur3.description]
        hist_annual = [dict(zip(cols3, r)) for r in cur3.fetchall()]

        # 4) company_financials_historical (quarterly)
        cur4 = conn.execute(
            "SELECT * FROM company_financials_historical WHERE yahoo_code = ? AND period_type = 'quarterly' ORDER BY period_date DESC",
            [yahoo_code])
        cols4 = [d[0] for d in cur4.description]
        hist_quarterly = [dict(zip(cols4, r)) for r in cur4.fetchall()]

        # 5) ETFs que contêm este ticker (busca em etf_holdings)
        etf_memberships = []
        try:
            ticker_base = yahoo_code.replace('.SA', '')  # VALE3.SA → VALE3
            etf_rows = conn.execute("""
                SELECT h.etf_ticker, e.name, h.weight, e.total_holdings,
                       e.aum, e.data_source, e.category
                FROM etf_holdings h
                JOIN etfs e ON e.ticker = h.etf_ticker
                WHERE h.holding_ticker = ? OR h.holding_ticker = ?
                   OR h.holding_ticker = ? OR h.holding_name LIKE ?
                ORDER BY h.weight DESC
            """, (yahoo_code, ticker_base, code, f'%{ticker_base}%')).fetchall()
            seen = set()
            for r in etf_rows:
                if r[0] not in seen:
                    seen.add(r[0])
                    etf_memberships.append({
                        'etf_ticker': r[0], 'etf_name': r[1],
                        'weight': round(r[2], 4) if r[2] else None,
                        'total_holdings': r[3], 'aum': r[4],
                        'data_source': r[5], 'category': r[6],
                    })
        except Exception:
            pass  # tabela etf_holdings pode não existir

        conn.close()

        # Limpar NaN
        def clean(d):
            for k, v in d.items():
                if isinstance(v, float) and (v != v):
                    d[k] = None
            return d

        basic = clean(basic)
        dam = clean(dam)
        hist_annual = [clean(r) for r in hist_annual]
        hist_quarterly = [clean(r) for r in hist_quarterly]

        return jsonify({
            'success': True,
            'basic': basic,
            'damodaran': dam,
            'historical_annual': hist_annual,
            'historical_quarterly': hist_quarterly,
            'etf_memberships': etf_memberships,
        })
    except Exception as e:
        logger.error(f"Erro em company_profile: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    """Handler para páginas não encontradas."""
    return render_template('error.html', 
                         error="Página não encontrada", 
                         error_code=404), 404


@app.errorhandler(500)
def internal_error(error):
    """Handler para erros internos."""
    return render_template('error.html', 
                         error="Erro interno do servidor", 
                         error_code=500), 500


@app.route('/api/get_field_categories')
def api_get_field_categories():
    """API endpoint para obter categorias de campos disponíveis."""
    try:
        categories_dict = field_manager.get_all_categories()
        # Converter dicionário para array com formato esperado pelo frontend
        categories_array = []
        for category_name, category_data in categories_dict.items():
            categories_array.append({
                'id': category_name,
                'name': category_name,
                'icon': category_data.get('icon', '📊'),
                'description': category_data.get('description', ''),
                'field_count': len(category_data.get('fields', {}))
            })
        
        # Ordenar categorias por nome alfabeticamente
        categories_array.sort(key=lambda x: x['name'])
        
        return jsonify({
            'success': True,
            'categories': categories_array
        })
    except Exception as e:
        logger.error(f"Erro ao obter categorias: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/get_category_fields/<path:category_id>')
def api_get_category_fields(category_id):
    """API endpoint para obter campos de uma categoria específica."""
    try:
        fields_dict = field_manager.get_category_fields(category_id)
        available_fields = set(field_manager.get_available_fields_from_db())
        # Converter dicionário para array com formato esperado pelo frontend
        fields_array = []
        for field_name, field_data in fields_dict.items():
            if field_name not in available_fields:
                continue
            fields_array.append({
                'name': field_name,
                'label': field_data.get('label', field_name),
                'type': field_data.get('type', 'text'),
                'format': field_data.get('format', ''),
                'decimals': field_data.get('decimals', 2)
            })
        
        # Ordenar campos por label alfabeticamente
        fields_array.sort(key=lambda x: x['label'])
        
        return jsonify({
            'success': True,
            'fields': fields_array
        })
    except Exception as e:
        logger.error(f"Erro ao obter campos da categoria {category_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/get_field_info/<field_name>')
def api_get_field_info(field_name):
    """API endpoint para obter informações de um campo específico."""
    try:
        field_info = field_manager.get_field_info(field_name)
        return jsonify({
            'success': True,
            'field': field_info
        })
    except Exception as e:
        logger.error(f"Erro ao obter informações do campo {field_name}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ===== DASHBOARD DADOS YAHOO ===== 

def _build_joined_filters(args):
    """Build WHERE conditions for joined dg/cbd dashboard queries.
    Returns (conditions_list, params_list)."""
    conditions = []
    params = []
    for arg_name, col_expr in [
        ('sectors', 'cbd.yahoo_sector'),
        ('industries', 'cbd.yahoo_industry'),
        ('countries', 'cbd.yahoo_country'),
        ('atividades', 'dg.atividade_anloc'),
        ('sic_descs', 'dg.sic_desc'),
    ]:
        val = args.get(arg_name, '').strip()
        if val:
            items = [v.strip() for v in val.split(',') if v.strip()]
            if items:
                placeholders = ','.join('?' * len(items))
                conditions.append(f"{col_expr} IN ({placeholders})")
                params.extend(items)
    return conditions, params


@app.route('/data-yahoo')
def data_yahoo_page():
    """Dashboard analítico dos dados obtidos do Yahoo Finance."""
    return render_template('data_yahoo.html')


@app.route('/api/yahoo_filter_options')
def api_yahoo_filter_options():
    """Returns distinct values for each filter dimension with counts.
    Supports cross-filtering: when filters are active, other dimensions
    show only values available within those filters."""
    try:
        conn = get_db()
        cur = conn.cursor()

        # Build cross-filter conditions per dimension
        # For each dimension, apply filters from ALL OTHER dimensions
        dim_config = [
            ('sectors', 'cbd.yahoo_sector', 'cbd.yahoo_sector'),
            ('industries', 'cbd.yahoo_industry', 'cbd.yahoo_industry'),
            ('countries', 'cbd.yahoo_country', 'cbd.yahoo_country'),
            ('atividades', 'dg.atividade_anloc', 'dg.atividade_anloc'),
            ('sic_descs', 'dg.sic_desc', 'dg.sic_desc'),
        ]
        filter_map = {
            'sectors': ('cbd.yahoo_sector', request.args.get('sectors', '').strip()),
            'industries': ('cbd.yahoo_industry', request.args.get('industries', '').strip()),
            'countries': ('cbd.yahoo_country', request.args.get('countries', '').strip()),
            'atividades': ('dg.atividade_anloc', request.args.get('atividades', '').strip()),
            'sic_descs': ('dg.sic_desc', request.args.get('sic_descs', '').strip()),
        }
        has_any_filter = any(v for _, v in filter_map.values())

        results = {}
        for dim_name, col_expr, _ in dim_config:
            conds = [f"{col_expr} IS NOT NULL"]
            if dim_name in ('atividades', 'sic_descs'):
                conds.append(f"{col_expr} != ''")
            params = []
            # Apply filters from OTHER dimensions only
            if has_any_filter:
                for other_dim, (other_col, other_val) in filter_map.items():
                    if other_dim == dim_name:
                        continue  # skip own dimension
                    if other_val:
                        items = [v.strip() for v in other_val.split(',') if v.strip()]
                        if items:
                            placeholders = ','.join('?' * len(items))
                            conds.append(f"{other_col} IN ({placeholders})")
                            params.extend(items)

            where = " AND ".join(conds)
            if dim_name in ('atividades', 'sic_descs'):
                query = f"""SELECT {col_expr}, COUNT(*) as cnt
                           FROM damodaran_global dg
                           LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
                           WHERE {where}
                           GROUP BY {col_expr} ORDER BY {col_expr}"""
            else:
                query = f"""SELECT {col_expr}, COUNT(*) as cnt
                           FROM company_basic_data cbd
                           LEFT JOIN damodaran_global dg ON dg.ticker = cbd.ticker
                           WHERE {where}
                           GROUP BY {col_expr} ORDER BY {col_expr}"""
            cur.execute(query, params)
            results[dim_name] = [{'name': r[0], 'count': r[1]} for r in cur.fetchall()]

        conn.close()
        return jsonify({'success': True, 'sectors': results['sectors'],
                        'industries': results['industries'],
                        'countries': results['countries'],
                        'atividades': results['atividades'],
                        'sic_descs': results['sic_descs']})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_summary')
def api_yahoo_dashboard_summary():
    """Retorna resumo geral dos dados Yahoo para os KPI cards."""
    try:
        conn = get_db()
        cur = conn.cursor()
        filter_conds, filter_params = _build_joined_filters(request.args)

        if filter_conds:
            where_clause = " AND ".join(filter_conds)
            query = f"""
                SELECT
                    COUNT(DISTINCT dg.ticker) as total_companies,
                    COUNT(DISTINCT CASE WHEN cbd.about IS NOT NULL AND cbd.about != '' THEN dg.ticker END) as with_about,
                    COUNT(DISTINCT CASE WHEN cbd.yahoo_sector IS NOT NULL THEN dg.ticker END) as with_sector,
                    COUNT(DISTINCT CASE WHEN cbd.yahoo_industry IS NOT NULL THEN dg.ticker END) as with_industry,
                    COUNT(DISTINCT CASE WHEN cbd.yahoo_country IS NOT NULL THEN dg.ticker END) as with_country,
                    COUNT(DISTINCT CASE WHEN cbd.enterprise_value IS NOT NULL AND CAST(cbd.enterprise_value AS TEXT) != '' THEN dg.ticker END) as with_ev,
                    COUNT(DISTINCT CASE WHEN cbd.market_cap IS NOT NULL AND CAST(cbd.market_cap AS TEXT) != '' THEN dg.ticker END) as with_mcap,
                    COUNT(DISTINCT CASE WHEN cbd.currency IS NOT NULL THEN dg.ticker END) as with_currency,
                    COUNT(DISTINCT CASE WHEN cbd.yahoo_website IS NOT NULL AND cbd.yahoo_website != '' THEN dg.ticker END) as with_website,
                    COUNT(DISTINCT cbd.yahoo_sector) as distinct_sectors,
                    COUNT(DISTINCT cbd.yahoo_industry) as distinct_industries,
                    COUNT(DISTINCT cbd.yahoo_country) as distinct_countries,
                    COUNT(DISTINCT cbd.currency) as distinct_currencies,
                    COUNT(DISTINCT dg.atividade_anloc) as distinct_atividades,
                    COUNT(DISTINCT dg.sic_desc) as distinct_sic_descs,
                    MAX(cbd.updated_at) as last_updated
                FROM damodaran_global dg
                LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
                WHERE {where_clause}
            """
            cur.execute(query, filter_params)
            row = cur.fetchone()
            cols = ['total_companies', 'with_about', 'with_sector', 'with_industry',
                    'with_country', 'with_ev', 'with_mcap', 'with_currency', 'with_website',
                    'distinct_sectors', 'distinct_industries', 'distinct_countries',
                    'distinct_currencies', 'distinct_atividades', 'distinct_sic_descs',
                    'last_updated']
            stats = dict(zip(cols, row))
        else:
            stats = {}
            cur.execute("SELECT COUNT(*) FROM company_basic_data")
            stats['total_companies'] = cur.fetchone()[0]

            for col, key in [
                ('about', 'with_about'), ('yahoo_sector', 'with_sector'),
                ('yahoo_industry', 'with_industry'), ('yahoo_country', 'with_country'),
                ('enterprise_value', 'with_ev'), ('market_cap', 'with_mcap'),
                ('currency', 'with_currency'), ('yahoo_website', 'with_website'),
            ]:
                cur.execute(f"SELECT COUNT(*) FROM company_basic_data WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS TEXT)) != ''")
                stats[key] = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT yahoo_sector) FROM company_basic_data WHERE yahoo_sector IS NOT NULL")
            stats['distinct_sectors'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT yahoo_industry) FROM company_basic_data WHERE yahoo_industry IS NOT NULL")
            stats['distinct_industries'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT yahoo_country) FROM company_basic_data WHERE yahoo_country IS NOT NULL")
            stats['distinct_countries'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT currency) FROM company_basic_data WHERE currency IS NOT NULL")
            stats['distinct_currencies'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT atividade_anloc) FROM damodaran_global WHERE atividade_anloc IS NOT NULL")
            stats['distinct_atividades'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT sic_desc) FROM damodaran_global WHERE sic_desc IS NOT NULL AND sic_desc != ''")
            stats['distinct_sic_descs'] = cur.fetchone()[0]

            cur.execute("SELECT MAX(updated_at) FROM company_basic_data")
            stats['last_updated'] = cur.fetchone()[0]

        conn.close()
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_sectors')
def api_yahoo_dashboard_sectors():
    """Métricas agregadas por setor Yahoo."""
    try:
        conn = get_db()
        filter_conds, filter_params = _build_joined_filters(request.args)
        where = "WHERE cbd.yahoo_sector IS NOT NULL"
        if filter_conds:
            where += " AND " + " AND ".join(filter_conds)
        query = f"""
            SELECT 
                cbd.yahoo_sector AS sector,
                COUNT(*) AS count,
                AVG(CAST(dg.pe_ratio AS REAL)) AS avg_pe,
                AVG(CAST(dg.ev_ebitda AS REAL)) AS avg_ev_ebitda,
                AVG(CAST(dg.ev_revenue AS REAL)) AS avg_ev_revenue,
                AVG(CAST(dg.pb_ratio AS REAL)) AS avg_pb,
                AVG(CAST(dg.roe AS REAL)) AS avg_roe,
                AVG(CAST(dg.operating_margin AS REAL)) AS avg_op_margin,
                AVG(CAST(dg.net_profit_margin AS REAL)) AS avg_net_margin,
                AVG(CAST(dg.gross_margin AS REAL)) AS avg_gross_margin,
                AVG(CAST(dg.revenue_growth AS REAL)) AS avg_rev_growth,
                AVG(CAST(dg.beta AS REAL)) AS avg_beta,
                AVG(CAST(dg.dividend_yield AS REAL)) AS avg_div_yield,
                AVG(CAST(dg.debt_equity AS REAL)) AS avg_debt_equity,
                0 AS med_pe
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            {where}
            GROUP BY cbd.yahoo_sector
            ORDER BY COUNT(*) DESC
        """
        df = pd.read_sql_query(query, conn, params=filter_params)
        conn.close()
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return jsonify({'success': True, 'sectors': df.to_dict('records')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_industries')
def api_yahoo_dashboard_industries():
    """Métricas agregadas por indústria Yahoo, filtrável por setor."""
    try:
        sector = request.args.get('sector', '')
        conn = get_db()
        params = []
        where = "WHERE cbd.yahoo_industry IS NOT NULL"
        if sector:
            where += " AND cbd.yahoo_sector = ?"
            params.append(sector)

        filter_conds, filter_params = _build_joined_filters(request.args)
        if filter_conds:
            where += " AND " + " AND ".join(filter_conds)
            params.extend(filter_params)

        query = f"""
            SELECT 
                cbd.yahoo_industry AS industry,
                cbd.yahoo_sector AS sector,
                COUNT(*) AS count,
                AVG(CAST(dg.pe_ratio AS REAL)) AS avg_pe,
                AVG(CAST(dg.ev_ebitda AS REAL)) AS avg_ev_ebitda,
                AVG(CAST(dg.ev_revenue AS REAL)) AS avg_ev_revenue,
                AVG(CAST(dg.pb_ratio AS REAL)) AS avg_pb,
                AVG(CAST(dg.roe AS REAL)) AS avg_roe,
                AVG(CAST(dg.operating_margin AS REAL)) AS avg_op_margin,
                AVG(CAST(dg.net_profit_margin AS REAL)) AS avg_net_margin,
                AVG(CAST(dg.gross_margin AS REAL)) AS avg_gross_margin,
                AVG(CAST(dg.revenue_growth AS REAL)) AS avg_rev_growth,
                AVG(CAST(dg.beta AS REAL)) AS avg_beta,
                AVG(CAST(dg.dividend_yield AS REAL)) AS avg_div_yield,
                AVG(CAST(dg.debt_equity AS REAL)) AS avg_debt_equity
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            {where}
            GROUP BY cbd.yahoo_industry
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
        """
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return jsonify({'success': True, 'industries': df.to_dict('records')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_countries')
def api_yahoo_dashboard_countries():
    """Métricas agregadas por país Yahoo."""
    try:
        conn = get_db()
        filter_conds, filter_params = _build_joined_filters(request.args)
        where = "WHERE cbd.yahoo_country IS NOT NULL"
        if filter_conds:
            where += " AND " + " AND ".join(filter_conds)
        query = f"""
            SELECT 
                cbd.yahoo_country AS country,
                COUNT(*) AS count,
                AVG(CAST(dg.pe_ratio AS REAL)) AS avg_pe,
                AVG(CAST(dg.ev_ebitda AS REAL)) AS avg_ev_ebitda,
                AVG(CAST(dg.roe AS REAL)) AS avg_roe,
                AVG(CAST(dg.operating_margin AS REAL)) AS avg_op_margin,
                AVG(CAST(dg.beta AS REAL)) AS avg_beta,
                AVG(CAST(dg.revenue_growth AS REAL)) AS avg_rev_growth,
                COUNT(DISTINCT cbd.yahoo_sector) AS sectors_count,
                COUNT(DISTINCT cbd.yahoo_industry) AS industries_count
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            {where}
            GROUP BY cbd.yahoo_country
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
        """
        df = pd.read_sql_query(query, conn, params=filter_params)
        conn.close()
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return jsonify({'success': True, 'countries': df.to_dict('records')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_atividades')
def api_yahoo_dashboard_atividades():
    """Métricas agregadas por atividade Anloc."""
    try:
        conn = get_db()
        filter_conds, filter_params = _build_joined_filters(request.args)
        has_cbd_filter = any(c.startswith('cbd.') for c in filter_conds) if filter_conds else False
        join_clause = "LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker" if has_cbd_filter else ""
        where = "WHERE dg.atividade_anloc IS NOT NULL AND dg.atividade_anloc != ''"
        if filter_conds:
            where += " AND " + " AND ".join(filter_conds)
        query = f"""
            SELECT 
                dg.atividade_anloc AS atividade,
                COUNT(*) AS count,
                AVG(CAST(dg.pe_ratio AS REAL)) AS avg_pe,
                AVG(CAST(dg.ev_ebitda AS REAL)) AS avg_ev_ebitda,
                AVG(CAST(dg.ev_revenue AS REAL)) AS avg_ev_revenue,
                AVG(CAST(dg.pb_ratio AS REAL)) AS avg_pb,
                AVG(CAST(dg.roe AS REAL)) AS avg_roe,
                AVG(CAST(dg.operating_margin AS REAL)) AS avg_op_margin,
                AVG(CAST(dg.net_profit_margin AS REAL)) AS avg_net_margin,
                AVG(CAST(dg.gross_margin AS REAL)) AS avg_gross_margin,
                AVG(CAST(dg.revenue_growth AS REAL)) AS avg_rev_growth,
                AVG(CAST(dg.beta AS REAL)) AS avg_beta,
                AVG(CAST(dg.debt_equity AS REAL)) AS avg_debt_equity
            FROM damodaran_global dg
            {join_clause}
            {where}
            GROUP BY dg.atividade_anloc
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
        """
        df = pd.read_sql_query(query, conn, params=filter_params)
        conn.close()
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return jsonify({'success': True, 'atividades': df.to_dict('records')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_cross')
def api_yahoo_dashboard_cross():
    """Análise cruzada setor x país."""
    try:
        sector = request.args.get('sector', '')
        country = request.args.get('country', '')
        conn = get_db()
        params = []
        where = "WHERE cbd.yahoo_sector IS NOT NULL AND cbd.yahoo_country IS NOT NULL"
        if sector:
            where += " AND cbd.yahoo_sector = ?"
            params.append(sector)
        if country:
            where += " AND cbd.yahoo_country = ?"
            params.append(country)

        filter_conds, filter_params = _build_joined_filters(request.args)
        if filter_conds:
            where += " AND " + " AND ".join(filter_conds)
            params.extend(filter_params)

        query = f"""
            SELECT 
                cbd.yahoo_sector AS sector,
                cbd.yahoo_country AS country,
                COUNT(*) AS count,
                AVG(CAST(dg.pe_ratio AS REAL)) AS avg_pe,
                AVG(CAST(dg.ev_ebitda AS REAL)) AS avg_ev_ebitda,
                AVG(CAST(dg.roe AS REAL)) AS avg_roe,
                AVG(CAST(dg.operating_margin AS REAL)) AS avg_op_margin
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            {where}
            GROUP BY cbd.yahoo_sector, cbd.yahoo_country
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
            LIMIT 200
        """
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return jsonify({'success': True, 'cross': df.to_dict('records')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_treemap')
def api_yahoo_dashboard_treemap():
    """Dados para treemap: indústrias agrupadas por setor com métricas."""
    try:
        conn = get_db()
        filter_conds, filter_params = _build_joined_filters(request.args)
        where = "WHERE cbd.yahoo_sector IS NOT NULL AND cbd.yahoo_industry IS NOT NULL"
        if filter_conds:
            where += " AND " + " AND ".join(filter_conds)

        query = f"""
            SELECT
                cbd.yahoo_sector AS sector,
                cbd.yahoo_industry AS industry,
                COUNT(*) AS count,
                COALESCE(SUM(CAST(cbd.market_cap AS REAL)), 0) AS total_market_cap,
                COALESCE(SUM(CAST(cbd.enterprise_value AS REAL)), 0) AS total_ev,
                AVG(CAST(dg.pe_ratio AS REAL)) AS avg_pe,
                AVG(CAST(dg.ev_ebitda AS REAL)) AS avg_ev_ebitda,
                AVG(CAST(dg.operating_margin AS REAL)) AS avg_op_margin,
                AVG(CAST(dg.roe AS REAL)) AS avg_roe,
                AVG(CAST(dg.revenue_growth AS REAL)) AS avg_rev_growth,
                AVG(CAST(dg.beta AS REAL)) AS avg_beta
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            {where}
            GROUP BY cbd.yahoo_sector, cbd.yahoo_industry
            HAVING COUNT(*) >= 1
            ORDER BY cbd.yahoo_sector, COUNT(*) DESC
        """
        df = pd.read_sql_query(query, conn, params=filter_params)
        conn.close()
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return jsonify({'success': True, 'items': df.to_dict('records')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== DRILL-DOWN APIs =====

@app.route('/api/yahoo_drill/companies')
def api_yahoo_drill_companies():
    """Lista paginada de empresas com filtros múltiplos."""
    try:
        sector = request.args.get('sector', '')
        industry = request.args.get('industry', '')
        country = request.args.get('country', '')
        atividade = request.args.get('atividade', '')
        search = request.args.get('search', '')
        sort = request.args.get('sort', 'company_name')
        order = request.args.get('order', 'asc')
        page = max(1, int(request.args.get('page', 1)))
        per_page = min(100, max(10, int(request.args.get('per_page', 50))))
        # Filtro especial: campo com/sem dados
        has_field = request.args.get('has_field', '')  # ex: 'about', 'yahoo_sector'
        has_value = request.args.get('has_value', '')   # '1' = com dados, '0' = sem dados

        conn = get_db()
        params = []
        conditions = []

        if sector:
            conditions.append("cbd.yahoo_sector = ?")
            params.append(sector)
        if industry:
            conditions.append("cbd.yahoo_industry = ?")
            params.append(industry)
        if country:
            conditions.append("cbd.yahoo_country = ?")
            params.append(country)
        if atividade:
            conditions.append("dg.atividade_anloc = ?")
            params.append(atividade)

        # Multi-value global filters
        filter_conds, filter_params = _build_joined_filters(request.args)
        conditions.extend(filter_conds)
        params.extend(filter_params)

        if search:
            conditions.append("(dg.company_name LIKE ? OR dg.ticker LIKE ? OR cbd.yahoo_code LIKE ?)")
            s = f"%{search}%"
            params.extend([s, s, s])
        if has_field and has_value in ('0', '1'):
            field_map = {
                'about': 'cbd.about',
                'yahoo_sector': 'cbd.yahoo_sector',
                'yahoo_industry': 'cbd.yahoo_industry',
                'yahoo_country': 'cbd.yahoo_country',
                'enterprise_value': 'cbd.enterprise_value',
                'market_cap': 'cbd.market_cap',
                'yahoo_website': 'cbd.yahoo_website'
            }
            col = field_map.get(has_field)
            if col:
                if has_value == '1':
                    conditions.append(f"{col} IS NOT NULL AND {col} != ''")
                else:
                    conditions.append(f"({col} IS NULL OR {col} = '')")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Validar sort column
        valid_sorts = {
            'company_name': 'dg.company_name',
            'ticker': 'dg.ticker',
            'yahoo_sector': 'cbd.yahoo_sector',
            'yahoo_industry': 'cbd.yahoo_industry',
            'yahoo_country': 'cbd.yahoo_country',
            'market_cap': 'cbd.market_cap',
            'enterprise_value': 'cbd.enterprise_value',
            'pe_ratio': 'dg.pe_ratio',
            'ev_ebitda': 'dg.ev_ebitda',
            'operating_margin': 'dg.operating_margin'
        }
        sort_col = valid_sorts.get(sort, 'dg.company_name')
        sort_dir = 'DESC' if order.lower() == 'desc' else 'ASC'

        # Count total
        count_query = f"""
            SELECT COUNT(*) FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            {where}
        """
        count_df = pd.read_sql_query(count_query, conn, params=params)
        total = int(count_df.iloc[0, 0])

        # Paginated data
        offset = (page - 1) * per_page
        query = f"""
            SELECT 
                dg.company_name, dg.ticker, cbd.yahoo_code,
                cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country,
                cbd.market_cap, cbd.enterprise_value,
                CAST(dg.pe_ratio AS REAL) AS pe_ratio,
                CAST(dg.ev_ebitda AS REAL) AS ev_ebitda,
                CAST(dg.ev_revenue AS REAL) AS ev_revenue,
                CAST(dg.operating_margin AS REAL) AS operating_margin,
                CAST(dg.beta AS REAL) AS beta,
                dg.atividade_anloc,
                CASE WHEN EXISTS(SELECT 1 FROM company_financials_historical cfh WHERE cfh.yahoo_code = cbd.yahoo_code LIMIT 1) THEN 1 ELSE 0 END AS has_historico
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            {where}
            ORDER BY {sort_col} {sort_dir}
            LIMIT ? OFFSET ?
        """
        params.extend([per_page, offset])
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

        return jsonify({
            'success': True,
            'companies': df.to_dict('records'),
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': (total + per_page - 1) // per_page
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_drill/distribution')
def api_yahoo_drill_distribution():
    """Distribuição de um indicador para um grupo (setor, indústria, país)."""
    try:
        metric = request.args.get('metric', 'pe_ratio')
        sector = request.args.get('sector', '')
        industry = request.args.get('industry', '')
        country = request.args.get('country', '')
        atividade = request.args.get('atividade', '')

        # Validar metric
        valid_metrics = {
            'pe_ratio': ('dg.pe_ratio', 'P/E'),
            'ev_ebitda': ('dg.ev_ebitda', 'EV/EBITDA'),
            'ev_revenue': ('dg.ev_revenue', 'EV/Revenue'),
            'pb_ratio': ('dg.pb_ratio', 'P/B'),
            'operating_margin': ('dg.operating_margin', 'Margem Oper.'),
            'net_profit_margin': ('dg.net_profit_margin', 'Margem Líq.'),
            'gross_margin': ('dg.gross_margin', 'Margem Bruta'),
            'roe': ('dg.roe', 'ROE'),
            'beta': ('dg.beta', 'Beta'),
            'debt_equity': ('dg.debt_equity', 'D/E'),
            'market_cap': ('cbd.market_cap', 'Market Cap')
        }
        if metric not in valid_metrics:
            return jsonify({'success': False, 'error': f'Métrica inválida: {metric}'}), 400

        col_expr, label = valid_metrics[metric]
        conn = get_db()
        params = []
        conditions = [f"CAST({col_expr} AS REAL) IS NOT NULL"]

        if sector:
            conditions.append("cbd.yahoo_sector = ?")
            params.append(sector)
        if industry:
            conditions.append("cbd.yahoo_industry = ?")
            params.append(industry)
        if country:
            conditions.append("cbd.yahoo_country = ?")
            params.append(country)
        if atividade:
            conditions.append("dg.atividade_anloc = ?")
            params.append(atividade)

        # Multi-value global filters
        filter_conds, filter_params = _build_joined_filters(request.args)
        conditions.extend(filter_conds)
        params.extend(filter_params)

        where = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT 
                dg.company_name, dg.ticker,
                CAST({col_expr} AS REAL) AS value
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            {where}
            ORDER BY value
        """
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        if df.empty:
            return jsonify({'success': True, 'label': label, 'stats': None, 'histogram': [], 'top10': [], 'bottom10': []})

        values = df['value'].dropna()

        # Stats
        stats = {
            'count': int(len(values)),
            'mean': float(values.mean()),
            'median': float(values.median()),
            'std': float(values.std()) if len(values) > 1 else 0,
            'min': float(values.min()),
            'max': float(values.max()),
            'p25': float(values.quantile(0.25)),
            'p75': float(values.quantile(0.75))
        }

        # Histogram (10 bins, excluir outliers extremos)
        q01 = values.quantile(0.01)
        q99 = values.quantile(0.99)
        trimmed = values[(values >= q01) & (values <= q99)]
        if len(trimmed) > 0:
            counts, bin_edges = np.histogram(trimmed, bins=min(15, max(5, len(trimmed) // 10)))
            histogram = []
            for i in range(len(counts)):
                histogram.append({
                    'bin_start': round(float(bin_edges[i]), 2),
                    'bin_end': round(float(bin_edges[i + 1]), 2),
                    'count': int(counts[i])
                })
        else:
            histogram = []

        # Top 10 and Bottom 10
        df_sorted = df.dropna(subset=['value']).sort_values('value', ascending=False)
        top10 = df_sorted.head(10).replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict('records')
        bottom10 = df_sorted.tail(10).replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict('records')

        return jsonify({
            'success': True,
            'label': label,
            'metric': metric,
            'stats': stats,
            'histogram': histogram,
            'top10': top10,
            'bottom10': bottom10
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_drill/coverage')
def api_yahoo_drill_coverage():
    """Detalhamento de cobertura: empresas com/sem dados de um campo."""
    try:
        field = request.args.get('field', 'about')
        field_map = {
            'about': ('cbd.about', 'About'),
            'yahoo_sector': ('cbd.yahoo_sector', 'Setor'),
            'yahoo_industry': ('cbd.yahoo_industry', 'Indústria'),
            'yahoo_country': ('cbd.yahoo_country', 'País'),
            'enterprise_value': ('cbd.enterprise_value', 'Enterprise Value'),
            'market_cap': ('cbd.market_cap', 'Market Cap'),
            'yahoo_website': ('cbd.yahoo_website', 'Website')
        }
        if field not in field_map:
            return jsonify({'success': False, 'error': f'Campo inválido: {field}'}), 400

        col, label = field_map[field]
        conn = get_db()

        # Total
        total_df = pd.read_sql_query("""
            SELECT COUNT(*) AS total FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
        """, conn)
        total = int(total_df.iloc[0, 0])

        # Com dados
        with_df = pd.read_sql_query(f"""
            SELECT COUNT(*) AS c FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            WHERE {col} IS NOT NULL AND {col} != ''
        """, conn)
        with_data = int(with_df.iloc[0, 0])
        without_data = total - with_data

        # Amostra sem dados (top 20)
        sample_df = pd.read_sql_query(f"""
            SELECT dg.company_name, dg.ticker, cbd.yahoo_code,
                   cbd.yahoo_sector, cbd.yahoo_country
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            WHERE ({col} IS NULL OR {col} = '')
            ORDER BY cbd.market_cap DESC
            LIMIT 20
        """, conn)
        conn.close()
        sample_df = sample_df.replace({np.nan: None})

        return jsonify({
            'success': True,
            'field': field,
            'label': label,
            'total': total,
            'with_data': with_data,
            'without_data': without_data,
            'pct': round(with_data / total * 100, 1) if total > 0 else 0,
            'sample_without': sample_df.to_dict('records')
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_drill/currencies')
def api_yahoo_drill_currencies():
    """Lista de moedas com contagem de empresas e países."""
    try:
        conn = get_db()
        query = """
            SELECT 
                cbd.currency,
                COUNT(*) AS company_count,
                GROUP_CONCAT(DISTINCT cbd.yahoo_country) AS countries
            FROM company_basic_data cbd
            WHERE cbd.currency IS NOT NULL AND cbd.currency != ''
            GROUP BY cbd.currency
            ORDER BY COUNT(*) DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        df = df.replace({np.nan: None})

        currencies = []
        for _, row in df.iterrows():
            countries_str = row['countries'] or ''
            countries_list = [c.strip() for c in countries_str.split(',') if c.strip()]
            currencies.append({
                'currency': row['currency'],
                'company_count': int(row['company_count']),
                'country_count': len(countries_list),
                'countries': countries_list[:10]  # Top 10 países
            })

        return jsonify({'success': True, 'currencies': currencies})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== EXPORTAÇÃO DE DADOS =====

@app.route('/exporta-data')
def exporta_data_page():
    """Página de exportação de dados para Excel."""
    return render_template('exporta_data.html')


@app.route('/api/export_excel', methods=['POST'])
def api_export_excel():
    """API endpoint para exportar dados filtrados para Excel."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Dados não fornecidos'}), 400

        fields = data.get('fields', [])
        filters = data.get('filters', {})

        if not fields:
            return jsonify({'success': False, 'error': 'Nenhum campo selecionado'}), 400

        # Sanitizar nomes de colunas (prevenir SQL injection)
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(damodaran_global)")
        valid_columns = {row[1] for row in cursor.fetchall()}

        # Adicionar colunas de company_basic_data
        valid_columns.add('about')

        safe_fields = [f for f in fields if f in valid_columns or f == 'about']
        if not safe_fields:
            conn.close()
            return jsonify({'success': False, 'error': 'Nenhum campo válido selecionado'}), 400

        # Construir SELECT
        select_parts = []
        need_cbd_join = False
        for f in safe_fields:
            if f == 'about':
                select_parts.append('cbd.about')
                need_cbd_join = True
            else:
                select_parts.append(f'dg.{f}')

        select_clause = ', '.join(select_parts)
        query = f"SELECT {select_clause} FROM damodaran_global dg"

        if need_cbd_join:
            query += " LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker"

        query += " WHERE 1=1"
        params = []

        # Aplicar filtros
        if filters.get('countries'):
            placeholders = ','.join(['?' for _ in filters['countries']])
            query += f" AND dg.country IN ({placeholders})"
            params.extend(filters['countries'])
        elif filters.get('subregions'):
            placeholders = ','.join(['?' for _ in filters['subregions']])
            query += f" AND dg.sub_group IN ({placeholders})"
            params.extend(filters['subregions'])
        elif filters.get('regions'):
            placeholders = ','.join(['?' for _ in filters['regions']])
            query += f" AND dg.broad_group IN ({placeholders})"
            params.extend(filters['regions'])

        if filters.get('industries'):
            placeholders = ','.join(['?' for _ in filters['industries']])
            query += f" AND dg.industry IN ({placeholders})"
            params.extend(filters['industries'])
        elif filters.get('subsectors'):
            placeholders = ','.join(['?' for _ in filters['subsectors']])
            query += f" AND dg.industry_group IN ({placeholders})"
            params.extend(filters['subsectors'])
        elif filters.get('sectors'):
            placeholders = ','.join(['?' for _ in filters['sectors']])
            query += f" AND dg.primary_sector IN ({placeholders})"
            params.extend(filters['sectors'])

        if filters.get('atividades_anloc'):
            placeholders = ','.join(['?' for _ in filters['atividades_anloc']])
            query += f" AND dg.atividade_anloc IN ({placeholders})"
            params.extend(filters['atividades_anloc'])

        if filters.get('min_market_cap'):
            query += " AND dg.market_cap >= ?"
            params.append(float(filters['min_market_cap']))
        if filters.get('max_market_cap'):
            query += " AND dg.market_cap <= ?"
            params.append(float(filters['max_market_cap']))

        query += " ORDER BY dg.market_cap DESC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        if df.empty:
            return jsonify({'success': False, 'error': 'Nenhum dado encontrado com os filtros aplicados'}), 404

        # Gerar labels para colunas (usar field_categories_manager)
        col_labels = {}
        all_cats = field_manager.get_all_categories()
        for cat_data in all_cats.values():
            for fname, fdata in cat_data.get('fields', {}).items():
                col_labels[fname] = fdata.get('label', fname)
        col_labels['about'] = 'Sobre a Empresa'

        # Renomear colunas para labels
        rename_map = {}
        for col in df.columns:
            rename_map[col] = col_labels.get(col, col)
        df = df.rename(columns=rename_map)

        # Gerar Excel em memória
        from io import BytesIO
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Dados Exportados')

            # Ajustar largura das colunas
            worksheet = writer.sheets['Dados Exportados']
            for idx, col in enumerate(df.columns):
                col_letter = worksheet.cell(row=1, column=idx+1).column_letter
                max_length = max(
                    df[col].astype(str).str.len().max() if len(df) > 0 else 0,
                    len(str(col))
                )
                worksheet.column_dimensions[col_letter].width = min(max_length + 2, 50)

        output.seek(0)

        from flask import send_file
        from datetime import datetime as dt
        filename = f"exportacao_dados_{dt.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Erro na exportação Excel: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/export_preview', methods=['POST'])
def api_export_preview():
    """API para preview dos dados que serão exportados."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Dados não fornecidos'}), 400

        fields = data.get('fields', [])
        filters = data.get('filters', {})

        if not fields:
            return jsonify({'success': False, 'error': 'Nenhum campo selecionado'}), 400

        # Sanitizar nomes de colunas
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(damodaran_global)")
        valid_columns = {row[1] for row in cursor.fetchall()}
        valid_columns.add('about')

        safe_fields = [f for f in fields if f in valid_columns or f == 'about']
        if not safe_fields:
            conn.close()
            return jsonify({'success': False, 'error': 'Nenhum campo válido'}), 400

        # Construir query
        select_parts = []
        need_cbd_join = False
        for f in safe_fields:
            if f == 'about':
                select_parts.append('cbd.about')
                need_cbd_join = True
            else:
                select_parts.append(f'dg.{f}')

        select_clause = ', '.join(select_parts)
        query = f"SELECT {select_clause} FROM damodaran_global dg"
        if need_cbd_join:
            query += " LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker"
        query += " WHERE 1=1"
        params = []

        # Filtros
        if filters.get('countries'):
            placeholders = ','.join(['?' for _ in filters['countries']])
            query += f" AND dg.country IN ({placeholders})"
            params.extend(filters['countries'])
        elif filters.get('subregions'):
            placeholders = ','.join(['?' for _ in filters['subregions']])
            query += f" AND dg.sub_group IN ({placeholders})"
            params.extend(filters['subregions'])
        elif filters.get('regions'):
            placeholders = ','.join(['?' for _ in filters['regions']])
            query += f" AND dg.broad_group IN ({placeholders})"
            params.extend(filters['regions'])

        if filters.get('industries'):
            placeholders = ','.join(['?' for _ in filters['industries']])
            query += f" AND dg.industry IN ({placeholders})"
            params.extend(filters['industries'])
        elif filters.get('subsectors'):
            placeholders = ','.join(['?' for _ in filters['subsectors']])
            query += f" AND dg.industry_group IN ({placeholders})"
            params.extend(filters['subsectors'])
        elif filters.get('sectors'):
            placeholders = ','.join(['?' for _ in filters['sectors']])
            query += f" AND dg.primary_sector IN ({placeholders})"
            params.extend(filters['sectors'])

        if filters.get('atividades_anloc'):
            placeholders = ','.join(['?' for _ in filters['atividades_anloc']])
            query += f" AND dg.atividade_anloc IN ({placeholders})"
            params.extend(filters['atividades_anloc'])

        if filters.get('min_market_cap'):
            query += " AND dg.market_cap >= ?"
            params.append(float(filters['min_market_cap']))
        if filters.get('max_market_cap'):
            query += " AND dg.market_cap <= ?"
            params.append(float(filters['max_market_cap']))

        # Contar total
        count_query = query.replace(f"SELECT {select_clause}", "SELECT COUNT(*)")
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]

        # Buscar preview (primeiras 20 linhas)
        preview_query = query + " ORDER BY dg.market_cap DESC LIMIT 20"
        df = pd.read_sql_query(preview_query, conn, params=params)
        conn.close()

        df = df.replace({np.nan: None})

        # Labels
        col_labels = {}
        all_cats = field_manager.get_all_categories()
        for cat_data in all_cats.values():
            for fname, fdata in cat_data.get('fields', {}).items():
                col_labels[fname] = fdata.get('label', fname)
        col_labels['about'] = 'Sobre a Empresa'

        columns = [{'field': f, 'label': col_labels.get(f, f)} for f in safe_fields]
        rows = df.to_dict('records')

        return jsonify({
            'success': True,
            'total_count': total_count,
            'preview_count': len(rows),
            'columns': columns,
            'rows': rows
        })

    except Exception as e:
        logger.error(f"Erro no preview de exportação: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ==========================================================================
# DADOS FINANCEIROS HISTÓRICOS
# ==========================================================================

@app.route('/metodologias')
def metodologias_page():
    """Página de metodologias de cálculo."""
    return render_template('metodologias.html')


@app.route('/data-yahoo-historico')
def data_yahoo_historico_page():
    """Dashboard de dados financeiros históricos do Yahoo Finance."""
    return render_template('data_yahoo_historico.html')


@app.route('/api/historico/summary')
def api_historico_summary():
    """Resumo geral dos dados históricos para KPI cards. Aceita filtros cross-filter."""
    try:
        conn = get_db()
        cur = conn.cursor()
        stats = {}

        # Parse cross-filter params
        f_sectors = request.args.get('sectors', '').strip()
        f_industries = request.args.get('industries', '').strip()
        f_countries = request.args.get('countries', '').strip()

        # Build filter conditions
        base_conds = []
        base_params = []
        if f_sectors:
            items = [v.strip() for v in f_sectors.split(',') if v.strip()]
            base_conds.append(f"cbd.yahoo_sector IN ({','.join('?' * len(items))})")
            base_params.extend(items)
        if f_industries:
            items = [v.strip() for v in f_industries.split(',') if v.strip()]
            base_conds.append(f"cbd.yahoo_industry IN ({','.join('?' * len(items))})")
            base_params.extend(items)
        if f_countries:
            items = [v.strip() for v in f_countries.split(',') if v.strip()]
            base_conds.append(f"cbd.yahoo_country IN ({','.join('?' * len(items))})")
            base_params.extend(items)

        has_filter = len(base_conds) > 0
        join_cbd = "JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id"

        if has_filter:
            where_base = " AND " + " AND ".join(base_conds)
        else:
            where_base = ""

        # Basic counts
        if has_filter:
            cur.execute(f"SELECT COUNT(*) FROM company_financials_historical cfh {join_cbd} WHERE 1=1 {where_base}", base_params)
        else:
            cur.execute("SELECT COUNT(*) FROM company_financials_historical")
        stats['total_records'] = cur.fetchone()[0]

        if has_filter:
            cur.execute(f"SELECT COUNT(DISTINCT cfh.yahoo_code) FROM company_financials_historical cfh {join_cbd} WHERE 1=1 {where_base}", base_params)
        else:
            cur.execute("SELECT COUNT(DISTINCT yahoo_code) FROM company_financials_historical")
        stats['total_companies'] = cur.fetchone()[0]

        if has_filter:
            cur.execute(f"SELECT COUNT(DISTINCT cfh.yahoo_code) FROM company_financials_historical cfh {join_cbd} WHERE cfh.period_type='annual' {where_base}", base_params)
        else:
            cur.execute("SELECT COUNT(DISTINCT yahoo_code) FROM company_financials_historical WHERE period_type='annual'")
        stats['annual_companies'] = cur.fetchone()[0]

        if has_filter:
            cur.execute(f"SELECT COUNT(DISTINCT cfh.yahoo_code) FROM company_financials_historical cfh {join_cbd} WHERE cfh.period_type='quarterly' {where_base}", base_params)
        else:
            cur.execute("SELECT COUNT(DISTINCT yahoo_code) FROM company_financials_historical WHERE period_type='quarterly'")
        stats['quarterly_companies'] = cur.fetchone()[0]

        if has_filter:
            cur.execute(f"SELECT MIN(cfh.fiscal_year), MAX(cfh.fiscal_year) FROM company_financials_historical cfh {join_cbd} WHERE cfh.period_type='annual' {where_base}", base_params)
        else:
            cur.execute("SELECT MIN(fiscal_year), MAX(fiscal_year) FROM company_financials_historical WHERE period_type='annual'")
        r = cur.fetchone()
        stats['min_year'] = r[0]
        stats['max_year'] = r[1]

        if has_filter:
            cur.execute(f"SELECT COUNT(DISTINCT cfh.original_currency) FROM company_financials_historical cfh {join_cbd} WHERE cfh.original_currency IS NOT NULL {where_base}", base_params)
        else:
            cur.execute("SELECT COUNT(DISTINCT original_currency) FROM company_financials_historical WHERE original_currency IS NOT NULL")
        stats['distinct_currencies'] = cur.fetchone()[0]

        # Distinct sectors, industries, countries
        cur.execute(f"""
            SELECT COUNT(DISTINCT cbd.yahoo_sector)
            FROM company_financials_historical cfh {join_cbd}
            WHERE cbd.yahoo_sector IS NOT NULL {where_base}
        """, base_params)
        stats['distinct_sectors'] = cur.fetchone()[0]
        cur.execute(f"""
            SELECT COUNT(DISTINCT cbd.yahoo_industry)
            FROM company_financials_historical cfh {join_cbd}
            WHERE cbd.yahoo_industry IS NOT NULL {where_base}
        """, base_params)
        stats['distinct_industries'] = cur.fetchone()[0]
        cur.execute(f"""
            SELECT COUNT(DISTINCT cbd.yahoo_country)
            FROM company_financials_historical cfh {join_cbd}
            WHERE cbd.yahoo_country IS NOT NULL AND cbd.yahoo_country != '' {where_base}
        """, base_params)
        stats['distinct_countries'] = cur.fetchone()[0]

        # Industries list
        cur.execute(f"""
            SELECT cbd.yahoo_industry, COUNT(DISTINCT cfh.yahoo_code) AS n
            FROM company_financials_historical cfh {join_cbd}
            WHERE cbd.yahoo_industry IS NOT NULL {where_base}
            GROUP BY cbd.yahoo_industry ORDER BY n DESC
        """, base_params)
        stats['industries'] = [{'industry': r[0], 'count': r[1]} for r in cur.fetchall()]

        if has_filter:
            cur.execute(f"""
                SELECT COUNT(DISTINCT cfh.yahoo_code) 
                FROM company_financials_historical cfh {join_cbd}
                WHERE cfh.enterprise_value_estimated IS NOT NULL {where_base}
            """, base_params)
        else:
            cur.execute("""
                SELECT COUNT(DISTINCT cfh.yahoo_code) 
                FROM company_financials_historical cfh
                WHERE cfh.enterprise_value_estimated IS NOT NULL
            """)
        stats['with_ev'] = cur.fetchone()[0]

        # Cobertura por setor
        cur.execute(f"""
            SELECT cbd.yahoo_sector, COUNT(DISTINCT cfh.yahoo_code) AS n
            FROM company_financials_historical cfh {join_cbd}
            WHERE cbd.yahoo_sector IS NOT NULL {where_base}
            GROUP BY cbd.yahoo_sector ORDER BY n DESC
        """, base_params)
        stats['sectors'] = [{'sector': r[0], 'count': r[1]} for r in cur.fetchall()]

        # Cobertura por país
        cur.execute(f"""
            SELECT cbd.yahoo_country, COUNT(DISTINCT cfh.yahoo_code) AS n
            FROM company_financials_historical cfh {join_cbd}
            WHERE cbd.yahoo_country IS NOT NULL AND cbd.yahoo_country != '' {where_base}
            GROUP BY cbd.yahoo_country ORDER BY n DESC
        """, base_params)
        countries_raw = cur.fetchall()
        stats['countries'] = [{'country': r[0], 'count': r[1]} for r in countries_raw]

        # Regiões e sub-regiões (via geographic_mappings)
        region_counts = {}
        subregion_counts = {}
        for country_name, count in countries_raw:
            geo = get_country_region(country_name)
            region = geo['region']
            subregion = geo['subregion']
            region_counts[region] = region_counts.get(region, 0) + count
            subregion_counts[subregion] = subregion_counts.get(subregion, 0) + count
        stats['regions'] = [{'region': r, 'count': c} for r, c in sorted(region_counts.items(), key=lambda x: -x[1])]
        stats['subregions'] = [{'subregion': r, 'count': c} for r, c in sorted(subregion_counts.items(), key=lambda x: -x[1])]
        # Mapa país→região para o frontend
        stats['country_region_map'] = {k: v for k, v in GEOGRAPHIC_MAPPING.items()}
        conn.close()
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico_filter_options')
def api_historico_filter_options():
    """Opções de filtro para cross-filter na página de dados históricos."""
    try:
        conn = get_db()
        cur = conn.cursor()

        filter_map = {
            'sectors': ('cbd.yahoo_sector', request.args.get('sectors', '').strip()),
            'industries': ('cbd.yahoo_industry', request.args.get('industries', '').strip()),
            'countries': ('cbd.yahoo_country', request.args.get('countries', '').strip()),
        }

        dim_config = [
            ('sectors', 'cbd.yahoo_sector'),
            ('industries', 'cbd.yahoo_industry'),
            ('countries', 'cbd.yahoo_country'),
        ]

        results = {}
        for dim_name, col_expr in dim_config:
            conds = [f"{col_expr} IS NOT NULL"]
            if dim_name == 'countries':
                conds.append(f"{col_expr} != ''")
            params = []
            # Apply filters from OTHER dimensions only (cross-filter)
            for other_dim, (other_col, other_val) in filter_map.items():
                if other_dim == dim_name:
                    continue
                if other_val:
                    items = [v.strip() for v in other_val.split(',') if v.strip()]
                    if items:
                        conds.append(f"{other_col} IN ({','.join('?' * len(items))})")
                        params.extend(items)

            where = " AND ".join(conds)
            query = f"""SELECT {col_expr}, COUNT(DISTINCT cfh.yahoo_code) as cnt
                       FROM company_financials_historical cfh
                       JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                       WHERE {where}
                       GROUP BY {col_expr} ORDER BY {col_expr}"""
            cur.execute(query, params)
            results[dim_name] = [{'name': r[0], 'count': r[1]} for r in cur.fetchall()]

        conn.close()
        return jsonify({'success': True, **results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico/search')
def api_historico_search():
    """Busca empresas com dados históricos. Params: q, sector, country, region, subregion, sectors, industries, countries, limit."""
    try:
        q = request.args.get('q', '').strip()
        sector = request.args.get('sector', '').strip()
        country = request.args.get('country', '').strip()
        region = request.args.get('region', '').strip()
        subregion = request.args.get('subregion', '').strip()
        # Cross-filter params (comma-separated)
        sectors_csv = request.args.get('sectors', '').strip()
        industries_csv = request.args.get('industries', '').strip()
        countries_csv = request.args.get('countries', '').strip()
        limit = int(request.args.get('limit', 50))

        conn = get_db()

        # Build filter conditions once
        filter_conds = ["cfh.period_type = 'annual'"]
        filter_params = []
        if q:
            filter_conds.append("(cfh.yahoo_code LIKE ? OR cfh.company_name LIKE ?)")
            filter_params.extend([f'%{q}%', f'%{q}%'])
        if sector:
            filter_conds.append("cbd.yahoo_sector = ?")
            filter_params.append(sector)
        if country:
            filter_conds.append("cbd.yahoo_country = ?")
            filter_params.append(country)
        if region:
            region_countries = [c for c, info in GEOGRAPHIC_MAPPING.items() if info['region'] == region]
            if region_countries:
                filter_conds.append(f"cbd.yahoo_country IN ({','.join('?' * len(region_countries))})")
                filter_params.extend(region_countries)
        if subregion:
            sub_countries = [c for c, info in GEOGRAPHIC_MAPPING.items() if info['subregion'] == subregion]
            if sub_countries:
                filter_conds.append(f"cbd.yahoo_country IN ({','.join('?' * len(sub_countries))})")
                filter_params.extend(sub_countries)
        if sectors_csv:
            items = [v.strip() for v in sectors_csv.split(',') if v.strip()]
            if items:
                filter_conds.append(f"cbd.yahoo_sector IN ({','.join('?' * len(items))})")
                filter_params.extend(items)
        if industries_csv:
            items = [v.strip() for v in industries_csv.split(',') if v.strip()]
            if items:
                filter_conds.append(f"cbd.yahoo_industry IN ({','.join('?' * len(items))})")
                filter_params.extend(items)
        if countries_csv:
            items = [v.strip() for v in countries_csv.split(',') if v.strip()]
            if items:
                filter_conds.append(f"cbd.yahoo_country IN ({','.join('?' * len(items))})")
                filter_params.extend(items)

        where_clause = " AND ".join(filter_conds)
        base_from = """FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id"""

        # Total count
        total_count = conn.execute(
            f"SELECT COUNT(DISTINCT cfh.yahoo_code) {base_from} WHERE {where_clause}",
            filter_params
        ).fetchone()[0]

        # Main query with limit
        query = f"""
            SELECT DISTINCT cfh.yahoo_code, cfh.company_name,
                   cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country,
                   dg.sub_group AS damodaran_region,
                   cfh.original_currency,
                   COUNT(*) AS periods,
                   MIN(cfh.fiscal_year) AS min_year,
                   MAX(cfh.fiscal_year) AS max_year
            {base_from}
            WHERE {where_clause}
            GROUP BY cfh.yahoo_code ORDER BY cfh.company_name LIMIT ?
        """

        cur = conn.execute(query, filter_params + [limit])
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'companies': rows, 'total_count': total_count})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico/company/<yahoo_code>')
def api_historico_company(yahoo_code):
    """Retorna todos os dados históricos de uma empresa. Params: period_type (annual/quarterly)."""
    try:
        period_type = request.args.get('period_type', 'annual')
        conn = get_db()
        cur = conn.execute("""
            SELECT * FROM company_financials_historical
            WHERE yahoo_code = ? AND period_type = ?
            ORDER BY period_date DESC
        """, [yahoo_code, period_type])
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        # Info da empresa
        info = conn.execute("""
            SELECT cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country,
                   cbd.yahoo_website, cbd.currency
            FROM company_basic_data cbd
            WHERE cbd.yahoo_code = ?
        """, [yahoo_code]).fetchone()
        company_info = {}
        if info:
            company_info = {
                'sector': info[0], 'industry': info[1], 'country': info[2],
                'website': info[3], 'currency': info[4]
            }

        conn.close()
        # Limpar NaN
        for row in rows:
            for k, v in row.items():
                if isinstance(v, float) and (v != v):  # NaN check
                    row[k] = None
        return jsonify({'success': True, 'periods': rows, 'company_info': company_info})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico/sector_evolution')
def api_historico_sector_evolution():
    """Evolução temporal de métricas por setor. Params: metric (ebitda_margin, ev_ebitda, etc.)."""
    try:
        metric = request.args.get('metric', 'ebitda_margin')
        allowed = [
            'ebitda_margin', 'ebit_margin', 'gross_margin', 'net_margin',
            'ev_ebitda', 'ev_ebit', 'ev_revenue', 'debt_equity', 'debt_ebitda',
            'fcf_revenue_ratio', 'fcf_ebitda_ratio', 'capex_revenue',
            'total_revenue_usd', 'ebitda_usd', 'net_income_usd',
            'free_cash_flow_usd', 'enterprise_value_usd'
        ]
        if metric not in allowed:
            return jsonify({'success': False, 'error': f'Métrica inválida: {metric}'}), 400

        conn = get_db()
        query = f"""
            SELECT cbd.yahoo_sector AS sector, cfh.fiscal_year,
                   AVG(cfh.{metric}) AS avg_value,
                   MEDIAN(cfh.{metric}) AS med_value,
                   COUNT(*) AS n
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cfh.period_type = 'annual'
              AND cbd.yahoo_sector IS NOT NULL
              AND cfh.{metric} IS NOT NULL
            GROUP BY cbd.yahoo_sector, cfh.fiscal_year
            ORDER BY cbd.yahoo_sector, cfh.fiscal_year
        """
        try:
            cur = conn.execute(query)
        except Exception:
            # SQLite doesn't have MEDIAN, use AVG only
            query = query.replace(f'MEDIAN(cfh.{metric}) AS med_value,', '')
            cur = conn.execute(query)

        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()

        # Agrupar por setor
        sectors = {}
        for r in rows:
            s = r['sector']
            if s not in sectors:
                sectors[s] = []
            sectors[s].append(r)

        return jsonify({'success': True, 'sectors': sectors})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico/compare')
def api_historico_compare():
    """Compara múltiplas empresas. Params: codes (comma-separated), metric, period_type."""
    try:
        codes_str = request.args.get('codes', '')
        metric = request.args.get('metric', 'ebitda_margin')
        period_type = request.args.get('period_type', 'annual')

        if not codes_str:
            return jsonify({'success': False, 'error': 'Parâmetro codes obrigatório'}), 400

        codes = [c.strip() for c in codes_str.split(',') if c.strip()][:10]

        allowed = [
            'total_revenue', 'ebit', 'ebitda', 'net_income', 'free_cash_flow',
            'total_revenue_usd', 'ebitda_usd', 'net_income_usd', 'free_cash_flow_usd',
            'enterprise_value_estimated', 'enterprise_value_usd', 'market_cap_estimated',
            'ebitda_margin', 'ebit_margin', 'gross_margin', 'net_margin',
            'ev_ebitda', 'ev_ebit', 'ev_revenue', 'debt_equity', 'debt_ebitda',
            'fcf_revenue_ratio', 'fcf_ebitda_ratio', 'capex_revenue'
        ]
        if metric not in allowed:
            return jsonify({'success': False, 'error': f'Métrica inválida: {metric}'}), 400

        placeholders = ','.join(['?'] * len(codes))
        conn = get_db()
        query = f"""
            SELECT yahoo_code, company_name, fiscal_year, period_date, {metric}
            FROM company_financials_historical
            WHERE yahoo_code IN ({placeholders}) AND period_type = ?
              AND {metric} IS NOT NULL
            ORDER BY yahoo_code, fiscal_year
        """
        cur = conn.execute(query, codes + [period_type])
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()

        # Agrupar por empresa
        companies = {}
        for r in rows:
            c = r['yahoo_code']
            if c not in companies:
                companies[c] = {'name': r['company_name'], 'data': []}
            companies[c]['data'].append(r)

        return jsonify({'success': True, 'companies': companies})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico/sectors_list')
def api_historico_sectors_list():
    """Lista setores com dados históricos disponíveis."""
    try:
        conn = get_db()
        cur = conn.execute("""
            SELECT cbd.yahoo_sector, COUNT(DISTINCT cfh.yahoo_code) AS n
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cbd.yahoo_sector IS NOT NULL AND cfh.period_type = 'annual'
            GROUP BY cbd.yahoo_sector ORDER BY n DESC
        """)
        rows = [{'sector': r[0], 'count': r[1]} for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'sectors': rows})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== DRILL-DOWN HISTÓRICO =====

@app.route('/api/historico_drill/companies')
def api_historico_drill_companies():
    """Lista paginada de empresas com dados históricos, com filtros."""
    try:
        sector = request.args.get('sector', '')
        country = request.args.get('country', '')
        industry = request.args.get('industry', '')
        region = request.args.get('region', '')
        subregion = request.args.get('subregion', '')
        search = request.args.get('search', '')
        has_ev = request.args.get('has_ev', '')
        currency = request.args.get('currency', '')
        sort = request.args.get('sort', 'company_name')
        order = request.args.get('order', 'asc')
        page = max(1, int(request.args.get('page', 1)))
        per_page = min(100, max(10, int(request.args.get('per_page', 50))))

        conn = get_db()
        params = []
        conditions = ["cfh.period_type = 'annual'"]

        if sector:
            conditions.append("cbd.yahoo_sector = ?")
            params.append(sector)
        if country:
            conditions.append("cbd.yahoo_country = ?")
            params.append(country)
        if industry:
            conditions.append("cbd.yahoo_industry = ?")
            params.append(industry)
        if region:
            region_countries = [c for c, info in GEOGRAPHIC_MAPPING.items() if info['region'] == region]
            if region_countries:
                placeholders = ','.join(['?'] * len(region_countries))
                conditions.append(f"cbd.yahoo_country IN ({placeholders})")
                params.extend(region_countries)
        if subregion:
            sub_countries = [c for c, info in GEOGRAPHIC_MAPPING.items() if info['subregion'] == subregion]
            if sub_countries:
                placeholders = ','.join(['?'] * len(sub_countries))
                conditions.append(f"cbd.yahoo_country IN ({placeholders})")
                params.extend(sub_countries)
        if search:
            conditions.append("(cfh.yahoo_code LIKE ? OR cfh.company_name LIKE ?)")
            s = f"%{search}%"
            params.extend([s, s])
        if has_ev == '1':
            conditions.append("cfh.enterprise_value_estimated IS NOT NULL")
        elif has_ev == '0':
            conditions.append("cfh.enterprise_value_estimated IS NULL")
        if currency:
            conditions.append("cfh.original_currency = ?")
            params.append(currency)

        where = "WHERE " + " AND ".join(conditions)

        valid_sorts = {
            'company_name': 'cfh.company_name',
            'yahoo_code': 'cfh.yahoo_code',
            'sector': 'cbd.yahoo_sector',
            'country': 'cbd.yahoo_country',
            'periods': 'periods',
            'max_year': 'max_year',
        }
        sort_col = valid_sorts.get(sort, 'cfh.company_name')
        sort_dir = 'DESC' if order.lower() == 'desc' else 'ASC'

        count_query = f"""
            SELECT COUNT(DISTINCT cfh.yahoo_code)
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            {where}
        """
        cur = conn.execute(count_query, params)
        total = cur.fetchone()[0]

        offset = (page - 1) * per_page
        query = f"""
            SELECT cfh.yahoo_code, cfh.company_name,
                   cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country,
                   dg.sub_group AS damodaran_region,
                   cfh.original_currency,
                   COUNT(*) AS periods,
                   MIN(cfh.fiscal_year) AS min_year,
                   MAX(cfh.fiscal_year) AS max_year
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
            {where}
            GROUP BY cfh.yahoo_code
            ORDER BY {sort_col} {sort_dir}
            LIMIT ? OFFSET ?
        """
        params.extend([per_page, offset])
        cur = conn.execute(query, params)
        cols = [d[0] for d in cur.description]
        companies = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()

        return jsonify({
            'success': True,
            'companies': companies,
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': (total + per_page - 1) // per_page
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico_drill/year_detail')
def api_historico_drill_year_detail():
    """Detalhe individual por empresa para um ano/métrica específico (do consolidado).
    Params: codes (comma-separated), year, metric
    """
    try:
        codes_str = request.args.get('codes', '')
        year = request.args.get('year', '')
        metric = request.args.get('metric', 'ebitda_margin')

        if not codes_str or not year:
            return jsonify({'success': False, 'error': 'Parâmetros obrigatórios: codes, year'}), 400

        codes = [c.strip() for c in codes_str.split(',') if c.strip()]
        year = int(year)

        valid_metrics = [
            'total_revenue_usd', 'ebitda_usd', 'net_income_usd', 'free_cash_flow_usd',
            'enterprise_value_usd', 'ebitda_margin', 'ebit_margin', 'gross_margin', 'net_margin',
            'ev_ebitda', 'ev_ebit', 'ev_revenue', 'debt_equity', 'debt_ebitda',
            'fcf_revenue_ratio', 'capex_revenue',
            'total_revenue', 'ebitda', 'net_income', 'free_cash_flow',
            'market_cap_estimated', 'enterprise_value_estimated',
            'total_debt', 'cash_and_equivalents', 'stockholders_equity'
        ]
        if metric not in valid_metrics:
            return jsonify({'success': False, 'error': f'Métrica inválida: {metric}'}), 400

        placeholders = ','.join(['?'] * len(codes))
        conn = get_db()
        query = f"""
            SELECT cfh.yahoo_code, cfh.company_name, cfh.{metric} AS value,
                   cbd.yahoo_sector, cbd.yahoo_country, cfh.original_currency
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cfh.yahoo_code IN ({placeholders})
              AND cfh.fiscal_year = ?
              AND cfh.period_type = 'annual'
              AND cfh.{metric} IS NOT NULL
            ORDER BY cfh.{metric} DESC
        """
        cur = conn.execute(query, codes + [year])
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()

        # Calcular stats
        values = [r['value'] for r in rows if r['value'] is not None]
        stats = None
        if values:
            arr = np.array(values, dtype=float)
            stats = {
                'count': len(arr),
                'mean': float(np.mean(arr)),
                'median': float(np.median(arr)),
                'min': float(np.min(arr)),
                'max': float(np.max(arr)),
                'p25': float(np.percentile(arr, 25)),
                'p75': float(np.percentile(arr, 75)),
            }

        # Labels for metrics
        metric_labels = {
            'ebitda_margin': 'Margem EBITDA', 'ebit_margin': 'Margem EBIT',
            'gross_margin': 'Margem Bruta', 'net_margin': 'Margem Líquida',
            'ev_ebitda': 'EV/EBITDA', 'ev_ebit': 'EV/EBIT', 'ev_revenue': 'EV/Receita',
            'debt_equity': 'Dívida/PL', 'debt_ebitda': 'Dívida/EBITDA',
            'fcf_revenue_ratio': 'FCF/Receita', 'capex_revenue': 'Capex/Receita',
            'total_revenue_usd': 'Receita USD', 'ebitda_usd': 'EBITDA USD',
            'net_income_usd': 'Lucro Líq. USD', 'free_cash_flow_usd': 'FCF USD',
            'enterprise_value_usd': 'EV USD', 'total_revenue': 'Receita',
            'ebitda': 'EBITDA', 'net_income': 'Lucro Líquido',
            'free_cash_flow': 'FCF', 'market_cap_estimated': 'Market Cap',
            'enterprise_value_estimated': 'EV', 'total_debt': 'Dívida Total',
            'cash_and_equivalents': 'Caixa', 'stockholders_equity': 'PL',
        }

        return jsonify({
            'success': True,
            'year': year,
            'metric': metric,
            'label': metric_labels.get(metric, metric),
            'companies': rows,
            'stats': stats,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico/consolidated', methods=['POST'])
def api_historico_consolidated():
    """Retorna dados consolidados (avg, median, min, max, p25, p75) para um conjunto de empresas.
    Body JSON: { codes: [...], period_type: 'annual' }
    """
    try:
        data = request.get_json()
        codes = data.get('codes', [])
        period_type = data.get('period_type', 'annual')
        include_detail = data.get('include_detail', False)
        exclude_critical = data.get('exclude_critical', False)
        if not codes:
            return jsonify({'success': False, 'error': 'Nenhuma empresa selecionada'}), 400

        placeholders = ','.join(['?'] * len(codes))
        conn = get_db()

        # Buscar todos os registros das empresas
        if period_type == 'all':
            query = f"""
                SELECT cfh.*, cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country,
                       dg.sub_group AS damodaran_region
                FROM company_financials_historical cfh
                JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE cfh.yahoo_code IN ({placeholders})
                ORDER BY cfh.fiscal_year, cfh.yahoo_code
            """
            df = pd.read_sql_query(query, conn, params=codes)
        else:
            query = f"""
                SELECT cfh.*, cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country,
                       dg.sub_group AS damodaran_region
                FROM company_financials_historical cfh
                JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE cfh.yahoo_code IN ({placeholders}) AND cfh.period_type = ?
                ORDER BY cfh.fiscal_year, cfh.yahoo_code
            """
            df = pd.read_sql_query(query, conn, params=codes + [period_type])
        conn.close()

        if df.empty:
            return jsonify({'success': True, 'years': [], 'companies': [], 'aggregated': {}, 'ranking': []})

        df = df.replace({np.nan: None})

        # Filtrar registros com problemas críticos de qualidade se solicitado
        df_agg = df.copy()
        excluded_count = 0
        if exclude_critical and 'data_quality' in df_agg.columns:
            mask = df_agg['data_quality'].fillna('').str.contains('critical', case=False)
            excluded_count = int(mask.sum())
            df_agg = df_agg[~mask]

        # Métricas para agregar (todos os campos numéricos da base)
        metrics = [
            # DRE (P&L)
            'total_revenue', 'cost_of_revenue', 'gross_profit',
            'operating_income', 'operating_expense',
            'ebit', 'ebitda', 'normalized_ebitda', 'net_income',
            'interest_expense', 'tax_provision',
            'research_and_development', 'sga', 'diluted_average_shares',
            # Fluxo de Caixa
            'free_cash_flow', 'operating_cash_flow', 'capital_expenditure',
            # Balanço
            'total_assets', 'current_assets',
            'total_liabilities', 'current_liabilities',
            'total_debt', 'short_term_debt', 'long_term_debt',
            'stockholders_equity', 'cash_and_equivalents', 'short_term_investments',
            'ordinary_shares_number', 'preferred_stock', 'minority_interest',
            # Mercado / EV
            'close_price', 'market_cap_estimated', 'enterprise_value_estimated',
            # Margens
            'ebitda_margin', 'ebit_margin', 'gross_margin', 'net_margin',
            # Múltiplos
            'ev_ebitda', 'ev_ebit', 'ev_revenue', 'debt_equity', 'debt_ebitda',
            # Indicadores
            'fcf_revenue_ratio', 'fcf_ebitda_ratio', 'capex_revenue',
            # Valores em USD
            'total_revenue_usd', 'ebit_usd', 'ebitda_usd',
            'net_income_usd', 'free_cash_flow_usd', 'enterprise_value_usd',
            # Câmbio
            'fx_rate_to_usd',
            # TTM
            'total_revenue_ttm', 'ebitda_ttm', 'ebit_ttm',
            'free_cash_flow_ttm', 'net_income_ttm', 'ttm_quarters_count',
        ]

        # Agregar por ano
        years = sorted(df_agg['fiscal_year'].dropna().unique().tolist())
        aggregated = {}
        for metric in metrics:
            if metric not in df_agg.columns:
                continue
            agg = {}
            for year in years:
                year_data = df_agg[df_agg['fiscal_year'] == year][metric].dropna()
                if len(year_data) == 0:
                    continue
                arr = year_data.values.astype(float)
                agg[int(year)] = {
                    'avg': float(np.mean(arr)),
                    'median': float(np.median(arr)),
                    'min': float(np.min(arr)),
                    'max': float(np.max(arr)),
                    'p25': float(np.percentile(arr, 25)),
                    'p75': float(np.percentile(arr, 75)),
                    'count': int(len(arr)),
                    'sum': float(np.sum(arr)),
                }
            if agg:
                aggregated[metric] = agg

        # Lista de empresas com info
        companies_info = []
        for code in df_agg['yahoo_code'].unique():
            cdf = df_agg[df_agg['yahoo_code'] == code]
            latest = cdf.sort_values('fiscal_year', ascending=False).iloc[0]
            companies_info.append({
                'yahoo_code': code,
                'company_name': latest.get('company_name'),
                'sector': latest.get('yahoo_sector'),
                'industry': latest.get('yahoo_industry'),
                'region': latest.get('damodaran_region'),
                'country': latest.get('yahoo_country'),
                'currency': latest.get('original_currency'),
                'periods': int(len(cdf)),
            })

        # Ranking: última período por empresa com métricas chave
        latest_year = max(years)
        latest_df = df_agg[df_agg['fiscal_year'] == latest_year].copy()
        ranking = []
        for _, row in latest_df.iterrows():
            ranking.append({
                'yahoo_code': row.get('yahoo_code'),
                'company_name': row.get('company_name'),
                'sector': row.get('yahoo_sector'),
                'industry': row.get('yahoo_industry'),
                'region': row.get('damodaran_region'),
                'total_revenue_usd': row.get('total_revenue_usd'),
                'ebitda_usd': row.get('ebitda_usd'),
                'net_income_usd': row.get('net_income_usd'),
                'free_cash_flow_usd': row.get('free_cash_flow_usd'),
                'enterprise_value_usd': row.get('enterprise_value_usd'),
                'ebitda_margin': row.get('ebitda_margin'),
                'net_margin': row.get('net_margin'),
                'ev_ebitda': row.get('ev_ebitda'),
                'ev_ebit': row.get('ev_ebit'),
                'ev_revenue': row.get('ev_revenue'),
                'debt_equity': row.get('debt_equity'),
                'fcf_revenue_ratio': row.get('fcf_revenue_ratio'),
                'short_term_debt': row.get('short_term_debt'),
                'long_term_debt': row.get('long_term_debt'),
                'diluted_average_shares': row.get('diluted_average_shares'),
                'data_quality': row.get('data_quality'),
            })

        # Limpar NaN dos rankings
        for item in ranking:
            for k, v in item.items():
                if isinstance(v, float) and (v != v):
                    item[k] = None

        # Dados detalhados por empresa (opcional)
        detail_records = []
        if include_detail:
            detail_cols = ['yahoo_code', 'company_name', 'fiscal_year', 'fiscal_quarter',
                           'period_type', 'original_currency',
                           'yahoo_sector', 'yahoo_industry', 'damodaran_region', 'yahoo_country'] + metrics
            available_cols = [c for c in detail_cols if c in df.columns]
            detail_df = df[available_cols].copy()
            for col in detail_df.columns:
                if detail_df[col].dtype in ['float64', 'float32']:
                    detail_df[col] = detail_df[col].where(detail_df[col].notna(), None)
                    # Replace inf/-inf
                    detail_df[col] = detail_df[col].replace([np.inf, -np.inf], None)
            detail_records = detail_df.to_dict('records')
            # Clean NaN
            for rec in detail_records:
                for k, v in rec.items():
                    if isinstance(v, float) and (v != v):
                        rec[k] = None

        return jsonify({
            'success': True,
            'years': [int(y) for y in years],
            'companies': companies_info,
            'aggregated': aggregated,
            'ranking': ranking,
            'detail': detail_records,
            'total_selected': len(codes),
            'total_with_data': len(companies_info),
            'latest_year': int(latest_year),
            'excluded_critical': excluded_count,
        })
    except Exception as e:
        logger.error(f"Erro na consolidação: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== ANÁLISE POR SETOR =====

@app.route('/analise-setor')
def analise_setor_page():
    """Página de análise comparativa por setor e localidade."""
    return render_template('analise_setor.html')


@app.route('/api/analise_setor/filters')
def api_analise_setor_filters():
    """Retorna opções de filtro: setores, regiões, países e anos disponíveis."""
    try:
        conn = get_db()
        cur = conn.cursor()

        # Setores
        cur.execute("""
            SELECT cbd.yahoo_sector, COUNT(DISTINCT cfh.company_basic_data_id) AS n
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cbd.yahoo_sector IS NOT NULL AND cfh.period_type = 'annual'
            GROUP BY cbd.yahoo_sector ORDER BY n DESC
        """)
        sectors = [{'name': r[0], 'count': r[1]} for r in cur.fetchall()]

        # Países com contagem
        cur.execute("""
            SELECT cbd.yahoo_country, COUNT(DISTINCT cfh.company_basic_data_id) AS n
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cbd.yahoo_country IS NOT NULL AND cbd.yahoo_country != '' AND cfh.period_type = 'annual'
            GROUP BY cbd.yahoo_country ORDER BY n DESC
        """)
        countries_raw = cur.fetchall()
        countries = [{'name': r[0], 'count': r[1]} for r in countries_raw]

        # Regiões e sub-regiões via geographic_mappings
        region_counts = {}
        subregion_counts = {}
        country_to_region = {}
        for country_name, count in countries_raw:
            geo = get_country_region(country_name)
            region = geo['region']
            subregion = geo['subregion']
            region_counts[region] = region_counts.get(region, 0) + count
            subregion_counts[subregion] = subregion_counts.get(subregion, 0) + count
            country_to_region[country_name] = {'region': region, 'subregion': subregion}

        regions = [{'name': r, 'count': c} for r, c in sorted(region_counts.items(), key=lambda x: -x[1])]
        subregions = [{'name': r, 'count': c} for r, c in sorted(subregion_counts.items(), key=lambda x: -x[1])]

        # Anos disponíveis
        cur.execute("SELECT DISTINCT fiscal_year FROM company_financials_historical WHERE period_type='annual' AND fiscal_year IS NOT NULL ORDER BY fiscal_year")
        years = [r[0] for r in cur.fetchall()]

        # SIC Desc
        cur.execute("""
            SELECT dg.sic_desc, COUNT(DISTINCT cfh.company_basic_data_id) AS n
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            JOIN damodaran_global dg ON dg.ticker = cbd.ticker
            WHERE dg.sic_desc IS NOT NULL AND dg.sic_desc != '' AND cfh.period_type = 'annual'
            GROUP BY dg.sic_desc ORDER BY n DESC
        """)
        sic_descs = [{'name': r[0], 'count': r[1]} for r in cur.fetchall()]

        conn.close()
        return jsonify({
            'success': True,
            'sectors': sectors,
            'regions': regions,
            'subregions': subregions,
            'countries': countries,
            'country_to_region': country_to_region,
            'years': years,
            'sic_descs': sic_descs
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/analise_setor/data')
def api_analise_setor_data():
    """Retorna dados agregados por setor/localidade para os períodos selecionados.
    Params: sectors, countries, regions, years, metrics, group_by (sector|country|region)
    """
    try:
        # Parse params
        sectors = [s.strip() for s in request.args.get('sectors', '').split(',') if s.strip()]
        countries = [s.strip() for s in request.args.get('countries', '').split(',') if s.strip()]
        regions = [s.strip() for s in request.args.get('regions', '').split(',') if s.strip()]
        years = [int(y) for y in request.args.get('years', '').split(',') if y.strip()]
        sic_descs = [s.strip() for s in request.args.get('sic_descs', '').split(',') if s.strip()]
        group_by = request.args.get('group_by', 'sector')

        allowed_metrics = [
            'total_revenue_usd', 'ebit_usd', 'ebitda_usd', 'net_income_usd',
            'free_cash_flow_usd', 'enterprise_value_usd',
            'ebit_margin', 'ebitda_margin', 'gross_margin', 'net_margin',
            'ev_ebitda', 'ev_ebit', 'ev_revenue', 'debt_equity', 'debt_ebitda',
            'fcf_revenue_ratio', 'fcf_ebitda_ratio', 'capex_revenue',
            'total_revenue', 'ebit', 'ebitda', 'net_income', 'free_cash_flow',
            'market_cap_estimated', 'enterprise_value_estimated',
            'total_assets', 'total_debt', 'stockholders_equity',
            'interest_expense', 'tax_provision', 'research_and_development',
        ]

        conn = get_db()
        params = []
        conditions = ["cfh.period_type = 'annual'"]

        if sectors:
            placeholders = ','.join(['?'] * len(sectors))
            conditions.append(f"cbd.yahoo_sector IN ({placeholders})")
            params.extend(sectors)

        if countries:
            placeholders = ','.join(['?'] * len(countries))
            conditions.append(f"cbd.yahoo_country IN ({placeholders})")
            params.extend(countries)
        elif regions:
            region_countries = [c for c, info in GEOGRAPHIC_MAPPING.items()
                                if info['region'] in regions]
            if region_countries:
                placeholders = ','.join(['?'] * len(region_countries))
                conditions.append(f"cbd.yahoo_country IN ({placeholders})")
                params.extend(region_countries)

        if years:
            placeholders = ','.join(['?'] * len(years))
            conditions.append(f"cfh.fiscal_year IN ({placeholders})")
            params.extend(years)

        if sic_descs:
            placeholders = ','.join(['?'] * len(sic_descs))
            conditions.append(f"dg.sic_desc IN ({placeholders})")
            params.extend(sic_descs)

        where = ' AND '.join(conditions)

        # Determine grouping column
        if group_by == 'country':
            group_col = 'cbd.yahoo_country'
            group_alias = 'group_name'
        elif group_by == 'region':
            group_col = 'cbd.yahoo_country'
            group_alias = 'country'
        else:
            group_col = 'cbd.yahoo_sector'
            group_alias = 'group_name'

        # Build metric aggregations
        metric_aggs = []
        for m in allowed_metrics:
            metric_aggs.append(f"AVG(cfh.{m}) AS avg_{m}")
            metric_aggs.append(f"MIN(cfh.{m}) AS min_{m}")
            metric_aggs.append(f"MAX(cfh.{m}) AS max_{m}")
            metric_aggs.append(f"SUM(CASE WHEN cfh.{m} IS NOT NULL THEN 1 ELSE 0 END) AS n_{m}")
        metric_sql = ', '.join(metric_aggs)

        sic_join = "LEFT JOIN damodaran_global dg ON dg.ticker = cbd.ticker" if sic_descs else ""

        query = f"""
            SELECT {group_col} AS {group_alias},
                   cfh.fiscal_year,
                   COUNT(DISTINCT cfh.company_basic_data_id) AS num_companies,
                   COUNT(*) AS num_records,
                   {metric_sql}
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            {sic_join}
            WHERE {where}
            GROUP BY {group_col}, cfh.fiscal_year
            ORDER BY {group_col}, cfh.fiscal_year
        """

        cur = conn.execute(query, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()

        # Se group_by == 'region', agregar countries em regions
        if group_by == 'region':
            region_data = {}
            for r in rows:
                geo = get_country_region(r.get('country', '') or '')
                region_name = geo['region']
                year = r['fiscal_year']
                key = (region_name, year)
                if key not in region_data:
                    region_data[key] = {'group_name': region_name, 'fiscal_year': year,
                                        'num_companies': 0, 'num_records': 0}
                    for m in allowed_metrics:
                        region_data[key][f'_sum_{m}'] = 0.0
                        region_data[key][f'_n_{m}'] = 0
                        region_data[key][f'min_{m}'] = None
                        region_data[key][f'max_{m}'] = None
                region_data[key]['num_companies'] += r['num_companies']
                region_data[key]['num_records'] += r['num_records']
                for m in allowed_metrics:
                    avg_v = r.get(f'avg_{m}')
                    n_v = r.get(f'n_{m}', 0) or 0
                    mn = r.get(f'min_{m}')
                    mx = r.get(f'max_{m}')
                    if avg_v is not None and n_v > 0:
                        region_data[key][f'_sum_{m}'] += avg_v * n_v
                        region_data[key][f'_n_{m}'] += n_v
                    if mn is not None:
                        cur_min = region_data[key][f'min_{m}']
                        region_data[key][f'min_{m}'] = mn if cur_min is None else min(cur_min, mn)
                    if mx is not None:
                        cur_max = region_data[key][f'max_{m}']
                        region_data[key][f'max_{m}'] = mx if cur_max is None else max(cur_max, mx)
            rows = []
            for key, data in region_data.items():
                row = {'group_name': data['group_name'], 'fiscal_year': data['fiscal_year'],
                       'num_companies': data['num_companies'], 'num_records': data['num_records']}
                for m in allowed_metrics:
                    total_n = data[f'_n_{m}']
                    row[f'avg_{m}'] = data[f'_sum_{m}'] / total_n if total_n > 0 else None
                    row[f'n_{m}'] = total_n
                    row[f'min_{m}'] = data[f'min_{m}']
                    row[f'max_{m}'] = data[f'max_{m}']
                rows.append(row)
            rows.sort(key=lambda x: (x['group_name'], x['fiscal_year']))

        # Clean NaN/Inf
        for r in rows:
            for k, v in r.items():
                if isinstance(v, float) and (v != v or v == float('inf') or v == float('-inf')):
                    r[k] = None

        return jsonify({'success': True, 'data': rows, 'group_by': group_by})
    except Exception as e:
        logger.error(f"Erro analise_setor/data: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/analise_setor/detail')
def api_analise_setor_detail():
    """Retorna dados individuais das empresas para exportação/tabela detalhada.
    Params: sectors, countries, regions, years, page, per_page, sort, order
    """
    try:
        sectors = [s.strip() for s in request.args.get('sectors', '').split(',') if s.strip()]
        countries = [s.strip() for s in request.args.get('countries', '').split(',') if s.strip()]
        regions = [s.strip() for s in request.args.get('regions', '').split(',') if s.strip()]
        years = [int(y) for y in request.args.get('years', '').split(',') if y.strip()]
        sic_descs_detail = [s.strip() for s in request.args.get('sic_descs', '').split(',') if s.strip()]
        sort = request.args.get('sort', 'company_name')
        order = request.args.get('order', 'asc').lower()
        page = max(1, int(request.args.get('page', 1)))
        per_page = min(500, max(10, int(request.args.get('per_page', 50))))

        allowed_sort = [
            'company_name', 'yahoo_code', 'yahoo_sector', 'yahoo_country',
            'fiscal_year', 'total_revenue_usd', 'ebitda_usd', 'ebit_usd',
            'net_income_usd', 'ebitda_margin', 'ebit_margin', 'net_margin',
            'ev_ebitda', 'ev_ebit', 'debt_equity', 'enterprise_value_usd',
            'market_cap_estimated', 'free_cash_flow_usd'
        ]
        if sort not in allowed_sort:
            sort = 'company_name'
        order_dir = 'DESC' if order == 'desc' else 'ASC'

        conn = get_db()
        params = []
        conditions = ["cfh.period_type = 'annual'"]

        if sectors:
            placeholders = ','.join(['?'] * len(sectors))
            conditions.append(f"cbd.yahoo_sector IN ({placeholders})")
            params.extend(sectors)
        if countries:
            placeholders = ','.join(['?'] * len(countries))
            conditions.append(f"cbd.yahoo_country IN ({placeholders})")
            params.extend(countries)
        elif regions:
            region_countries = [c for c, info in GEOGRAPHIC_MAPPING.items()
                                if info['region'] in regions]
            if region_countries:
                placeholders = ','.join(['?'] * len(region_countries))
                conditions.append(f"cbd.yahoo_country IN ({placeholders})")
                params.extend(region_countries)
        if years:
            placeholders = ','.join(['?'] * len(years))
            conditions.append(f"cfh.fiscal_year IN ({placeholders})")
            params.extend(years)

        if sic_descs_detail:
            placeholders = ','.join(['?'] * len(sic_descs_detail))
            conditions.append(f"dg.sic_desc IN ({placeholders})")
            params.extend(sic_descs_detail)

        where = ' AND '.join(conditions)
        sic_join_detail = "LEFT JOIN damodaran_global dg ON dg.ticker = cbd.ticker" if sic_descs_detail else ""

        # Total count
        count_q = f"""SELECT COUNT(*) FROM company_financials_historical cfh
                      JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                      {sic_join_detail}
                      WHERE {where}"""
        total = conn.execute(count_q, params).fetchone()[0]

        # Sort column mapping
        sort_col = f'cfh.{sort}' if sort not in ('company_name', 'yahoo_code', 'yahoo_sector', 'yahoo_country') else f'cbd.{sort}' if sort in ('yahoo_sector', 'yahoo_country') else f'cfh.{sort}'

        query = f"""
            SELECT cfh.yahoo_code, cfh.company_name, cbd.yahoo_sector, cbd.yahoo_industry,
                   cbd.yahoo_country, cfh.fiscal_year, cfh.original_currency,
                   cfh.total_revenue, cfh.ebit, cfh.ebitda, cfh.net_income,
                   cfh.free_cash_flow, cfh.total_debt, cfh.stockholders_equity,
                   cfh.enterprise_value_estimated, cfh.market_cap_estimated,
                   cfh.total_revenue_usd, cfh.ebit_usd, cfh.ebitda_usd,
                   cfh.net_income_usd, cfh.free_cash_flow_usd, cfh.enterprise_value_usd,
                   cfh.ebit_margin, cfh.ebitda_margin, cfh.gross_margin, cfh.net_margin,
                   cfh.ev_ebitda, cfh.ev_ebit, cfh.ev_revenue,
                   cfh.debt_equity, cfh.debt_ebitda, cfh.capex_revenue,
                   cfh.fcf_revenue_ratio, cfh.interest_expense, cfh.tax_provision
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            {sic_join_detail}
            WHERE {where}
            ORDER BY {sort_col} {order_dir} NULLS LAST
            LIMIT ? OFFSET ?
        """
        params.extend([per_page, (page - 1) * per_page])
        cur = conn.execute(query, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()

        # Clean NaN
        for r in rows:
            for k, v in r.items():
                if isinstance(v, float) and (v != v or v == float('inf') or v == float('-inf')):
                    r[k] = None

        return jsonify({
            'success': True,
            'data': rows,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        })
    except Exception as e:
        logger.error(f"Erro analise_setor/detail: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# ANÁLISE DE CONSISTÊNCIA DOS DADOS
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/data-consistency')
def data_consistency_page():
    """Página de análise de consistência dos dados financeiros históricos."""
    return render_template('data_consistency.html')


@app.route('/api/data_consistency/validate')
def api_data_consistency_validate():
    """Executa validação de consistência e retorna resultados."""
    try:
        from scripts.validate_data_consistency import get_validation_results_for_api
        conn = get_db()
        results = get_validation_results_for_api(conn)
        conn.close()
        return jsonify({'success': True, 'results': results})
    except Exception as e:
        logger.error(f"Erro na validação de consistência: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/data_consistency/update_quality', methods=['POST'])
def api_data_consistency_update():
    """Atualiza o campo data_quality no banco com base nos problemas encontrados."""
    try:
        from scripts.validate_data_consistency import run_validation, update_data_quality
        conn = get_db()
        results = run_validation(conn)
        updated = update_data_quality(conn, results)
        conn.close()
        return jsonify({'success': True, 'updated': updated})
    except Exception as e:
        logger.error(f"Erro ao atualizar data_quality: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ════════════════════════════════════════════════════════════════════
# ETF Explorer – Rotas (Fase 3)
# ════════════════════════════════════════════════════════════════════

# Tag types disponíveis para cross-filter
_TAG_FILTER_TYPES = ['asset_class', 'geography', 'strategy', 'style', 'sector', 'cap_size', 'index']


def _etf_cross_filter_where(args, table_alias='e'):
    """Monta cláusulas WHERE para cross-filtering de ETFs (inclui tags)."""
    conds, params = [], []
    f_sources = args.get('sources', '').strip()
    f_categories = args.get('categories', '').strip()
    f_issuers = args.get('issuers', '').strip()
    f_regions = args.get('regions', '').strip()

    if f_sources:
        items = [v.strip() for v in f_sources.split(',') if v.strip()]
        conds.append(f"{table_alias}.data_source IN ({','.join('?' * len(items))})")
        params.extend(items)
    if f_categories:
        items = [v.strip() for v in f_categories.split(',') if v.strip()]
        conds.append(f"{table_alias}.category IN ({','.join('?' * len(items))})")
        params.extend(items)
    if f_issuers:
        items = [v.strip() for v in f_issuers.split(',') if v.strip()]
        conds.append(f"{table_alias}.issuer IN ({','.join('?' * len(items))})")
        params.extend(items)
    if f_regions:
        region_conds = []
        for reg in [v.strip() for v in f_regions.split(',') if v.strip()]:
            if reg == 'Brasil':
                region_conds.append(f"{table_alias}.ticker LIKE '%.SA'")
            elif reg == 'EUA':
                region_conds.append(f"({table_alias}.ticker NOT LIKE '%.SA' AND {table_alias}.ticker NOT LIKE '%.L' AND {table_alias}.ticker NOT LIKE '%.TO' AND {table_alias}.ticker NOT LIKE '%.AX' AND {table_alias}.ticker NOT LIKE '%.HK')")
            else:
                ext = {'UK': '.L', 'Canadá': '.TO', 'Austrália': '.AX', 'Hong Kong': '.HK'}.get(reg)
                if ext:
                    region_conds.append(f"{table_alias}.ticker LIKE '%{ext}'")
        if region_conds:
            conds.append(f"({' OR '.join(region_conds)})")

    # Tag filters: tag_asset_class=Equity,Crypto → ticker IN (SELECT ...)
    for tt in _TAG_FILTER_TYPES:
        raw = args.get(f'tag_{tt}', '').strip()
        if raw:
            items = [v.strip() for v in raw.split(',') if v.strip()]
            placeholders = ','.join('?' * len(items))
            conds.append(
                f"{table_alias}.ticker IN ("
                f"SELECT etf_ticker FROM etf_tags "
                f"WHERE tag_type = ? AND tag_value IN ({placeholders}))"
            )
            params.append(tt)
            params.extend(items)

    return conds, params


_ETF_REGION_CASE = """
    CASE
        WHEN {t}.ticker LIKE '%.SA' THEN 'Brasil'
        WHEN {t}.ticker LIKE '%.L'  THEN 'UK'
        WHEN {t}.ticker LIKE '%.TO' THEN 'Canadá'
        WHEN {t}.ticker LIKE '%.AX' THEN 'Austrália'
        WHEN {t}.ticker LIKE '%.HK' THEN 'Hong Kong'
        ELSE 'EUA'
    END"""


@app.route('/etfs')
def etfs_page():
    """Página do ETF Explorer."""
    return render_template('etf_explorer.html')


@app.route('/api/etfs', methods=['GET'])
def api_etfs_list():
    """Lista ETFs com filtros opcionais + cross-filter."""
    try:
        search = request.args.get('search', '').strip()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        per_page = min(per_page, 500)

        conn = get_db()
        conn.row_factory = sqlite3.Row

        conds, params = _etf_cross_filter_where(request.args)

        if search:
            conds.append("(e.ticker LIKE ? OR e.name LIKE ?)")
            params.extend([f'%{search}%', f'%{search}%'])

        where = (" AND " + " AND ".join(conds)) if conds else ""

        total = conn.execute(f"SELECT COUNT(*) FROM etfs e WHERE 1=1{where}", params).fetchone()[0]

        rows = conn.execute(
            f"SELECT e.*, {_ETF_REGION_CASE.format(t='e')} as computed_region FROM etfs e WHERE 1=1{where} ORDER BY e.ticker LIMIT ? OFFSET ?",
            params + [per_page, (page - 1) * per_page],
        ).fetchall()

        conn.close()

        return jsonify({
            'success': True,
            'etfs': [dict(r) for r in rows],
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': max(1, (total + per_page - 1) // per_page),
        })
    except Exception as e:
        logger.error(f"Erro na API ETF list: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/stats', methods=['GET'])
def api_etfs_stats():
    """Estatísticas da base de ETFs (com cross-filter)."""
    try:
        conn = get_db()
        conds, params = _etf_cross_filter_where(request.args)
        where = (" AND " + " AND ".join(conds)) if conds else ""

        etf_count = conn.execute(f"SELECT COUNT(*) FROM etfs e WHERE 1=1{where}", params).fetchone()[0]

        # Holdings via join
        h_where = where.replace('e.', 'e2.') if where else ""
        holding_count = conn.execute(
            f"SELECT COUNT(*) FROM etf_holdings h JOIN etfs e2 ON e2.ticker=h.etf_ticker WHERE 1=1{h_where}", params
        ).fetchone()[0]
        unique_holdings = conn.execute(
            f"SELECT COUNT(DISTINCT h.holding_ticker) FROM etf_holdings h JOIN etfs e2 ON e2.ticker=h.etf_ticker WHERE h.holding_ticker IS NOT NULL{h_where}", params
        ).fetchone()[0]

        last_update = conn.execute(f"SELECT MAX(e.last_updated) FROM etfs e WHERE 1=1{where}", params).fetchone()[0]

        # By source
        by_source = []
        for r in conn.execute(f"SELECT e.data_source, COUNT(*) FROM etfs e WHERE e.data_source IS NOT NULL{where} GROUP BY e.data_source ORDER BY 2 DESC", params).fetchall():
            by_source.append({'name': r[0], 'count': r[1]})

        # By region
        by_region = []
        for r in conn.execute(
            f"SELECT {_ETF_REGION_CASE.format(t='e')} as reg, COUNT(*) FROM etfs e WHERE 1=1{where} GROUP BY 1 ORDER BY 2 DESC", params
        ).fetchall():
            by_region.append({'name': r[0], 'count': r[1]})

        # By category
        by_category = []
        for r in conn.execute(f"SELECT e.category, COUNT(*) FROM etfs e WHERE e.category IS NOT NULL{where} GROUP BY e.category ORDER BY 2 DESC", params).fetchall():
            by_category.append({'name': r[0], 'count': r[1]})

        # By issuer
        by_issuer = []
        for r in conn.execute(f"SELECT e.issuer, COUNT(*) FROM etfs e WHERE e.issuer IS NOT NULL{where} GROUP BY e.issuer ORDER BY 2 DESC", params).fetchall():
            by_issuer.append({'name': r[0], 'count': r[1]})

        # Distinct counts
        distinct_sources = len(by_source)
        distinct_categories = len(by_category)
        distinct_issuers = len(by_issuer)
        distinct_regions = len(by_region)

        # AUM total
        aum_total = conn.execute(f"SELECT COALESCE(SUM(e.aum),0) FROM etfs e WHERE e.aum IS NOT NULL{where}", params).fetchone()[0]

        conn.close()

        return jsonify({
            'success': True,
            'etfs': etf_count,
            'holdings_total': holding_count,
            'unique_holdings': unique_holdings,
            'last_update': last_update,
            'aum_total': aum_total,
            'distinct_sources': distinct_sources,
            'distinct_categories': distinct_categories,
            'distinct_issuers': distinct_issuers,
            'distinct_regions': distinct_regions,
            'by_source': by_source,
            'by_region': by_region,
            'by_category': by_category,
            'by_issuer': by_issuer,
        })
    except Exception as e:
        logger.error(f"Erro na API ETF stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/filter_options', methods=['GET'])
def api_etfs_filter_options():
    """Opções de filtro cross-filter: cada dimensão filtrada pelas OUTRAS."""
    try:
        conn = get_db()
        f_sources = request.args.get('sources', '').strip()
        f_categories = request.args.get('categories', '').strip()
        f_issuers = request.args.get('issuers', '').strip()
        f_regions = request.args.get('regions', '').strip()

        filter_map = {
            'sources': f_sources,
            'categories': f_categories,
            'issuers': f_issuers,
            'regions': f_regions,
        }

        def build_other_conds(skip_dim):
            c, p = [], []
            for dim, val in filter_map.items():
                if dim == skip_dim or not val:
                    continue
                items = [v.strip() for v in val.split(',') if v.strip()]
                if not items:
                    continue
                if dim == 'sources':
                    c.append(f"e.data_source IN ({','.join('?' * len(items))})")
                    p.extend(items)
                elif dim == 'categories':
                    c.append(f"e.category IN ({','.join('?' * len(items))})")
                    p.extend(items)
                elif dim == 'issuers':
                    c.append(f"e.issuer IN ({','.join('?' * len(items))})")
                    p.extend(items)
                elif dim == 'regions':
                    rc = []
                    for reg in items:
                        if reg == 'Brasil':
                            rc.append("e.ticker LIKE '%.SA'")
                        elif reg == 'EUA':
                            rc.append("(e.ticker NOT LIKE '%.SA' AND e.ticker NOT LIKE '%.L' AND e.ticker NOT LIKE '%.TO' AND e.ticker NOT LIKE '%.AX' AND e.ticker NOT LIKE '%.HK')")
                        else:
                            ext = {'UK': '.L', 'Canadá': '.TO', 'Austrália': '.AX', 'Hong Kong': '.HK'}.get(reg)
                            if ext:
                                rc.append(f"e.ticker LIKE '%{ext}'")
                    if rc:
                        c.append(f"({' OR '.join(rc)})")
            return c, p

        results = {}

        # Sources
        c, p = build_other_conds('sources')
        w = (" AND " + " AND ".join(c)) if c else ""
        results['sources'] = [{'name': r[0], 'count': r[1]} for r in
            conn.execute(f"SELECT e.data_source, COUNT(*) FROM etfs e WHERE e.data_source IS NOT NULL{w} GROUP BY e.data_source ORDER BY e.data_source", p).fetchall()]

        # Categories
        c, p = build_other_conds('categories')
        w = (" AND " + " AND ".join(c)) if c else ""
        results['categories'] = [{'name': r[0], 'count': r[1]} for r in
            conn.execute(f"SELECT e.category, COUNT(*) FROM etfs e WHERE e.category IS NOT NULL{w} GROUP BY e.category ORDER BY e.category", p).fetchall()]

        # Issuers
        c, p = build_other_conds('issuers')
        w = (" AND " + " AND ".join(c)) if c else ""
        results['issuers'] = [{'name': r[0], 'count': r[1]} for r in
            conn.execute(f"SELECT e.issuer, COUNT(*) FROM etfs e WHERE e.issuer IS NOT NULL{w} GROUP BY e.issuer ORDER BY e.issuer", p).fetchall()]

        # Regions
        c, p = build_other_conds('regions')
        w = (" AND " + " AND ".join(c)) if c else ""
        results['regions'] = [{'name': r[0], 'count': r[1]} for r in
            conn.execute(f"SELECT {_ETF_REGION_CASE.format(t='e')} as reg, COUNT(*) FROM etfs e WHERE 1=1{w} GROUP BY 1 ORDER BY 1", p).fetchall()]

        # Tag filters cross-filter options
        tag_results = {}
        for tt in _TAG_FILTER_TYPES:
            # Build conditions excluding this tag type
            tag_conds, tag_params = [], []
            # Apply non-tag filters
            for dim, val in filter_map.items():
                if not val:
                    continue
                items = [v.strip() for v in val.split(',') if v.strip()]
                if not items:
                    continue
                if dim == 'sources':
                    tag_conds.append(f"e.data_source IN ({','.join('?' * len(items))})")
                    tag_params.extend(items)
                elif dim == 'categories':
                    tag_conds.append(f"e.category IN ({','.join('?' * len(items))})")
                    tag_params.extend(items)
                elif dim == 'issuers':
                    tag_conds.append(f"e.issuer IN ({','.join('?' * len(items))})")
                    tag_params.extend(items)
                elif dim == 'regions':
                    rc = []
                    for reg in items:
                        if reg == 'Brasil':
                            rc.append("e.ticker LIKE '%.SA'")
                        elif reg == 'EUA':
                            rc.append("(e.ticker NOT LIKE '%.SA' AND e.ticker NOT LIKE '%.L' AND e.ticker NOT LIKE '%.TO' AND e.ticker NOT LIKE '%.AX' AND e.ticker NOT LIKE '%.HK')")
                        else:
                            ext = {'UK': '.L', 'Canadá': '.TO', 'Austrália': '.AX', 'Hong Kong': '.HK'}.get(reg)
                            if ext:
                                rc.append(f"e.ticker LIKE '%{ext}'")
                    if rc:
                        tag_conds.append(f"({' OR '.join(rc)})")
            # Apply OTHER tag filters (cross-filter)
            for tt2 in _TAG_FILTER_TYPES:
                if tt2 == tt:
                    continue
                raw = request.args.get(f'tag_{tt2}', '').strip()
                if raw:
                    items = [v.strip() for v in raw.split(',') if v.strip()]
                    placeholders = ','.join('?' * len(items))
                    tag_conds.append(
                        f"e.ticker IN (SELECT etf_ticker FROM etf_tags WHERE tag_type = ? AND tag_value IN ({placeholders}))"
                    )
                    tag_params.append(tt2)
                    tag_params.extend(items)

            tw = (" AND " + " AND ".join(tag_conds)) if tag_conds else ""
            tag_results[tt] = [{'name': r[0], 'count': r[1]} for r in
                conn.execute(
                    f"SELECT t.tag_value, COUNT(DISTINCT t.etf_ticker) "
                    f"FROM etf_tags t JOIN etfs e ON e.ticker = t.etf_ticker "
                    f"WHERE t.tag_type = ?{tw} "
                    f"GROUP BY t.tag_value ORDER BY COUNT(DISTINCT t.etf_ticker) DESC",
                    [tt] + tag_params
                ).fetchall()]

        results['tags'] = tag_results

        conn.close()
        return jsonify({'success': True, **results})
    except Exception as e:
        logger.error(f"Erro na API ETF filter_options: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/drill/top_holdings', methods=['GET'])
def api_etfs_drill_top_holdings():
    """Top holdings agregados de todos ETFs (com cross-filter)."""
    try:
        conn = get_db()
        conds, params = _etf_cross_filter_where(request.args, 'e2')
        where = (" AND " + " AND ".join(conds)) if conds else ""
        limit = request.args.get('limit', 50, type=int)

        rows = conn.execute(f"""
            SELECT h.holding_ticker, h.holding_name,
                   COUNT(DISTINCT h.etf_ticker) as in_etfs,
                   AVG(h.weight) as avg_weight,
                   SUM(h.market_value) as total_value
            FROM etf_holdings h
            JOIN etfs e2 ON e2.ticker = h.etf_ticker
            WHERE h.holding_ticker IS NOT NULL AND h.holding_ticker != ''{where}
            GROUP BY h.holding_ticker
            ORDER BY in_etfs DESC, avg_weight DESC
            LIMIT ?
        """, params + [limit]).fetchall()
        conn.close()

        return jsonify({
            'success': True,
            'holdings': [{'ticker': r[0], 'name': r[1], 'in_etfs': r[2],
                          'avg_weight': round(r[3], 2) if r[3] else None,
                          'total_value': r[4]} for r in rows],
        })
    except Exception as e:
        logger.error(f"Erro na API ETF drill holdings: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ══════════════════════════════════════════════════════════════
#  ETF Tags API  (antes de <ticker> para evitar conflito de rota)
# ══════════════════════════════════════════════════════════════

@app.route('/api/etfs/tags/stats', methods=['GET'])
def api_etfs_tags_stats():
    """Estatísticas das tags de ETFs."""
    try:
        from data_extractors.etf_extractor import ETFExtractor
        ext = ETFExtractor()
        stats = ext.get_tag_stats()
        return jsonify({'success': True, **stats})
    except Exception as e:
        logger.error(f"Erro tags stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/tags/search', methods=['GET'])
def api_etfs_tags_search():
    """Busca ETFs por tag (type + value)."""
    try:
        tag_type = request.args.get('type', '').strip()
        tag_value = request.args.get('value', '').strip()
        if not tag_type:
            return jsonify({'success': False, 'error': 'Parâmetro type é obrigatório'}), 400

        conn = get_db()
        conn.row_factory = sqlite3.Row
        if tag_value:
            rows = conn.execute("""
                SELECT t.etf_ticker, t.tag_type, t.tag_value, t.confidence, t.source,
                       e.name, e.category, e.issuer
                FROM etf_tags t JOIN etfs e ON t.etf_ticker = e.ticker
                WHERE t.tag_type = ? AND t.tag_value = ?
                ORDER BY t.confidence DESC, e.name
            """, (tag_type, tag_value)).fetchall()
        else:
            rows = conn.execute("""
                SELECT t.etf_ticker, t.tag_type, t.tag_value, t.confidence, t.source,
                       e.name, e.category, e.issuer
                FROM etf_tags t JOIN etfs e ON t.etf_ticker = e.ticker
                WHERE t.tag_type = ?
                ORDER BY t.tag_value, t.confidence DESC, e.name
            """, (tag_type,)).fetchall()
        conn.close()

        return jsonify({
            'success': True,
            'count': len(rows),
            'results': [dict(r) for r in rows],
        })
    except Exception as e:
        logger.error(f"Erro tags search: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/tags/values', methods=['GET'])
def api_etfs_tags_values():
    """Lista valores únicos de um tag_type (para filtros)."""
    try:
        tag_type = request.args.get('type', '').strip()
        conn = get_db()
        if tag_type:
            rows = conn.execute("""
                SELECT tag_value, COUNT(*) as cnt FROM etf_tags
                WHERE tag_type = ? GROUP BY tag_value ORDER BY cnt DESC
            """, (tag_type,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT tag_type, tag_value, COUNT(*) as cnt FROM etf_tags
                GROUP BY tag_type, tag_value ORDER BY tag_type, cnt DESC
            """).fetchall()
        conn.close()

        if tag_type:
            return jsonify({'success': True, 'type': tag_type,
                            'values': [{'value': v, 'count': c} for v, c in rows]})
        else:
            return jsonify({'success': True,
                            'values': [{'type': t, 'value': v, 'count': c} for t, v, c in rows]})
    except Exception as e:
        logger.error(f"Erro tags values: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/tags/auto-tag', methods=['POST'])
def api_etfs_auto_tag():
    """Executa auto-tagging (all ou ticker específico)."""
    try:
        from data_extractors.etf_extractor import ETFExtractor
        ext = ETFExtractor()
        ticker = request.json.get('ticker') if request.is_json else None
        if ticker:
            tags = ext.auto_tag_etf(ticker.upper())
            count = ext.save_tags(ticker.upper(), tags)
            return jsonify({'success': True, 'ticker': ticker.upper(), 'tags_saved': count})
        else:
            result = ext.auto_tag_all()
            return jsonify({'success': True, **result})
    except Exception as e:
        logger.error(f"Erro auto-tag: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/<ticker>', methods=['GET'])
def api_etf_detail(ticker):
    """Detalhes de um ETF + seus holdings + breakdowns."""
    try:
        conn = get_db()
        conn.row_factory = sqlite3.Row

        etf = conn.execute("SELECT * FROM etfs WHERE ticker = ?", (ticker.upper(),)).fetchone()
        if not etf:
            conn.close()
            return jsonify({'success': False, 'error': 'ETF não encontrado'}), 404

        holdings = conn.execute(
            "SELECT * FROM etf_holdings WHERE etf_ticker = ? ORDER BY weight DESC",
            (ticker.upper(),),
        ).fetchall()

        tags = conn.execute(
            "SELECT tag_type, tag_value, confidence, source FROM etf_tags WHERE etf_ticker = ? ORDER BY tag_type",
            (ticker.upper(),),
        ).fetchall()

        conn.close()

        holdings_list = [dict(h) for h in holdings]

        # Build breakdowns
        sector_map, asset_map, country_map = {}, {}, {}
        for h in holdings_list:
            w = h.get('weight') or 0
            s = h.get('sector') or 'N/A'
            a = h.get('asset_class') or 'N/A'
            c = h.get('country') or 'N/A'
            sector_map[s] = sector_map.get(s, 0) + w
            asset_map[a] = asset_map.get(a, 0) + w
            country_map[c] = country_map.get(c, 0) + w

        def _sorted_breakdown(m):
            return sorted([{'name': k, 'weight': round(v, 2)} for k, v in m.items()], key=lambda x: x['weight'], reverse=True)

        return jsonify({
            'success': True,
            'etf': dict(etf),
            'holdings': holdings_list,
            'tags': [dict(t) for t in tags],
            'breakdowns': {
                'sector': _sorted_breakdown(sector_map),
                'asset_class': _sorted_breakdown(asset_map),
                'country': _sorted_breakdown(country_map)[:30],
            },
        })
    except Exception as e:
        logger.error(f"Erro na API ETF detail: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/search', methods=['GET'])
def api_etfs_reverse_search():
    """Busca reversa: em quais ETFs um ticker aparece?"""
    try:
        q = request.args.get('q', '').strip().upper()
        if not q:
            return jsonify({'success': False, 'error': 'Parâmetro q é obrigatório'}), 400

        conn = get_db()
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT h.etf_ticker, e.name as etf_name, e.category, e.aum,
                   h.weight, h.holding_name, h.holding_ticker,
                   e.data_source, e.total_holdings
            FROM etf_holdings h
            JOIN etfs e ON e.ticker = h.etf_ticker
            WHERE h.holding_ticker LIKE ?
               OR h.holding_name LIKE ?
            ORDER BY h.weight DESC
        """, (f'%{q}%', f'%{q}%')).fetchall()

        conn.close()

        return jsonify({
            'success': True,
            'query': q,
            'results': [dict(r) for r in rows],
            'total': len(rows),
        })
    except Exception as e:
        logger.error(f"Erro na API ETF search: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/overlap', methods=['GET'])
def api_etfs_overlap():
    """Calcula sobreposição entre dois ETFs."""
    try:
        etf1 = request.args.get('etf1', '').strip().upper()
        etf2 = request.args.get('etf2', '').strip().upper()
        if not etf1 or not etf2:
            return jsonify({'success': False, 'error': 'Parâmetros etf1 e etf2 são obrigatórios'}), 400

        conn = get_db()
        conn.row_factory = sqlite3.Row

        h1 = conn.execute(
            "SELECT holding_ticker, holding_name, weight FROM etf_holdings WHERE etf_ticker = ?", (etf1,)
        ).fetchall()
        h2 = conn.execute(
            "SELECT holding_ticker, holding_name, weight FROM etf_holdings WHERE etf_ticker = ?", (etf2,)
        ).fetchall()

        n1 = conn.execute("SELECT name FROM etfs WHERE ticker = ?", (etf1,)).fetchone()
        n2 = conn.execute("SELECT name FROM etfs WHERE ticker = ?", (etf2,)).fetchone()
        conn.close()

        t1 = {r['holding_ticker'] for r in h1 if r['holding_ticker']}
        t2 = {r['holding_ticker'] for r in h2 if r['holding_ticker']}
        common = t1 & t2
        union = t1 | t2
        pct = round(len(common) / len(union) * 100, 2) if union else 0

        w1 = {r['holding_ticker']: dict(r) for r in h1 if r['holding_ticker']}
        w2 = {r['holding_ticker']: dict(r) for r in h2 if r['holding_ticker']}
        common_detail = []
        for tk in sorted(common):
            common_detail.append({
                'ticker': tk,
                'name': (w1.get(tk) or w2.get(tk, {})).get('holding_name', ''),
                'weight_etf1': w1.get(tk, {}).get('weight'),
                'weight_etf2': w2.get(tk, {}).get('weight'),
            })
        common_detail.sort(key=lambda x: (x.get('weight_etf1') or 0), reverse=True)

        return jsonify({
            'success': True,
            'etf1': etf1, 'etf1_name': n1['name'] if n1 else etf1,
            'etf2': etf2, 'etf2_name': n2['name'] if n2 else etf2,
            'holdings_etf1': len(t1),
            'holdings_etf2': len(t2),
            'common': len(common),
            'overlap_pct': pct,
            'common_holdings': common_detail[:100],
        })
    except Exception as e:
        logger.error(f"Erro na API ETF overlap: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/compare', methods=['GET'])
def api_etfs_compare():
    """Compara N ETFs side-by-side (overlap + pesos)."""
    try:
        raw = request.args.get('tickers', '').strip().upper()
        if not raw:
            return jsonify({'success': False, 'error': 'Parâmetro tickers é obrigatório'}), 400
        tickers = [t.strip() for t in raw.split(',') if t.strip()][:10]
        if len(tickers) < 2:
            return jsonify({'success': False, 'error': 'Pelo menos 2 ETFs necessários'}), 400

        conn = get_db()
        conn.row_factory = sqlite3.Row

        etf_meta = {}
        etf_holdings = {}
        for tk in tickers:
            etf = conn.execute("SELECT ticker, name, category, aum, total_holdings, data_source FROM etfs WHERE ticker = ?", (tk,)).fetchone()
            if etf:
                etf_meta[tk] = dict(etf)
            rows = conn.execute(
                "SELECT holding_ticker, holding_name, weight, sector, asset_class, country FROM etf_holdings WHERE etf_ticker = ? ORDER BY weight DESC",
                (tk,),
            ).fetchall()
            etf_holdings[tk] = {r['holding_ticker']: dict(r) for r in rows if r['holding_ticker']}
        conn.close()

        # Encontrar tickers válidos
        valid = [t for t in tickers if t in etf_meta]
        if len(valid) < 2:
            return jsonify({'success': False, 'error': 'ETFs não encontrados'}), 404

        # Overlap matrix
        overlap_matrix = {}
        for i, t1 in enumerate(valid):
            s1 = set(etf_holdings.get(t1, {}).keys())
            for t2 in valid[i+1:]:
                s2 = set(etf_holdings.get(t2, {}).keys())
                common = s1 & s2
                union = s1 | s2
                pct = round(len(common) / len(union) * 100, 2) if union else 0
                overlap_matrix[f"{t1}|{t2}"] = {'common': len(common), 'pct': pct}

        # Holdings presentes em todos os ETFs
        all_sets = [set(etf_holdings.get(t, {}).keys()) for t in valid]
        common_all = set.intersection(*all_sets) if all_sets else set()

        # Top common holdings com pesos por ETF
        common_detail = []
        for tk in common_all:
            row = {'ticker': tk, 'name': ''}
            for etf_tk in valid:
                h = etf_holdings.get(etf_tk, {}).get(tk, {})
                if not row['name']:
                    row['name'] = h.get('holding_name', '')
                row[f'weight_{etf_tk}'] = h.get('weight')
            common_detail.append(row)
        common_detail.sort(key=lambda x: max((x.get(f'weight_{t}') or 0) for t in valid), reverse=True)

        # Sector breakdown por ETF
        sector_breakdown = {}
        for tk in valid:
            sectors = {}
            for h in etf_holdings.get(tk, {}).values():
                s = h.get('sector') or 'N/A'
                sectors[s] = sectors.get(s, 0) + (h.get('weight') or 0)
            sector_breakdown[tk] = sorted(sectors.items(), key=lambda x: x[1], reverse=True)

        return jsonify({
            'success': True,
            'etfs': [etf_meta[t] for t in valid],
            'tickers': valid,
            'overlap_matrix': overlap_matrix,
            'common_all': len(common_all),
            'common_holdings': common_detail[:100],
            'sector_breakdown': sector_breakdown,
        })
    except Exception as e:
        logger.error(f"Erro na API ETF compare: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/etfs/export', methods=['GET'])
def api_etfs_export():
    """Exporta holdings de um ou mais ETFs como CSV."""
    try:
        raw = request.args.get('tickers', '').strip().upper()
        if not raw:
            return jsonify({'success': False, 'error': 'Parâmetro tickers é obrigatório'}), 400
        tickers = [t.strip() for t in raw.split(',') if t.strip()][:20]

        conn = get_db()
        placeholders = ','.join('?' * len(tickers))
        rows = conn.execute(f"""
            SELECT h.etf_ticker, h.holding_ticker, h.holding_name, h.weight,
                   h.shares, h.market_value, h.sector, h.asset_class,
                   h.country, h.cusip, h.isin, h.report_date
            FROM etf_holdings h
            WHERE h.etf_ticker IN ({placeholders})
            ORDER BY h.etf_ticker, h.weight DESC
        """, tickers).fetchall()
        conn.close()

        import io as _io
        import csv as _csv
        output = _io.StringIO()
        writer = _csv.writer(output)
        writer.writerow(['ETF', 'Ticker', 'Nome', 'Peso (%)', 'Shares', 'Market Value',
                        'Setor', 'Asset Class', 'País', 'CUSIP', 'ISIN', 'Data'])
        for r in rows:
            writer.writerow(list(r))

        from flask import Response
        resp = Response(output.getvalue(), mimetype='text/csv')
        fname = tickers[0] if len(tickers) == 1 else 'etfs_export'
        resp.headers['Content-Disposition'] = f'attachment; filename={fname}_holdings.csv'
        return resp
    except Exception as e:
        logger.error(f"Erro na API ETF export: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================
# ESTUDO ANLOC — Múltiplos setoriais com filtros configuráveis
# ============================================================

@app.route('/estudoanloc')
def estudoanloc_page():
    """Página de estudo de múltiplos setoriais Anloc."""
    return render_template('estudoanloc.html')


@app.route('/api/estudoanloc/cross_sector', methods=['POST'])
def api_estudoanloc_cross_sector():
    """Calcula múltiplos agregados por setor para comparação cross-sector."""
    try:
        data = request.get_json() or {}
        fiscal_year = int(data.get('fiscal_year', 2025))
        min_ev_usd = float(data.get('min_ev_usd', 100_000_000))
        max_ev_ebitda = float(data.get('max_ev_ebitda', 60))
        region_filter = data.get('region', '')  # empty = global

        import datetime
        current_year = datetime.date.today().year
        is_current = (fiscal_year >= current_year)

        conn = get_db()

        region_clause = ""
        params = []

        if is_current:
            # TTM + annual fallback com EV atual
            prev_year = fiscal_year - 1
            sql = """
                WITH ttm_data AS (
                    SELECT q.company_basic_data_id AS cid,
                           cbd.yahoo_sector AS sector,
                           cbd.yahoo_country AS country,
                           dg.sub_group AS region,
                           q.total_revenue_ttm AS revenue,
                           q.ebitda_ttm AS ebitda,
                           q.free_cash_flow_ttm AS fcf,
                           cbd.enterprise_value AS ev,
                           CASE WHEN q.fx_rate_to_usd > 0 THEN cbd.enterprise_value * q.fx_rate_to_usd ELSE NULL END AS ev_usd,
                           'TTM' AS src
                    FROM company_financials_historical q
                    JOIN company_basic_data cbd ON q.company_basic_data_id = cbd.id
                    LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                    WHERE q.period_type = 'quarterly'
                      AND q.ttm_quarters_count >= 4
                      AND q.total_revenue_ttm IS NOT NULL
                      AND q.ebitda_ttm IS NOT NULL
                      AND cbd.enterprise_value IS NOT NULL
                      AND cbd.yahoo_sector IS NOT NULL
                      AND q.id IN (
                          SELECT MAX(q2.id) FROM company_financials_historical q2
                          WHERE q2.company_basic_data_id = q.company_basic_data_id
                            AND q2.period_type = 'quarterly' AND q2.ttm_quarters_count >= 4
                      )
                ),
                annual_data AS (
                    SELECT cfh.company_basic_data_id AS cid,
                           cbd.yahoo_sector AS sector,
                           cbd.yahoo_country AS country,
                           dg.sub_group AS region,
                           cfh.total_revenue AS revenue,
                           cfh.normalized_ebitda AS ebitda,
                           cfh.free_cash_flow AS fcf,
                           cbd.enterprise_value AS ev,
                           CASE WHEN cfh.fx_rate_to_usd > 0 THEN cbd.enterprise_value * cfh.fx_rate_to_usd ELSE NULL END AS ev_usd,
                           'Annual' AS src
                    FROM company_financials_historical cfh
                    JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                    LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                    WHERE cfh.period_type = 'annual'
                      AND cfh.fiscal_year = ?
                      AND cbd.enterprise_value IS NOT NULL
                      AND cbd.yahoo_sector IS NOT NULL
                      AND cfh.total_revenue IS NOT NULL
                      AND cfh.normalized_ebitda IS NOT NULL
                      AND cfh.company_basic_data_id NOT IN (SELECT cid FROM ttm_data)
                )
                SELECT * FROM ttm_data
                UNION ALL
                SELECT * FROM annual_data
            """
            params = [prev_year]
        else:
            sql = """
                SELECT cfh.company_basic_data_id AS cid,
                       cbd.yahoo_sector AS sector,
                       cbd.yahoo_country AS country,
                       dg.sub_group AS region,
                       cfh.total_revenue AS revenue,
                       cfh.normalized_ebitda AS ebitda,
                       cfh.free_cash_flow AS fcf,
                       cfh.enterprise_value_estimated AS ev,
                       cfh.enterprise_value_usd AS ev_usd,
                       'Annual' AS src
                FROM company_financials_historical cfh
                JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE cfh.period_type = 'annual'
                  AND cfh.fiscal_year = ?
                  AND cbd.yahoo_sector IS NOT NULL
                  AND cfh.total_revenue IS NOT NULL
                  AND cfh.normalized_ebitda IS NOT NULL
            """
            params = [fiscal_year]

        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()

        if df.empty:
            return jsonify({'success': True, 'sectors': [], 'metadata': {'fiscal_year': fiscal_year, 'total': 0}})

        # Region filter
        if region_filter:
            if region_filter == 'Brazil':
                df = df[df['country'] == 'Brazil']
            elif region_filter == 'LATAM':
                df = df[df['region'] == 'Latin America & Caribbean']
            else:
                df = df[df['region'] == region_filter]

        # EV filter
        if min_ev_usd > 0:
            ev_col = 'ev_usd' if df['ev_usd'].notna().sum() > df['ev'].notna().sum() * 0.5 else 'ev'
            df = df[df[ev_col].notna() & (df[ev_col] >= min_ev_usd)]

        # Calculate multiples
        df['ev_ebitda'] = np.where((df['ebitda'] > 0) & df['ev'].notna(), df['ev'] / df['ebitda'], np.nan)
        df['ev_revenue'] = np.where((df['revenue'] > 0) & df['ev'].notna(), df['ev'] / df['revenue'], np.nan)
        df['fcf_revenue'] = np.where((df['revenue'] > 0) & df['fcf'].notna(), df['fcf'] / df['revenue'], np.nan)
        df['fcf_ebitda'] = np.where((df['ebitda'] > 0) & df['fcf'].notna(), df['fcf'] / df['ebitda'], np.nan)

        # Cap EV/EBITDA
        if max_ev_ebitda > 0:
            df.loc[df['ev_ebitda'].notna() & (df['ev_ebitda'] > max_ev_ebitda), 'ev_ebitda'] = np.nan

        # Aggregate by sector
        sectors_result = []
        for sector_name in sorted(df['sector'].dropna().unique()):
            sub = df[df['sector'] == sector_name]
            n = len(sub)
            if n < 3:
                continue
            entry = {'sector': sector_name, 'n': n}
            for metric in ['ev_ebitda', 'ev_revenue', 'fcf_revenue', 'fcf_ebitda']:
                valid = sub[metric].dropna()
                nv = len(valid)
                if nv == 0:
                    entry[metric] = {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0}
                else:
                    entry[metric] = {
                        'median': round(float(np.median(valid)), 4),
                        'p25': round(float(np.percentile(valid, 25)), 4),
                        'p75': round(float(np.percentile(valid, 75)), 4),
                        'mean': round(float(np.mean(valid)), 4),
                        'n': nv
                    }
            sectors_result.append(entry)

        return jsonify({
            'success': True,
            'sectors': sectors_result,
            'metadata': {
                'fiscal_year': fiscal_year,
                'region': region_filter or 'Global',
                'total_companies': len(df),
                'total_sectors': len(sectors_result)
            }
        })
    except Exception as e:
        logger.error(f"Erro estudoanloc cross_sector: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/filters', methods=['GET'])
def api_estudoanloc_filters():
    """Retorna opções de filtro disponíveis para o estudo."""
    try:
        conn = get_db()
        sectors = [r[0] for r in conn.execute(
            "SELECT DISTINCT cbd.yahoo_sector FROM company_basic_data cbd WHERE cbd.yahoo_sector IS NOT NULL ORDER BY cbd.yahoo_sector"
        ).fetchall()]

        years = [r[0] for r in conn.execute(
            """SELECT fiscal_year FROM company_financials_historical
               WHERE period_type='annual' AND fiscal_year >= 2021
                 AND total_revenue IS NOT NULL AND normalized_ebitda IS NOT NULL
               GROUP BY fiscal_year HAVING COUNT(*) >= 100
               ORDER BY fiscal_year DESC"""
        ).fetchall()]

        # Adicionar ano corrente para "múltiplos atuais" (EV atual + últimos financeiros)
        import datetime
        current_year = datetime.date.today().year
        if current_year not in years:
            years = [current_year] + years

        regions = [r[0] for r in conn.execute(
            "SELECT DISTINCT dg.sub_group FROM damodaran_global dg WHERE dg.sub_group IS NOT NULL ORDER BY dg.sub_group"
        ).fetchall()]

        conn.close()
        return jsonify({
            'success': True,
            'sectors': sectors,
            'years': years,
            'regions': regions
        })
    except Exception as e:
        logger.error(f"Erro estudoanloc filters: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/industries', methods=['GET'])
def api_estudoanloc_industries():
    """Retorna indústrias disponíveis para um setor específico."""
    try:
        sector = request.args.get('sector', '').strip()
        if not sector:
            return jsonify({'success': True, 'industries': []})

        conn = get_db()
        industries = [r[0] for r in conn.execute(
            "SELECT DISTINCT cbd.yahoo_industry FROM company_basic_data cbd WHERE cbd.yahoo_sector = ? AND cbd.yahoo_industry IS NOT NULL ORDER BY cbd.yahoo_industry",
            (sector,)
        ).fetchall()]
        conn.close()

        return jsonify({'success': True, 'industries': industries})
    except Exception as e:
        logger.error(f"Erro estudoanloc industries: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/calculate', methods=['POST'])
def api_estudoanloc_calculate():
    """Calcula múltiplos setoriais com filtros configuráveis e fallback TTM."""
    try:
        data = request.get_json()
        sector = data.get('sector', '').strip()
        fiscal_year = int(data.get('fiscal_year', 2025))
        use_ttm_fallback = data.get('use_ttm_fallback', True)
        min_ttm_quarters = int(data.get('min_ttm_quarters', 4))

        # Filtros configuráveis (4 camadas)
        filters = data.get('filters', {})
        min_ev_usd = float(filters.get('min_ev_usd', 100_000_000))
        max_ev_ebitda = float(filters.get('max_ev_ebitda', 60))
        require_positive_ebitda = filters.get('require_positive_ebitda', True)
        min_history_years = int(filters.get('min_history_years', 0))
        selected_industries = data.get('industries', [])

        if not sector:
            return jsonify({'success': False, 'error': 'Setor é obrigatório'}), 400

        import datetime
        current_year = datetime.date.today().year
        is_current_multiples = (fiscal_year >= current_year)

        conn = get_db()

        # === STEP 1: Carregar dados ===
        industry_filter = ""
        if selected_industries:
            placeholders = ','.join('?' * len(selected_industries))
            industry_filter = f"AND cbd.yahoo_industry IN ({placeholders})"

        if is_current_multiples:
            # --- MODO MÚLTIPLOS ATUAIS (FY corrente/futuro) ---
            # Financeiros: TTM mais recente (Q2025/Q2026) || Annual do ano anterior
            # EV: cbd.enterprise_value (EV de mercado atual)
            # 1a) TTM: busca último quarterly com TTM completo (fiscal_year mais recente)
            params_ttm = [min_ttm_quarters, sector]
            if selected_industries:
                params_ttm.extend(selected_industries)

            ttm_sql = f"""
                WITH latest_q AS (
                    SELECT q.company_basic_data_id AS cid,
                           MAX(q.period_date) AS max_date
                    FROM company_financials_historical q
                    JOIN company_basic_data cbd2 ON q.company_basic_data_id = cbd2.id
                    WHERE q.period_type = 'quarterly'
                      AND q.ttm_quarters_count >= ?
                      AND q.total_revenue_ttm IS NOT NULL
                      AND q.ebitda_ttm IS NOT NULL
                      AND cbd2.yahoo_sector = ?
                    GROUP BY q.company_basic_data_id
                )
                SELECT q.company_basic_data_id AS cid,
                       cbd.ticker, cbd.yahoo_industry AS industry,
                       cbd.yahoo_country AS country,
                       dg.sub_group AS region,
                       q.total_revenue_ttm AS revenue,
                       q.ebitda_ttm AS ebitda,
                       q.ebitda_ttm AS ebitda_raw,
                       q.free_cash_flow_ttm AS fcf,
                       cbd.enterprise_value AS ev,
                       CASE WHEN q.fx_rate_to_usd IS NOT NULL AND q.fx_rate_to_usd > 0
                            THEN cbd.enterprise_value * q.fx_rate_to_usd
                            ELSE NULL END AS ev_usd,
                       q.total_revenue_usd AS revenue_usd,
                       q.ebitda_usd AS ebitda_usd,
                       q.free_cash_flow_usd AS fcf_usd,
                       'Current+TTM' AS data_source
                FROM company_financials_historical q
                JOIN latest_q lq ON q.company_basic_data_id = lq.cid AND q.period_date = lq.max_date
                JOIN company_basic_data cbd ON q.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE q.period_type = 'quarterly'
                  AND cbd.enterprise_value IS NOT NULL
                  {industry_filter}
            """
            df_ttm = pd.read_sql_query(ttm_sql, conn, params=params_ttm)

            # 1b) Annual fallback: ano anterior com EV atual
            prev_year = fiscal_year - 1
            ttm_cids = set(df_ttm['cid'].tolist()) if not df_ttm.empty else set()

            params_annual = [prev_year, sector]
            if selected_industries:
                params_annual.extend(selected_industries)

            annual_sql = f"""
                SELECT cfh.company_basic_data_id AS cid,
                       cbd.ticker, cbd.yahoo_industry AS industry,
                       cbd.yahoo_country AS country,
                       dg.sub_group AS region,
                       cfh.total_revenue AS revenue,
                       cfh.normalized_ebitda AS ebitda,
                       cfh.ebitda AS ebitda_raw,
                       cfh.free_cash_flow AS fcf,
                       cbd.enterprise_value AS ev,
                       CASE WHEN cfh.fx_rate_to_usd IS NOT NULL AND cfh.fx_rate_to_usd > 0
                            THEN cbd.enterprise_value * cfh.fx_rate_to_usd
                            ELSE NULL END AS ev_usd,
                       cfh.total_revenue_usd AS revenue_usd,
                       cfh.ebitda_usd AS ebitda_usd,
                       cfh.free_cash_flow_usd AS fcf_usd,
                       'Current+FY{prev_year}' AS data_source
                FROM company_financials_historical cfh
                JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE cfh.period_type = 'annual'
                  AND cfh.fiscal_year = ?
                  AND cbd.yahoo_sector = ?
                  AND cbd.enterprise_value IS NOT NULL
                  {industry_filter}
            """
            df_annual = pd.read_sql_query(annual_sql, conn, params=params_annual)

            # Excluir do annual quem já tem TTM
            if not df_annual.empty and ttm_cids:
                df_annual = df_annual[~df_annual['cid'].isin(ttm_cids)]

        else:
            # --- MODO HISTÓRICO (ano com dados consolidados) ---
            params_annual_sql = [fiscal_year, sector]
            if selected_industries:
                params_annual_sql.extend(selected_industries)

            annual_sql = f"""
                SELECT cfh.company_basic_data_id AS cid,
                       cbd.ticker, cbd.yahoo_industry AS industry,
                       cbd.yahoo_country AS country,
                       dg.sub_group AS region,
                       cfh.total_revenue AS revenue,
                       cfh.normalized_ebitda AS ebitda,
                       cfh.ebitda AS ebitda_raw,
                       cfh.free_cash_flow AS fcf,
                       cfh.enterprise_value_estimated AS ev,
                       cfh.enterprise_value_usd AS ev_usd,
                       cfh.total_revenue_usd AS revenue_usd,
                       cfh.ebitda_usd AS ebitda_usd,
                       cfh.free_cash_flow_usd AS fcf_usd,
                       'Annual' AS data_source
                FROM company_financials_historical cfh
                JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE cfh.period_type = 'annual'
                  AND cfh.fiscal_year = ?
                  AND cbd.yahoo_sector = ?
                  {industry_filter}
            """
            df_annual = pd.read_sql_query(annual_sql, conn, params=params_annual_sql)

            # === STEP 2: Carregar dados TTM (fallback) ===
            df_ttm = pd.DataFrame()
            if use_ttm_fallback:
                annual_cids = set(df_annual['cid'].tolist()) if not df_annual.empty else set()

                ttm_sql = f"""
                    WITH latest_q AS (
                        SELECT q.company_basic_data_id AS cid,
                               MAX(q.period_date) AS max_date
                        FROM company_financials_historical q
                        JOIN company_basic_data cbd2 ON q.company_basic_data_id = cbd2.id
                        WHERE q.period_type = 'quarterly'
                          AND q.fiscal_year = ?
                          AND q.ttm_quarters_count >= ?
                          AND q.total_revenue_ttm IS NOT NULL
                          AND cbd2.yahoo_sector = ?
                        GROUP BY q.company_basic_data_id
                    )
                    SELECT q.company_basic_data_id AS cid,
                           cbd.ticker, cbd.yahoo_industry AS industry,
                           cbd.yahoo_country AS country,
                           dg.sub_group AS region,
                           q.total_revenue_ttm AS revenue,
                           q.ebitda_ttm AS ebitda,
                           q.ebitda_ttm AS ebitda_raw,
                           q.free_cash_flow_ttm AS fcf,
                           q.enterprise_value_estimated AS ev,
                           q.enterprise_value_usd AS ev_usd,
                           q.total_revenue_usd AS revenue_usd,
                           q.ebitda_usd AS ebitda_usd,
                           q.free_cash_flow_usd AS fcf_usd,
                           'TTM' AS data_source
                    FROM company_financials_historical q
                    JOIN latest_q lq ON q.company_basic_data_id = lq.cid AND q.period_date = lq.max_date
                    JOIN company_basic_data cbd ON q.company_basic_data_id = cbd.id
                    LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                    WHERE q.period_type = 'quarterly'
                      AND q.fiscal_year = ?
                      AND q.ttm_quarters_count >= ?
                      {industry_filter}
                """
                params_ttm = [fiscal_year, min_ttm_quarters, sector, fiscal_year, min_ttm_quarters]
                if selected_industries:
                    params_ttm.extend(selected_industries)

                df_ttm_raw = pd.read_sql_query(ttm_sql, conn, params=params_ttm)
                # Excluir empresas que já têm dado anual
                if not df_ttm_raw.empty and annual_cids:
                    df_ttm = df_ttm_raw[~df_ttm_raw['cid'].isin(annual_cids)]
                elif not df_ttm_raw.empty:
                    df_ttm = df_ttm_raw

        conn.close()

        # === STEP 3: Combinar annual + TTM ===
        frames = [df_annual]
        if not df_ttm.empty:
            frames.append(df_ttm)
        df = pd.concat(frames, ignore_index=True)

        if df.empty:
            empty_stats = {'label': '', 'n': 0, 'n_annual': 0, 'n_ttm': 0,
                           'ev_ebitda': {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0},
                           'ev_revenue': {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0},
                           'fcf_revenue': {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0},
                           'fcf_ebitda': {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0}}
            return jsonify({
                'success': True,
                'summary': {'global': {**empty_stats, 'label': f'{sector} — Global'},
                            'latam': {**empty_stats, 'label': f'{sector} — LATAM'},
                            'brasil': {**empty_stats, 'label': f'{sector} — Brasil'}},
                'by_industry': [],
                'by_geography': [],
                'companies': [],
                'metadata': {'total_before_filters': 0, 'total_after_filters': 0, 'filters_applied': [],
                             'sector': sector, 'fiscal_year': fiscal_year, 'use_ttm_fallback': use_ttm_fallback,
                             'min_ttm_quarters': min_ttm_quarters,
                             'filters_config': {'min_ev_usd': min_ev_usd, 'max_ev_ebitda': max_ev_ebitda,
                                                'require_positive_ebitda': require_positive_ebitda, 'min_history_years': min_history_years}}
            })

        total_before = len(df)
        filters_applied = []

        # === STEP 4: Aplicar filtros (4 camadas) ===
        # Camada 1: Dados mínimos (receita e EBITDA não nulos)
        mask = df['revenue'].notna() & df['ebitda'].notna()
        n_removed = (~mask).sum()
        if n_removed > 0:
            filters_applied.append(f"Camada 1 — Dados mínimos: -{n_removed} (receita ou EBITDA nulo)")
        df = df[mask]

        # Camada 2: Tamanho mínimo (EV > threshold em USD)
        if min_ev_usd > 0:
            ev_col = 'ev_usd' if df['ev_usd'].notna().sum() > df['ev'].notna().sum() * 0.5 else 'ev'
            mask_ev = df[ev_col].notna() & (df[ev_col] >= min_ev_usd)
            n_removed = len(df) - mask_ev.sum()
            if n_removed > 0:
                filters_applied.append(f"Camada 2 — EV mínimo (USD {min_ev_usd/1e6:.0f}M): -{n_removed}")
            df = df[mask_ev]

        # Calcular múltiplos
        df['ev_ebitda'] = np.where(
            (df['ebitda'].notna()) & (df['ebitda'] > 0) & (df['ev'].notna()),
            df['ev'] / df['ebitda'], np.nan
        )
        df['ev_revenue'] = np.where(
            (df['revenue'].notna()) & (df['revenue'] > 0) & (df['ev'].notna()),
            df['ev'] / df['revenue'], np.nan
        )
        df['fcf_revenue'] = np.where(
            (df['revenue'].notna()) & (df['revenue'] > 0) & (df['fcf'].notna()),
            df['fcf'] / df['revenue'], np.nan
        )
        df['fcf_ebitda'] = np.where(
            (df['ebitda'].notna()) & (df['ebitda'] > 0) & (df['fcf'].notna()),
            df['fcf'] / df['ebitda'], np.nan
        )

        # Camada 3: EBITDA positivo para EV/EBITDA
        if require_positive_ebitda:
            n_neg = (df['ebitda'] <= 0).sum()
            if n_neg > 0:
                filters_applied.append(f"Camada 3 — EBITDA negativo: {n_neg} empresas (excluídas do EV/EBITDA)")
            # Não remove linhas, apenas marca ev_ebitda como NaN (já feito acima)

        # Camada 4: Teto EV/EBITDA
        if max_ev_ebitda > 0:
            mask_cap = df['ev_ebitda'].notna() & (df['ev_ebitda'] > max_ev_ebitda)
            n_capped = mask_cap.sum()
            if n_capped > 0:
                filters_applied.append(f"Camada 4 — Teto EV/EBITDA ({max_ev_ebitda:.0f}x): -{n_capped}")
            df.loc[mask_cap, 'ev_ebitda'] = np.nan

        total_after = len(df)

        # === STEP 5: Calcular estatísticas ===
        def calc_stats(subset, label):
            """Calcula estatísticas para um subconjunto."""
            n = len(subset)
            if n == 0:
                return {'label': label, 'n': 0, 'n_annual': 0, 'n_ttm': 0,
                        'ev_ebitda': {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0},
                        'ev_revenue': {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0},
                        'fcf_revenue': {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0, 'n_positive': 0, 'pct_positive': 0, 'median_positive': None},
                        'fcf_ebitda': {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0, 'n_positive': 0, 'pct_positive': 0, 'median_positive': None}}

            result = {'label': label, 'n': n}

            for metric in ['ev_ebitda', 'ev_revenue', 'fcf_revenue', 'fcf_ebitda']:
                valid = subset[metric].dropna()
                nv = len(valid)
                if nv == 0:
                    result[metric] = {'median': None, 'p25': None, 'p75': None, 'mean': None, 'n': 0}
                    continue

                result[metric] = {
                    'median': round(float(np.median(valid)), 4),
                    'p25': round(float(np.percentile(valid, 25)), 4),
                    'p75': round(float(np.percentile(valid, 75)), 4),
                    'mean': round(float(np.mean(valid)), 4),
                    'n': nv
                }

                # Para FCF metrics: stats adicionais
                if metric.startswith('fcf'):
                    positive = valid[valid > 0]
                    result[metric]['n_positive'] = len(positive)
                    result[metric]['pct_positive'] = round(len(positive) / nv * 100, 1) if nv > 0 else 0
                    result[metric]['median_positive'] = round(float(np.median(positive)), 4) if len(positive) > 0 else None

            # Fonte de dados
            result['n_annual'] = int(subset['data_source'].isin(['Annual']).sum() + subset['data_source'].str.startswith('Current+FY').sum())
            result['n_ttm'] = int(subset['data_source'].isin(['TTM', 'Current+TTM']).sum())
            return result

        # Global (setor inteiro)
        summary_global = calc_stats(df, f'{sector} — Global')

        # LATAM
        df_latam = df[df['region'] == 'Latin America & Caribbean']
        summary_latam = calc_stats(df_latam, f'{sector} — LATAM')

        # Brasil
        df_brasil = df[df['country'] == 'Brazil']
        summary_brasil = calc_stats(df_brasil, f'{sector} — Brasil')

        summary = {
            'global': summary_global,
            'latam': summary_latam,
            'brasil': summary_brasil
        }

        # === STEP 6: Por indústria ===
        by_industry = []
        for ind in sorted(df['industry'].dropna().unique()):
            sub = df[df['industry'] == ind]
            if len(sub) >= 1:
                stats = calc_stats(sub, ind)
                # Sub-recortes geográficos por indústria
                sub_latam = sub[sub['region'] == 'Latin America & Caribbean']
                sub_brasil = sub[sub['country'] == 'Brazil']
                stats['latam'] = calc_stats(sub_latam, f'{ind} — LATAM')
                stats['brasil'] = calc_stats(sub_brasil, f'{ind} — Brasil')
                by_industry.append(stats)

        # === STEP 7: Por geografia ===
        by_geography = []
        for reg in sorted(df['region'].dropna().unique()):
            sub = df[df['region'] == reg]
            if len(sub) >= 1:
                by_geography.append(calc_stats(sub, reg))

        # === STEP 8: Lista de empresas (drill-down) ===
        companies = []
        for _, row in df.iterrows():
            companies.append({
                'cid': int(row['cid']),
                'ticker': row['ticker'],
                'industry': row['industry'],
                'country': row['country'],
                'region': row['region'],
                'data_source': row['data_source'],
                'revenue': round(float(row['revenue']), 0) if pd.notna(row['revenue']) else None,
                'ebitda': round(float(row['ebitda']), 0) if pd.notna(row['ebitda']) else None,
                'fcf': round(float(row['fcf']), 0) if pd.notna(row['fcf']) else None,
                'ev': round(float(row['ev']), 0) if pd.notna(row['ev']) else None,
                'ev_usd': round(float(row['ev_usd']), 0) if pd.notna(row['ev_usd']) else None,
                'revenue_usd': round(float(row['revenue_usd']), 0) if pd.notna(row['revenue_usd']) else None,
                'ebitda_usd': round(float(row['ebitda_usd']), 0) if pd.notna(row['ebitda_usd']) else None,
                'fcf_usd': round(float(row['fcf_usd']), 0) if pd.notna(row['fcf_usd']) else None,
                'ev_ebitda': round(float(row['ev_ebitda']), 2) if pd.notna(row['ev_ebitda']) else None,
                'ev_revenue': round(float(row['ev_revenue']), 2) if pd.notna(row['ev_revenue']) else None,
                'fcf_revenue': round(float(row['fcf_revenue']), 4) if pd.notna(row['fcf_revenue']) else None,
                'fcf_ebitda': round(float(row['fcf_ebitda']), 4) if pd.notna(row['fcf_ebitda']) else None,
            })

        return jsonify({
            'success': True,
            'summary': summary,
            'by_industry': by_industry,
            'by_geography': by_geography,
            'companies': companies,
            'metadata': {
                'sector': sector,
                'fiscal_year': fiscal_year,
                'use_ttm_fallback': use_ttm_fallback,
                'min_ttm_quarters': min_ttm_quarters,
                'total_before_filters': total_before,
                'total_after_filters': total_after,
                'filters_applied': filters_applied,
                'filters_config': {
                    'min_ev_usd': min_ev_usd,
                    'max_ev_ebitda': max_ev_ebitda,
                    'require_positive_ebitda': require_positive_ebitda,
                    'min_history_years': min_history_years
                }
            }
        })

    except Exception as e:
        logger.error(f"Erro estudoanloc calculate: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/evolution', methods=['POST'])
def api_estudoanloc_evolution():
    """Calcula evolução de múltiplos ao longo de vários anos."""
    try:
        data = request.get_json()
        sector = data.get('sector', '').strip()
        years = data.get('years', [])
        use_ttm_fallback = data.get('use_ttm_fallback', True)
        min_ttm_quarters = int(data.get('min_ttm_quarters', 4))
        filters = data.get('filters', {})
        min_ev_usd = float(filters.get('min_ev_usd', 100_000_000))
        max_ev_ebitda = float(filters.get('max_ev_ebitda', 60))
        require_positive_ebitda = filters.get('require_positive_ebitda', True)
        selected_industries = data.get('industries', [])

        if not sector or not years:
            return jsonify({'success': False, 'error': 'Setor e anos são obrigatórios'}), 400

        years = [int(y) for y in years]
        conn = get_db()

        import datetime
        current_year = datetime.date.today().year

        industry_filter = ""
        if selected_industries:
            placeholders = ','.join('?' * len(selected_industries))
            industry_filter = f"AND cbd.yahoo_industry IN ({placeholders})"

        def calc_year_stats(subset):
            """Calcula estatísticas para um subconjunto."""
            n = len(subset)
            result = {'n': n, 'n_annual': 0, 'n_ttm': 0}
            if n == 0:
                for metric in ['ev_ebitda', 'ev_revenue', 'fcf_revenue', 'fcf_ebitda']:
                    result[metric] = {'median': None, 'p25': None, 'p75': None, 'n': 0}
                return result

            result['n_annual'] = int(subset['data_source'].isin(['Annual']).sum() + subset['data_source'].str.startswith('Current+FY').sum())
            result['n_ttm'] = int(subset['data_source'].isin(['TTM', 'Current+TTM']).sum())

            for metric in ['ev_ebitda', 'ev_revenue', 'fcf_revenue', 'fcf_ebitda']:
                valid = subset[metric].dropna()
                nv = len(valid)
                if nv == 0:
                    result[metric] = {'median': None, 'p25': None, 'p75': None, 'n': 0}
                else:
                    result[metric] = {
                        'median': round(float(np.median(valid)), 4),
                        'p25': round(float(np.percentile(valid, 25)), 4),
                        'p75': round(float(np.percentile(valid, 75)), 4),
                        'n': nv
                    }
                    if metric.startswith('fcf'):
                        positive = valid[valid > 0]
                        result[metric]['pct_positive'] = round(len(positive) / nv * 100, 1) if nv > 0 else 0
            return result

        evolution = []
        for yr in sorted(years):
            is_current = (yr >= current_year)

            if is_current:
                # --- MODO MÚLTIPLOS ATUAIS ---
                params_ttm_evo = [min_ttm_quarters, sector]
                if selected_industries:
                    params_ttm_evo.extend(selected_industries)

                ttm_evo_sql = f"""
                    WITH latest_q AS (
                        SELECT q.company_basic_data_id AS cid, MAX(q.period_date) AS max_date
                        FROM company_financials_historical q
                        JOIN company_basic_data cbd2 ON q.company_basic_data_id = cbd2.id
                        WHERE q.period_type = 'quarterly'
                          AND q.ttm_quarters_count >= ? AND q.total_revenue_ttm IS NOT NULL
                          AND q.ebitda_ttm IS NOT NULL AND cbd2.yahoo_sector = ?
                        GROUP BY q.company_basic_data_id
                    )
                    SELECT q.company_basic_data_id AS cid,
                           cbd.yahoo_country AS country, dg.sub_group AS region,
                           q.total_revenue_ttm AS revenue, q.ebitda_ttm AS ebitda,
                           q.free_cash_flow_ttm AS fcf, cbd.enterprise_value AS ev,
                           CASE WHEN q.fx_rate_to_usd IS NOT NULL AND q.fx_rate_to_usd > 0
                                THEN cbd.enterprise_value * q.fx_rate_to_usd ELSE NULL END AS ev_usd,
                           'Current+TTM' AS data_source
                    FROM company_financials_historical q
                    JOIN latest_q lq ON q.company_basic_data_id = lq.cid AND q.period_date = lq.max_date
                    JOIN company_basic_data cbd ON q.company_basic_data_id = cbd.id
                    LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                    WHERE q.period_type = 'quarterly' AND cbd.enterprise_value IS NOT NULL
                    {industry_filter}
                """
                df_t = pd.read_sql_query(ttm_evo_sql, conn, params=params_ttm_evo)

                prev_year = yr - 1
                ttm_cids = set(df_t['cid'].tolist()) if not df_t.empty else set()
                params_a_evo = [prev_year, sector]
                if selected_industries:
                    params_a_evo.extend(selected_industries)
                annual_evo_sql = f"""
                    SELECT cfh.company_basic_data_id AS cid,
                           cbd.yahoo_country AS country, dg.sub_group AS region,
                           cfh.total_revenue AS revenue, cfh.normalized_ebitda AS ebitda,
                           cfh.free_cash_flow AS fcf, cbd.enterprise_value AS ev,
                           CASE WHEN cfh.fx_rate_to_usd IS NOT NULL AND cfh.fx_rate_to_usd > 0
                                THEN cbd.enterprise_value * cfh.fx_rate_to_usd ELSE NULL END AS ev_usd,
                           'Current+FY{prev_year}' AS data_source
                    FROM company_financials_historical cfh
                    JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                    LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                    WHERE cfh.period_type = 'annual' AND cfh.fiscal_year = ? AND cbd.yahoo_sector = ?
                      AND cbd.enterprise_value IS NOT NULL
                    {industry_filter}
                """
                df_a = pd.read_sql_query(annual_evo_sql, conn, params=params_a_evo)
                if not df_a.empty and ttm_cids:
                    df_a = df_a[~df_a['cid'].isin(ttm_cids)]

            else:
                # --- MODO HISTÓRICO ---
                params_a = [yr, sector]
                if selected_industries:
                    params_a.extend(selected_industries)
                annual_sql = f"""
                    SELECT cfh.company_basic_data_id AS cid,
                           cbd.yahoo_country AS country, dg.sub_group AS region,
                           cfh.total_revenue AS revenue, cfh.normalized_ebitda AS ebitda,
                           cfh.free_cash_flow AS fcf, cfh.enterprise_value_estimated AS ev,
                           cfh.enterprise_value_usd AS ev_usd, 'Annual' AS data_source
                    FROM company_financials_historical cfh
                    JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
                    LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                    WHERE cfh.period_type = 'annual' AND cfh.fiscal_year = ? AND cbd.yahoo_sector = ?
                    {industry_filter}
                """
                df_a = pd.read_sql_query(annual_sql, conn, params=params_a)

                df_t = pd.DataFrame()
                if use_ttm_fallback:
                    annual_cids = set(df_a['cid'].tolist()) if not df_a.empty else set()
                    params_t = [yr, min_ttm_quarters, sector, yr, min_ttm_quarters]
                    if selected_industries:
                        params_t.extend(selected_industries)
                    ttm_sql = f"""
                        WITH latest_q AS (
                            SELECT q.company_basic_data_id AS cid, MAX(q.period_date) AS max_date
                            FROM company_financials_historical q
                            JOIN company_basic_data cbd2 ON q.company_basic_data_id = cbd2.id
                            WHERE q.period_type = 'quarterly' AND q.fiscal_year = ?
                              AND q.ttm_quarters_count >= ? AND q.total_revenue_ttm IS NOT NULL
                              AND cbd2.yahoo_sector = ?
                            GROUP BY q.company_basic_data_id
                        )
                        SELECT q.company_basic_data_id AS cid,
                               cbd.yahoo_country AS country, dg.sub_group AS region,
                               q.total_revenue_ttm AS revenue, q.ebitda_ttm AS ebitda,
                               q.free_cash_flow_ttm AS fcf, q.enterprise_value_estimated AS ev,
                               q.enterprise_value_usd AS ev_usd, 'TTM' AS data_source
                        FROM company_financials_historical q
                        JOIN latest_q lq ON q.company_basic_data_id = lq.cid AND q.period_date = lq.max_date
                        JOIN company_basic_data cbd ON q.company_basic_data_id = cbd.id
                        LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                        WHERE q.period_type = 'quarterly' AND q.fiscal_year = ? AND q.ttm_quarters_count >= ?
                        {industry_filter}
                    """
                    df_t_raw = pd.read_sql_query(ttm_sql, conn, params=params_t)
                    if not df_t_raw.empty and annual_cids:
                        df_t = df_t_raw[~df_t_raw['cid'].isin(annual_cids)]
                    elif not df_t_raw.empty:
                        df_t = df_t_raw

            frames = [df_a]
            if not df_t.empty:
                frames.append(df_t)
            df = pd.concat(frames, ignore_index=True)

            if df.empty:
                evolution.append({'year': yr, 'global': calc_year_stats(pd.DataFrame()),
                                  'latam': calc_year_stats(pd.DataFrame()),
                                  'brasil': calc_year_stats(pd.DataFrame())})
                continue

            # Apply filters
            df = df[df['revenue'].notna() & df['ebitda'].notna()]
            if min_ev_usd > 0:
                ev_col = 'ev_usd' if df['ev_usd'].notna().sum() > df['ev'].notna().sum() * 0.5 else 'ev'
                df = df[df[ev_col].notna() & (df[ev_col] >= min_ev_usd)]

            # Calc multiples
            df['ev_ebitda'] = np.where((df['ebitda'] > 0) & df['ev'].notna(), df['ev'] / df['ebitda'], np.nan)
            df['ev_revenue'] = np.where((df['revenue'] > 0) & df['ev'].notna(), df['ev'] / df['revenue'], np.nan)
            df['fcf_revenue'] = np.where((df['revenue'] > 0) & df['fcf'].notna(), df['fcf'] / df['revenue'], np.nan)
            df['fcf_ebitda'] = np.where((df['ebitda'] > 0) & df['fcf'].notna(), df['fcf'] / df['ebitda'], np.nan)

            if max_ev_ebitda > 0:
                df.loc[df['ev_ebitda'].notna() & (df['ev_ebitda'] > max_ev_ebitda), 'ev_ebitda'] = np.nan

            df_latam = df[df['region'] == 'Latin America & Caribbean']
            df_brasil = df[df['country'] == 'Brazil']

            evolution.append({
                'year': yr,
                'global': calc_year_stats(df),
                'latam': calc_year_stats(df_latam),
                'brasil': calc_year_stats(df_brasil)
            })

        conn.close()

        return jsonify({'success': True, 'evolution': evolution,
                        'metadata': {'sector': sector, 'years': sorted(years)}})

    except Exception as e:
        logger.error(f"Erro estudoanloc evolution: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/company_detail', methods=['POST'])
def api_estudoanloc_company_detail():
    """Retorna histórico multi-ano de uma empresa para drill-down."""
    try:
        data = request.get_json()
        cid = int(data.get('cid', 0))
        if not cid:
            return jsonify({'success': False, 'error': 'cid é obrigatório'}), 400

        conn = get_db()

        # Dados básicos
        basic = conn.execute("""
            SELECT cbd.ticker, cbd.company_name, cbd.yahoo_sector, cbd.yahoo_industry,
                   cbd.yahoo_country, cbd.currency, cbd.market_cap, cbd.enterprise_value,
                   cbd.yahoo_website, dg.sub_group AS region
            FROM company_basic_data cbd
            LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
            WHERE cbd.id = ?
        """, [cid]).fetchone()

        if not basic:
            conn.close()
            return jsonify({'success': False, 'error': 'Empresa não encontrada'}), 404

        cols = ['ticker', 'company_name', 'sector', 'industry', 'country', 'currency',
                'market_cap', 'enterprise_value', 'website', 'region']
        company_info = dict(zip(cols, basic))

        # Histórico anual
        annual_rows = conn.execute("""
            SELECT cfh.fiscal_year,
                   cfh.total_revenue, cfh.ebitda, cfh.normalized_ebitda, cfh.free_cash_flow,
                   cfh.enterprise_value_estimated AS ev, cfh.enterprise_value_usd AS ev_usd,
                   cfh.total_revenue_usd, cfh.ebitda_usd, cfh.free_cash_flow_usd,
                   cfh.net_income, cfh.total_debt, cfh.cash_and_equivalents AS total_cash
            FROM company_financials_historical cfh
            WHERE cfh.company_basic_data_id = ? AND cfh.period_type = 'annual'
            ORDER BY cfh.fiscal_year
        """, [cid]).fetchall()

        annual_cols = ['fiscal_year', 'revenue', 'ebitda', 'normalized_ebitda', 'fcf',
                       'ev', 'ev_usd', 'revenue_usd', 'ebitda_usd', 'fcf_usd',
                       'net_income', 'total_debt', 'total_cash']
        annual = []
        for row in annual_rows:
            d = dict(zip(annual_cols, row))
            # Calcular múltiplos
            ev = d['ev']
            rev = d['revenue']
            ebitda = d['normalized_ebitda'] or d['ebitda']
            fcf = d['fcf']
            d['ev_ebitda'] = round(ev / ebitda, 2) if ev and ebitda and ebitda > 0 else None
            d['ev_revenue'] = round(ev / rev, 2) if ev and rev and rev > 0 else None
            d['fcf_revenue'] = round(fcf / rev, 4) if fcf is not None and rev and rev > 0 else None
            d['fcf_ebitda'] = round(fcf / ebitda, 4) if fcf is not None and ebitda and ebitda > 0 else None
            annual.append(d)

        # Último TTM
        ttm_row = conn.execute("""
            SELECT q.fiscal_year, q.fiscal_quarter, q.period_date,
                   q.total_revenue_ttm AS revenue, q.ebitda_ttm AS ebitda,
                   q.free_cash_flow_ttm AS fcf,
                   q.enterprise_value_estimated AS ev, q.enterprise_value_usd AS ev_usd,
                   q.total_revenue_usd, q.ebitda_usd, q.free_cash_flow_usd,
                   q.ttm_quarters_count
            FROM company_financials_historical q
            WHERE q.company_basic_data_id = ? AND q.period_type = 'quarterly'
              AND q.ttm_quarters_count >= 4 AND q.total_revenue_ttm IS NOT NULL
            ORDER BY q.period_date DESC LIMIT 1
        """, [cid]).fetchone()

        ttm = None
        if ttm_row:
            ttm_cols = ['fiscal_year', 'fiscal_quarter', 'period_date',
                        'revenue', 'ebitda', 'fcf', 'ev', 'ev_usd',
                        'revenue_usd', 'ebitda_usd', 'fcf_usd', 'ttm_quarters_count']
            ttm = dict(zip(ttm_cols, ttm_row))
            ev = ttm['ev']
            rev = ttm['revenue']
            ebitda = ttm['ebitda']
            fcf = ttm['fcf']
            ttm['ev_ebitda'] = round(ev / ebitda, 2) if ev and ebitda and ebitda > 0 else None
            ttm['ev_revenue'] = round(ev / rev, 2) if ev and rev and rev > 0 else None
            ttm['fcf_revenue'] = round(fcf / rev, 4) if fcf is not None and rev and rev > 0 else None
            ttm['fcf_ebitda'] = round(fcf / ebitda, 4) if fcf is not None and ebitda and ebitda > 0 else None

        conn.close()

        return jsonify({
            'success': True,
            'company': company_info,
            'annual': annual,
            'ttm': ttm
        })

    except Exception as e:
        logger.error(f"Erro estudoanloc company_detail: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/companies_multiyear', methods=['POST'])
def api_estudoanloc_companies_multiyear():
    """Exporta empresas de um setor com múltiplos em todos os anos disponíveis."""
    try:
        data = request.get_json()
        sector = data.get('sector', '').strip()
        if not sector:
            return jsonify({'success': False, 'error': 'Setor é obrigatório'}), 400

        selected_industries = data.get('industries', [])
        min_ev_usd = float(data.get('filters', {}).get('min_ev_usd', 0))

        conn = get_db()

        industry_filter = ""
        params = [sector]
        if selected_industries:
            placeholders = ','.join('?' * len(selected_industries))
            industry_filter = f"AND cbd.yahoo_industry IN ({placeholders})"
            params.extend(selected_industries)

        # Buscar todas as empresas do setor com dados anuais
        sql = f"""
            SELECT cbd.id AS cid, cbd.ticker, cbd.yahoo_industry AS industry,
                   cbd.yahoo_country AS country, cbd.currency,
                   dg.sub_group AS region,
                   cfh.fiscal_year,
                   cfh.total_revenue AS revenue, cfh.normalized_ebitda AS ebitda,
                   cfh.free_cash_flow AS fcf,
                   cfh.enterprise_value_estimated AS ev,
                   cfh.enterprise_value_usd AS ev_usd,
                   cfh.total_revenue_usd AS revenue_usd,
                   cfh.ebitda_usd, cfh.free_cash_flow_usd AS fcf_usd
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
            WHERE cfh.period_type = 'annual'
              AND cbd.yahoo_sector = ?
              AND cfh.total_revenue IS NOT NULL
              {industry_filter}
            ORDER BY cbd.ticker, cfh.fiscal_year
        """
        df = pd.read_sql_query(sql, conn, params=params)

        # Filtro EV mínimo (no último ano disponível por empresa)
        if min_ev_usd > 0 and not df.empty:
            last_ev = df.groupby('cid').last()[['ev_usd', 'ev']].reset_index()
            ev_col = 'ev_usd' if last_ev['ev_usd'].notna().sum() > last_ev['ev'].notna().sum() * 0.5 else 'ev'
            valid_cids = last_ev[last_ev[ev_col].notna() & (last_ev[ev_col] >= min_ev_usd)]['cid']
            df = df[df['cid'].isin(valid_cids)]

        conn.close()

        if df.empty:
            return jsonify({'success': True, 'companies': [], 'years': []})

        # Calcular múltiplos
        df['ev_ebitda'] = np.where((df['ebitda'] > 0) & df['ev'].notna(), df['ev'] / df['ebitda'], np.nan)
        df['ev_revenue'] = np.where((df['revenue'] > 0) & df['ev'].notna(), df['ev'] / df['revenue'], np.nan)
        df['fcf_revenue'] = np.where((df['revenue'] > 0) & df['fcf'].notna(), df['fcf'] / df['revenue'], np.nan)
        df['fcf_ebitda'] = np.where((df['ebitda'] > 0) & df['fcf'].notna(), df['fcf'] / df['ebitda'], np.nan)

        years = sorted(df['fiscal_year'].unique().tolist())

        # Pivotar: cada empresa com dados de cada ano
        companies = {}
        for _, row in df.iterrows():
            cid = int(row['cid'])
            if cid not in companies:
                companies[cid] = {
                    'ticker': row['ticker'], 'industry': row['industry'],
                    'country': row['country'], 'region': row['region'],
                    'currency': row['currency'], 'years': {}
                }
            yr = int(row['fiscal_year'])
            companies[cid]['years'][yr] = {
                'revenue': round(float(row['revenue']), 0) if pd.notna(row['revenue']) else None,
                'ebitda': round(float(row['ebitda']), 0) if pd.notna(row['ebitda']) else None,
                'fcf': round(float(row['fcf']), 0) if pd.notna(row['fcf']) else None,
                'ev': round(float(row['ev']), 0) if pd.notna(row['ev']) else None,
                'ev_ebitda': round(float(row['ev_ebitda']), 2) if pd.notna(row['ev_ebitda']) else None,
                'ev_revenue': round(float(row['ev_revenue']), 2) if pd.notna(row['ev_revenue']) else None,
                'fcf_revenue': round(float(row['fcf_revenue']), 4) if pd.notna(row['fcf_revenue']) else None,
                'fcf_ebitda': round(float(row['fcf_ebitda']), 4) if pd.notna(row['fcf_ebitda']) else None,
            }

        return jsonify({
            'success': True,
            'companies': list(companies.values()),
            'years': years,
            'metadata': {'sector': sector, 'total': len(companies)}
        })

    except Exception as e:
        logger.error(f"Erro estudoanloc companies_multiyear: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/companies_full', methods=['POST'])
def api_estudoanloc_companies_full():
    """Retorna base analítica completa: todas empresas × todos períodos anuais + TTM."""
    try:
        data = request.get_json()
        sector = data.get('sector', '').strip()
        if not sector:
            return jsonify({'success': False, 'error': 'Setor é obrigatório'}), 400

        selected_industries = data.get('industries', [])
        min_ev_usd = float(data.get('filters', {}).get('min_ev_usd', 0))

        conn = get_db()

        industry_filter = ""
        params = [sector]
        if selected_industries:
            placeholders = ','.join('?' * len(selected_industries))
            industry_filter = f"AND cbd.yahoo_industry IN ({placeholders})"
            params.extend(selected_industries)

        # ===== ANNUAL records =====
        sql_annual = f"""
            SELECT cbd.id AS cid, cbd.ticker, cbd.company_name,
                   cbd.yahoo_industry AS industry,
                   cbd.yahoo_country AS country, cbd.currency,
                   dg.sub_group AS region,
                   cfh.fiscal_year, 'Annual' AS period_type_label,
                   cfh.shares_outstanding_current AS shares,
                   cfh.close_price,
                   cfh.total_revenue AS revenue,
                   cfh.normalized_ebitda AS ebitda,
                   cfh.free_cash_flow AS fcf,
                   cfh.operating_cash_flow,
                   cfh.capital_expenditure AS capex,
                   cfh.net_income,
                   cfh.total_debt,
                   cfh.cash_and_equivalents AS cash,
                   cfh.stockholders_equity AS equity,
                   cfh.total_assets,
                   cfh.market_cap_estimated AS market_cap,
                   cfh.enterprise_value_estimated AS ev,
                   cfh.ev_ebitda,
                   cfh.ev_revenue,
                   cfh.fcf_revenue_ratio AS fcf_revenue,
                   cfh.fcf_ebitda_ratio AS fcf_ebitda,
                   cfh.ebitda_margin,
                   cfh.gross_margin,
                   cfh.net_margin,
                   cfh.total_revenue_usd AS revenue_usd,
                   cfh.enterprise_value_usd AS ev_usd,
                   cfh.fx_rate_to_usd
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
            WHERE cfh.period_type = 'annual'
              AND cbd.yahoo_sector = ?
              AND cfh.total_revenue IS NOT NULL
              {industry_filter}
            ORDER BY cbd.ticker, cfh.fiscal_year
        """
        rows_annual = conn.execute(sql_annual, params).fetchall()
        col_names = [desc[0] for desc in conn.execute(sql_annual, params).description] if not rows_annual else None

        # Get column names from cursor
        cursor = conn.execute(sql_annual, params)
        col_names = [desc[0] for desc in cursor.description]
        rows_annual = cursor.fetchall()

        # ===== TTM records (latest per company) =====
        sql_ttm = f"""
            SELECT cbd.id AS cid, cbd.ticker, cbd.company_name,
                   cbd.yahoo_industry AS industry,
                   cbd.yahoo_country AS country, cbd.currency,
                   dg.sub_group AS region,
                   cfh.fiscal_year, 'TTM' AS period_type_label,
                   cfh.shares_outstanding_current AS shares,
                   cfh.close_price,
                   cfh.total_revenue_ttm AS revenue,
                   cfh.ebitda_ttm AS ebitda,
                   cfh.free_cash_flow_ttm AS fcf,
                   NULL AS operating_cash_flow,
                   NULL AS capex,
                   cfh.net_income_ttm AS net_income,
                   cfh.total_debt,
                   cfh.cash_and_equivalents AS cash,
                   cfh.stockholders_equity AS equity,
                   cfh.total_assets,
                   cfh.market_cap_estimated AS market_cap,
                   cfh.enterprise_value_estimated AS ev,
                   CASE WHEN cfh.ebitda_ttm > 0 AND cfh.enterprise_value_estimated IS NOT NULL
                        THEN cfh.enterprise_value_estimated / cfh.ebitda_ttm END AS ev_ebitda,
                   CASE WHEN cfh.total_revenue_ttm > 0 AND cfh.enterprise_value_estimated IS NOT NULL
                        THEN cfh.enterprise_value_estimated / cfh.total_revenue_ttm END AS ev_revenue,
                   CASE WHEN cfh.total_revenue_ttm > 0 AND cfh.free_cash_flow_ttm IS NOT NULL
                        THEN cfh.free_cash_flow_ttm / cfh.total_revenue_ttm END AS fcf_revenue,
                   CASE WHEN cfh.ebitda_ttm > 0 AND cfh.free_cash_flow_ttm IS NOT NULL
                        THEN cfh.free_cash_flow_ttm / cfh.ebitda_ttm END AS fcf_ebitda,
                   CASE WHEN cfh.total_revenue_ttm > 0 AND cfh.ebitda_ttm IS NOT NULL
                        THEN cfh.ebitda_ttm / cfh.total_revenue_ttm END AS ebitda_margin,
                   cfh.gross_margin,
                   CASE WHEN cfh.total_revenue_ttm > 0 AND cfh.net_income_ttm IS NOT NULL
                        THEN cfh.net_income_ttm / cfh.total_revenue_ttm END AS net_margin,
                   cfh.total_revenue_usd AS revenue_usd,
                   cfh.enterprise_value_usd AS ev_usd,
                   cfh.fx_rate_to_usd
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
            WHERE cfh.period_type = 'quarterly'
              AND cbd.yahoo_sector = ?
              AND cfh.ttm_quarters_count >= 4
              AND cfh.total_revenue_ttm IS NOT NULL
              {industry_filter}
              AND cfh.id IN (
                  SELECT MAX(q2.id)
                  FROM company_financials_historical q2
                  WHERE q2.company_basic_data_id = cfh.company_basic_data_id
                    AND q2.period_type = 'quarterly'
                    AND q2.ttm_quarters_count >= 4
                    AND q2.total_revenue_ttm IS NOT NULL
              )
            ORDER BY cbd.ticker
        """
        params_ttm = list(params)  # same params (sector + industries)
        cursor_ttm = conn.execute(sql_ttm, params_ttm)
        rows_ttm = cursor_ttm.fetchall()

        conn.close()

        # Combine all records
        all_records = []
        cid_set = set()

        def row_to_dict(row, cols):
            d = {}
            for i, c in enumerate(cols):
                v = row[i]
                if isinstance(v, float):
                    d[c] = round(v, 4) if c in ('ev_ebitda', 'ev_revenue', 'fcf_revenue',
                        'fcf_ebitda', 'ebitda_margin', 'gross_margin', 'net_margin',
                        'fx_rate_to_usd') else (round(v, 0) if v else v)
                else:
                    d[c] = v
            return d

        for row in rows_annual:
            d = row_to_dict(row, col_names)
            all_records.append(d)
            cid_set.add(d['cid'])

        for row in rows_ttm:
            d = row_to_dict(row, col_names)
            all_records.append(d)
            cid_set.add(d['cid'])

        # Apply EV filter: exclude companies whose max EV (any year) < min_ev_usd
        if min_ev_usd > 0 and all_records:
            max_ev_by_cid = {}
            for r in all_records:
                cid = r['cid']
                ev = r.get('ev_usd') or r.get('ev') or 0
                if ev and (cid not in max_ev_by_cid or ev > max_ev_by_cid[cid]):
                    max_ev_by_cid[cid] = ev
            valid_cids = {cid for cid, ev in max_ev_by_cid.items() if ev >= min_ev_usd}
            all_records = [r for r in all_records if r['cid'] in valid_cids]

        # Get unique years
        years = sorted(set(r['fiscal_year'] for r in all_records if r.get('period_type_label') == 'Annual'))

        return jsonify({
            'success': True,
            'records': all_records,
            'years': years,
            'metadata': {
                'sector': sector,
                'total_records': len(all_records),
                'total_companies': len(set(r['cid'] for r in all_records))
            }
        })

    except Exception as e:
        logger.error(f"Erro estudoanloc companies_full: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/insights', methods=['POST'])
def api_estudoanloc_insights():
    """Gera insights heurísticos (Fase 5.1) com base nos dados calculados."""
    try:
        data = request.get_json() or {}
        summary = data.get('summary', {})
        by_industry = data.get('by_industry', [])
        by_geography = data.get('by_geography', [])
        companies = data.get('companies', [])
        evolution = data.get('evolution', [])
        metadata = data.get('metadata', {})
        sector = metadata.get('sector', 'Setor')
        fiscal_year = metadata.get('fiscal_year', '')

        insights = []
        metrics = ['ev_ebitda', 'ev_revenue', 'fcf_revenue', 'fcf_ebitda']
        metric_labels = {
            'ev_ebitda': 'EV/EBITDA', 'ev_revenue': 'EV/Vendas',
            'fcf_revenue': 'FCF/Vendas', 'fcf_ebitda': 'FCF/EBITDA'
        }

        # Helper
        def safe_get(obj, metric, stat):
            try:
                return obj.get(metric, {}).get(stat)
            except Exception:
                return None

        # --- 1. CONTEXTO SETORIAL ---
        g = summary.get('global', {})
        n = g.get('n', 0)
        ev_med = safe_get(g, 'ev_ebitda', 'median')
        ev_p25 = safe_get(g, 'ev_ebitda', 'p25')
        ev_p75 = safe_get(g, 'ev_ebitda', 'p75')
        if ev_med is not None and n > 0:
            spread = (ev_p75 - ev_p25) if ev_p25 and ev_p75 else None
            spread_txt = f' (dispersão P25-P75: {spread:.1f}x)' if spread else ''
            insights.append({
                'category': 'contexto',
                'icon': 'fa-globe',
                'title': f'Panorama {sector}',
                'text': f'O setor {sector} apresenta EV/EBITDA mediana de {ev_med:.1f}x com {n} empresas analisadas{spread_txt} para {fiscal_year}.',
                'severity': 'info'
            })

        # --- 2. COMPARAÇÃO BRASIL vs GLOBAL ---
        br = summary.get('brasil', {})
        latam = summary.get('latam', {})
        for m in metrics:
            g_val = safe_get(g, m, 'median')
            br_val = safe_get(br, m, 'median')
            br_n = br.get('n', 0)
            if g_val and br_val and g_val != 0 and br_n >= 3:
                discount = ((br_val - g_val) / abs(g_val)) * 100
                label = metric_labels[m]
                if abs(discount) > 15:
                    direction = 'desconto' if discount < 0 else 'prêmio'
                    insights.append({
                        'category': 'geo',
                        'icon': 'fa-flag',
                        'title': f'Brasil vs Global — {label}',
                        'text': f'Brasil opera com {direction} de {abs(discount):.0f}% em {label} ({br_val:.1f}x vs {g_val:.1f}x global). N={br_n} empresas brasileiras.',
                        'severity': 'warning' if abs(discount) > 30 else 'info'
                    })

        # LATAM vs Global
        for m in metrics:
            g_val = safe_get(g, m, 'median')
            la_val = safe_get(latam, m, 'median')
            la_n = latam.get('n', 0)
            if g_val and la_val and g_val != 0 and la_n >= 5:
                discount = ((la_val - g_val) / abs(g_val)) * 100
                label = metric_labels[m]
                if abs(discount) > 15:
                    direction = 'desconto' if discount < 0 else 'prêmio'
                    insights.append({
                        'category': 'geo',
                        'icon': 'fa-globe-americas',
                        'title': f'LATAM vs Global — {label}',
                        'text': f'LATAM opera com {direction} de {abs(discount):.0f}% em {label} ({la_val:.1f}x vs {g_val:.1f}x global). N={la_n} empresas.',
                        'severity': 'info'
                    })

        # --- 3. INDÚSTRIAS OUTLIERS ---
        for m in metrics:
            g_val = safe_get(g, m, 'median')
            g_p25 = safe_get(g, m, 'p25')
            g_p75 = safe_get(g, m, 'p75')
            if g_val is None or g_p25 is None or g_p75 is None:
                continue
            iqr = g_p75 - g_p25
            if iqr <= 0:
                continue
            label = metric_labels[m]
            for ind in by_industry:
                ind_val = safe_get(ind, m, 'median')
                ind_n = ind.get('n', 0)
                if ind_val is None or ind_n < 3:
                    continue
                z_score = (ind_val - g_val) / iqr
                if abs(z_score) > 2:
                    direction = 'acima' if z_score > 0 else 'abaixo'
                    insights.append({
                        'category': 'anomalia',
                        'icon': 'fa-exclamation-triangle',
                        'title': f'{ind.get("label", "?")} — {label} atípico',
                        'text': f'{ind.get("label", "?")} apresenta {label} mediana de {ind_val:.1f}x, significativamente {direction} do setor ({g_val:.1f}x). N={ind_n}.',
                        'severity': 'danger' if abs(z_score) > 3 else 'warning'
                    })

        # --- 4. INDÚSTRIAS COM MAIOR DISPERSÃO ---
        dispersions = []
        for ind in by_industry:
            p25 = safe_get(ind, 'ev_ebitda', 'p25')
            p75 = safe_get(ind, 'ev_ebitda', 'p75')
            if p25 is not None and p75 is not None and ind.get('n', 0) >= 5:
                dispersions.append((ind.get('label', '?'), p75 - p25, ind.get('n', 0)))
        dispersions.sort(key=lambda x: -x[1])
        for name, disp, n in dispersions[:3]:
            if disp > 5:
                insights.append({
                    'category': 'dispersao',
                    'icon': 'fa-arrows-alt-h',
                    'title': f'Alta dispersão em {name}',
                    'text': f'{name} tem amplitude P25-P75 de {disp:.1f}x em EV/EBITDA (N={n}), sugerindo heterogeneidade significativa entre empresas.',
                    'severity': 'info'
                })

        # --- 5. EMPRESAS OUTLIERS (Top 5 acima e abaixo) ---
        if companies:
            ev_vals = [c.get('ev_ebitda') for c in companies if c.get('ev_ebitda') is not None and c.get('ev_ebitda') > 0]
            if len(ev_vals) >= 10:
                import statistics
                med = statistics.median(ev_vals)
                stdev = statistics.stdev(ev_vals) if len(ev_vals) > 1 else 0
                if stdev > 0:
                    outliers_high = [c for c in companies if c.get('ev_ebitda') is not None and (c['ev_ebitda'] - med) / stdev > 2]
                    outliers_high.sort(key=lambda c: -c.get('ev_ebitda', 0))
                    for c in outliers_high[:5]:
                        insights.append({
                            'category': 'empresa',
                            'icon': 'fa-building',
                            'title': f'{c.get("ticker","?")} — EV/EBITDA {c["ev_ebitda"]:.1f}x',
                            'text': f'{c.get("company_name",c.get("ticker","?"))} ({c.get("country","?")}) opera com EV/EBITDA de {c["ev_ebitda"]:.1f}x vs mediana do setor {med:.1f}x ({((c["ev_ebitda"]-med)/med*100):.0f}% acima).',
                            'severity': 'warning'
                        })

        # --- 6. EVOLUÇÃO / TENDÊNCIAS ---
        if len(evolution) >= 3:
            for m in metrics:
                label = metric_labels[m]
                vals = []
                for evo in sorted(evolution, key=lambda e: e.get('year', 0)):
                    v = safe_get(evo.get('global', {}), m, 'median')
                    if v is not None:
                        vals.append((evo['year'], v))
                if len(vals) >= 3:
                    first_yr, first_val = vals[0]
                    last_yr, last_val = vals[-1]
                    if first_val and first_val != 0:
                        change_pct = ((last_val - first_val) / abs(first_val)) * 100
                        if abs(change_pct) > 15:
                            trend = 'crescente' if change_pct > 0 else 'decrescente'
                            insights.append({
                                'category': 'evolucao',
                                'icon': 'fa-chart-line',
                                'title': f'Tendência {trend} em {label}',
                                'text': f'{label} global variou de {first_val:.1f}x ({first_yr}) para {last_val:.1f}x ({last_yr}), uma mudança de {change_pct:+.0f}%.',
                                'severity': 'warning' if abs(change_pct) > 30 else 'info'
                            })
                    # Detectar reversão recente
                    if len(vals) >= 4:
                        prev_val = vals[-2][1]
                        ante_val = vals[-3][1]
                        if prev_val and ante_val and prev_val != 0:
                            prev_trend = prev_val - ante_val
                            curr_trend = last_val - prev_val
                            if prev_trend * curr_trend < 0 and abs(curr_trend) > 0.3:
                                direction = 'reverteu para alta' if curr_trend > 0 else 'reverteu para queda'
                                insights.append({
                                    'category': 'evolucao',
                                    'icon': 'fa-sync-alt',
                                    'title': f'Reversão em {label}',
                                    'text': f'{label} global {direction} em {last_yr}: {prev_val:.1f}x → {last_val:.1f}x (mudança de {curr_trend:+.1f}x vs {prev_trend:+.1f}x anterior).',
                                    'severity': 'warning'
                                })

        # --- 7. REGIÕES COM DESTAQUE ---
        for geo in by_geography:
            geo_val = safe_get(geo, 'ev_ebitda', 'median')
            geo_n = geo.get('n', 0)
            if geo_val is not None and ev_med is not None and ev_med != 0 and geo_n >= 5:
                diff_pct = ((geo_val - ev_med) / abs(ev_med)) * 100
                if abs(diff_pct) > 25:
                    direction = 'prêmio' if diff_pct > 0 else 'desconto'
                    insights.append({
                        'category': 'geo',
                        'icon': 'fa-map-marker-alt',
                        'title': f'{geo.get("label","?")} — EV/EBITDA',
                        'text': f'{geo.get("label","?")} opera com {direction} de {abs(diff_pct):.0f}% em EV/EBITDA ({geo_val:.1f}x vs {ev_med:.1f}x global). N={geo_n}.',
                        'severity': 'info'
                    })

        # Ordenar por severidade
        severity_order = {'danger': 0, 'warning': 1, 'info': 2}
        insights.sort(key=lambda x: severity_order.get(x.get('severity', 'info'), 9))

        return jsonify({
            'success': True,
            'insights': insights,
            'total': len(insights),
            'metadata': {'sector': sector, 'fiscal_year': fiscal_year, 'mode': 'heuristic'}
        })
    except Exception as e:
        logger.error(f"Erro estudoanloc insights: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================
# ESTUDO ANLOC — Página Insights dedicada com LLM
# ============================================================

@app.route('/estudoanloc/insights')
def estudoanloc_insights_page():
    """Página dedicada de Insights com análise LLM."""
    return render_template('estudoanloc_insights.html')


@app.route('/api/estudoanloc/insights_llm', methods=['POST'])
def api_estudoanloc_insights_llm():
    """Gera insights usando LLM (Gemini) + heurísticas sobre dados do setor."""
    try:
        data = request.get_json() or {}
        sector = data.get('sector', '')
        fiscal_year = data.get('fiscal_year', 2025)
        industries = data.get('industries', [])
        region_filter = data.get('region', '')
        country_filter = data.get('country', '')
        filters = data.get('filters', {})
        min_ev_usd = filters.get('min_ev_usd', 100_000_000)
        max_ev_ebitda = filters.get('max_ev_ebitda', 60)

        if not sector:
            return jsonify({'success': False, 'error': 'Setor é obrigatório'}), 400

        conn = get_db()
        current_year = datetime.now().year
        is_current = (fiscal_year >= current_year)

        # --- Buscar dados ---
        if is_current:
            query = """
                SELECT cbd.id as company_id, cbd.ticker, cbd.company_name, cbd.yahoo_sector,
                       cbd.yahoo_industry, cbd.yahoo_country as country,
                       COALESCE(dg.sub_group, 'Other') as region,
                       cbd.enterprise_value as ev,
                       cfh.total_revenue as revenue, cfh.normalized_ebitda as ebitda,
                       cfh.free_cash_flow as fcf,
                       cfh.net_income, cfh.total_debt, cfh.cash_and_equivalents as cash,
                       cfh.fiscal_year, cfh.period_type
                FROM company_basic_data cbd
                JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE cbd.yahoo_sector = ?
                  AND cbd.enterprise_value IS NOT NULL
                  AND cbd.enterprise_value > 0
                  AND cfh.total_revenue IS NOT NULL
                  AND cfh.total_revenue > 0
                  AND (cfh.period_type = 'annual' OR (cfh.period_type = 'annual' AND cfh.fiscal_year = ?))
            """
            params = [sector, fiscal_year - 1]
        else:
            query = """
                SELECT cbd.id as company_id, cbd.ticker, cbd.company_name, cbd.yahoo_sector,
                       cbd.yahoo_industry, cbd.yahoo_country as country,
                       COALESCE(dg.sub_group, 'Other') as region,
                       cfh.enterprise_value_estimated as ev,
                       cfh.total_revenue as revenue, cfh.normalized_ebitda as ebitda,
                       cfh.free_cash_flow as fcf,
                       cfh.net_income, cfh.total_debt, cfh.cash_and_equivalents as cash,
                       cfh.fiscal_year, cfh.period_type
                FROM company_basic_data cbd
                JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE cbd.yahoo_sector = ?
                  AND cfh.fiscal_year = ?
                  AND cfh.period_type = 'annual'
                  AND cfh.enterprise_value_estimated IS NOT NULL
                  AND cfh.enterprise_value_estimated > 0
                  AND cfh.total_revenue IS NOT NULL
                  AND cfh.total_revenue > 0
            """
            params = [sector, fiscal_year]

        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return jsonify({'success': False, 'error': f'Sem dados para {sector} em {fiscal_year}'}), 404

        # Priorizar TTM sobre Annual por empresa
        if is_current:
            type_priority = {'quarterly': 0, 'annual': 1}
            df['_prio'] = df['period_type'].map(type_priority).fillna(2)
            df = df.sort_values('_prio').drop_duplicates(subset=['company_id'], keep='first').drop(columns=['_prio'])

        # Filtros
        if min_ev_usd:
            df = df[df['ev'] >= min_ev_usd]
        if industries:
            df = df[df['yahoo_industry'].isin(industries)]
        if region_filter:
            if region_filter == 'Brazil':
                df = df[df['country'] == 'Brazil']
            elif region_filter == 'LATAM':
                latam = ['Brazil','Mexico','Argentina','Chile','Colombia','Peru','Uruguay','Paraguay',
                         'Bolivia','Ecuador','Venezuela','Costa Rica','Panama','Guatemala','Honduras',
                         'El Salvador','Nicaragua','Dominican Republic','Puerto Rico','Cuba']
                df = df[df['country'].isin(latam)]
            else:
                df = df[df['region'] == region_filter]
        if country_filter:
            df = df[df['country'] == country_filter]

        # Calcular múltiplos
        df['ev_ebitda'] = np.where((df['ebitda'] > 0) & (df['ev'] > 0), df['ev'] / df['ebitda'], np.nan)
        df['ev_revenue'] = np.where(df['revenue'] > 0, df['ev'] / df['revenue'], np.nan)
        df['fcf_revenue'] = np.where(df['revenue'] > 0, df['fcf'] / df['revenue'], np.nan)
        df['fcf_ebitda'] = np.where(df['ebitda'] > 0, df['fcf'] / df['ebitda'], np.nan)
        df['ebitda_margin'] = np.where(df['revenue'] > 0, df['ebitda'] / df['revenue'] * 100, np.nan)

        if max_ev_ebitda:
            df.loc[df['ev_ebitda'] > max_ev_ebitda, 'ev_ebitda'] = np.nan

        total_companies = len(df)
        if total_companies == 0:
            return jsonify({'success': False, 'error': 'Nenhuma empresa após filtros'}), 404

        # --- Estatísticas agregadas ---
        def calc_stats(subset, label=''):
            result = {'label': label, 'n': len(subset)}
            for m in ['ev_ebitda', 'ev_revenue', 'fcf_revenue', 'fcf_ebitda']:
                vals = subset[m].dropna()
                if len(vals) >= 1:
                    result[m] = {
                        'median': float(np.median(vals)), 'mean': float(np.mean(vals)),
                        'p25': float(np.percentile(vals, 25)), 'p75': float(np.percentile(vals, 75)),
                        'min': float(vals.min()), 'max': float(vals.max()), 'n': len(vals)
                    }
                else:
                    result[m] = None
            return result

        stats_global = calc_stats(df, 'Global')

        # Por indústria
        by_industry = []
        for ind, grp in df.groupby('yahoo_industry'):
            if len(grp) >= 2:
                by_industry.append(calc_stats(grp, ind))
        by_industry.sort(key=lambda x: x['n'], reverse=True)

        # Por região
        by_region = []
        for reg, grp in df.groupby('region'):
            if len(grp) >= 2:
                by_region.append(calc_stats(grp, reg))
        by_region.sort(key=lambda x: x['n'], reverse=True)

        # Por país (top 15)
        by_country = []
        for ctry, grp in df.groupby('country'):
            if len(grp) >= 2:
                by_country.append(calc_stats(grp, ctry))
        by_country.sort(key=lambda x: x['n'], reverse=True)
        by_country = by_country[:15]

        # Top/Bottom empresas
        df_ev = df.dropna(subset=['ev_ebitda']).sort_values('ev_ebitda', ascending=False)
        top_companies = df_ev.head(10)[['ticker', 'company_name', 'yahoo_industry', 'country',
                                         'ev_ebitda', 'ev_revenue', 'ebitda_margin', 'ev']].to_dict('records')
        bottom_companies = df_ev.tail(10)[['ticker', 'company_name', 'yahoo_industry', 'country',
                                            'ev_ebitda', 'ev_revenue', 'ebitda_margin', 'ev']].to_dict('records')

        # Dados para gráficos
        chart_data = {
            'industry_chart': [{'label': x['label'], 'median': x.get('ev_ebitda', {}).get('median') if x.get('ev_ebitda') else None,
                                'p25': x.get('ev_ebitda', {}).get('p25') if x.get('ev_ebitda') else None,
                                'p75': x.get('ev_ebitda', {}).get('p75') if x.get('ev_ebitda') else None,
                                'n': x['n']} for x in by_industry[:15]],
            'region_chart': [{'label': x['label'], 'median': x.get('ev_ebitda', {}).get('median') if x.get('ev_ebitda') else None,
                              'n': x['n']} for x in by_region],
            'country_chart': [{'label': x['label'], 'median': x.get('ev_ebitda', {}).get('median') if x.get('ev_ebitda') else None,
                               'n': x['n']} for x in by_country],
            'distribution': df['ev_ebitda'].dropna().tolist()
        }

        # --- Gerar Insights Heurísticos ---
        heuristic_insights = _generate_heuristic_insights(
            stats_global, by_industry, by_region, by_country,
            top_companies, bottom_companies, sector, fiscal_year
        )

        # --- Tentar LLM (multi-provider) ---
        llm_analysis = None
        llm_provider = data.get('llm_provider', 'gemini')
        user_api_key = data.get('api_key', '')
        env_keys = {
            'gemini': os.environ.get('GEMINI_API_KEY', ''),
            'openai': os.environ.get('OPENAI_API_KEY', ''),
            'anthropic': os.environ.get('ANTHROPIC_API_KEY', '')
        }
        active_key = user_api_key or env_keys.get(llm_provider, '')
        if active_key:
            try:
                llm_analysis = _generate_llm_analysis(
                    active_key, sector, fiscal_year, stats_global,
                    by_industry, by_region, by_country,
                    top_companies, region_filter, country_filter,
                    provider=llm_provider
                )
            except Exception as e:
                logger.warning(f"LLM analysis failed ({llm_provider}): {e}")
                llm_provider = None
        else:
            llm_provider = None

        return jsonify({
            'success': True,
            'stats': stats_global,
            'by_industry': by_industry,
            'by_region': by_region,
            'by_country': by_country,
            'top_companies': top_companies,
            'bottom_companies': bottom_companies,
            'chart_data': chart_data,
            'heuristic_insights': heuristic_insights,
            'llm_analysis': llm_analysis,
            'llm_provider': llm_provider,
            'metadata': {
                'sector': sector,
                'fiscal_year': fiscal_year,
                'total_companies': total_companies,
                'region': region_filter or 'Global',
                'country': country_filter or 'Todos',
                'industries_filter': industries
            }
        })

    except Exception as e:
        logger.error(f"Erro estudoanloc insights_llm: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/evolution_data', methods=['POST'])
def api_estudoanloc_evolution_data():
    """Retorna estatísticas de múltiplos por ano para análise de evolução temporal."""
    try:
        data = request.get_json() or {}
        sector = data.get('sector', '')
        industry = data.get('industry', '')
        region_filter = data.get('region', '')
        country_filter = data.get('country', '')
        min_ev = data.get('min_ev_usd', 100_000_000)
        max_ev_ebitda = data.get('max_ev_ebitda', 60)
        year_start = data.get('year_start', 2021)
        year_end = data.get('year_end', 2025)

        if not sector:
            return jsonify({'success': False, 'error': 'Setor é obrigatório'}), 400

        conn = get_db()
        results_by_year = []

        for yr in range(year_start, year_end + 1):
            query = """
                SELECT cbd.id as company_id, cbd.ticker, cbd.yahoo_industry,
                       cbd.yahoo_country as country,
                       COALESCE(dg.sub_group, 'Other') as region,
                       cfh.enterprise_value_estimated as ev,
                       cfh.total_revenue as revenue, cfh.normalized_ebitda as ebitda,
                       cfh.free_cash_flow as fcf
                FROM company_basic_data cbd
                JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id
                LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
                WHERE cbd.yahoo_sector = ?
                  AND cfh.fiscal_year = ?
                  AND cfh.period_type = 'annual'
                  AND cfh.enterprise_value_estimated IS NOT NULL
                  AND cfh.enterprise_value_estimated > 0
                  AND cfh.total_revenue IS NOT NULL
                  AND cfh.total_revenue > 0
            """
            params = [sector, yr]
            df = pd.read_sql_query(query, conn, params=params)

            if df.empty:
                results_by_year.append({'year': yr, 'n': 0})
                continue

            # Aplicar filtros
            if min_ev:
                df = df[df['ev'] >= min_ev]
            if industry:
                df = df[df['yahoo_industry'] == industry]
            if region_filter:
                if region_filter == 'Brazil':
                    df = df[df['country'] == 'Brazil']
                elif region_filter == 'LATAM':
                    latam = ['Brazil','Mexico','Argentina','Chile','Colombia','Peru','Uruguay','Paraguay',
                             'Bolivia','Ecuador','Venezuela','Costa Rica','Panama','Guatemala','Honduras',
                             'El Salvador','Nicaragua','Dominican Republic','Puerto Rico','Cuba']
                    df = df[df['country'].isin(latam)]
                else:
                    df = df[df['region'] == region_filter]
            if country_filter:
                df = df[df['country'] == country_filter]

            if df.empty:
                results_by_year.append({'year': yr, 'n': 0})
                continue

            # Calcular múltiplos
            df['ev_ebitda'] = np.where((df['ebitda'] > 0) & (df['ev'] > 0), df['ev'] / df['ebitda'], np.nan)
            df['ev_revenue'] = np.where(df['revenue'] > 0, df['ev'] / df['revenue'], np.nan)
            df['fcf_revenue'] = np.where(df['revenue'] > 0, df['fcf'] / df['revenue'], np.nan)
            df['fcf_ebitda'] = np.where(df['ebitda'] > 0, df['fcf'] / df['ebitda'], np.nan)

            if max_ev_ebitda:
                df.loc[df['ev_ebitda'] > max_ev_ebitda, 'ev_ebitda'] = np.nan

            yr_data = {'year': yr, 'n': len(df)}
            for m in ['ev_ebitda', 'ev_revenue', 'fcf_revenue', 'fcf_ebitda']:
                vals = df[m].dropna()
                if len(vals) >= 2:
                    yr_data[m] = {
                        'median': round(float(np.median(vals)), 2),
                        'mean': round(float(np.mean(vals)), 2),
                        'p25': round(float(np.percentile(vals, 25)), 2),
                        'p75': round(float(np.percentile(vals, 75)), 2),
                        'n': int(len(vals))
                    }
                else:
                    yr_data[m] = None
            results_by_year.append(yr_data)

        # Calcular variações ano-a-ano
        trends = {}
        for m in ['ev_ebitda', 'ev_revenue', 'fcf_revenue', 'fcf_ebitda']:
            vals = [(r['year'], r[m]['median']) for r in results_by_year if r.get(m) and r[m].get('median')]
            if len(vals) >= 2:
                first_val = vals[0][1]
                last_val = vals[-1][1]
                change_pct = ((last_val - first_val) / abs(first_val)) * 100 if first_val else 0
                trends[m] = {
                    'start_year': vals[0][0], 'end_year': vals[-1][0],
                    'start_val': first_val, 'end_val': last_val,
                    'change_pct': round(change_pct, 1),
                    'direction': 'up' if change_pct > 5 else ('down' if change_pct < -5 else 'stable')
                }

        return jsonify({
            'success': True,
            'evolution': results_by_year,
            'trends': trends,
            'metadata': {'sector': sector, 'industry': industry or 'Todas',
                         'year_start': year_start, 'year_end': year_end}
        })
    except Exception as e:
        logger.error(f"Erro evolution_data: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/chat', methods=['POST'])
def api_estudoanloc_chat():
    """Endpoint de chat conversacional com LLM para análise de dados."""
    try:
        data = request.get_json() or {}
        messages = data.get('messages', [])
        provider = data.get('provider', 'gemini')
        api_key = data.get('api_key', '')
        data_context = data.get('data_context', '')
        custom_instructions = data.get('custom_instructions', '')

        if not messages:
            return jsonify({'success': False, 'error': 'Nenhuma mensagem enviada'}), 400

        # Usar env var como fallback
        if not api_key:
            env_keys = {
                'gemini': os.environ.get('GEMINI_API_KEY', ''),
                'openai': os.environ.get('OPENAI_API_KEY', ''),
                'anthropic': os.environ.get('ANTHROPIC_API_KEY', '')
            }
            api_key = env_keys.get(provider, '')

        if not api_key:
            return jsonify({'success': False, 'error': f'API Key não configurada para {provider}. Configure nas definições ou via variável de ambiente.'}), 400

        system_prompt = """Você é um analista financeiro sênior especializado em valuation por múltiplos de mercado.
Responda sempre em português brasileiro, de forma objetiva e analítica.
Baseie-se EXCLUSIVAMENTE nos dados fornecidos no contexto. Não invente números.
Quando relevante, cite os dados específicos (medianas, percentis, empresas) para fundamentar suas análises.
Formate suas respostas com parágrafos curtos para legibilidade."""

        if custom_instructions:
            system_prompt += f"\n\nINSTRUÇÕES ADICIONAIS DO USUÁRIO:\n{custom_instructions}"

        if data_context:
            system_prompt += f"\n\nCONTEXTO DE DADOS (use como base para suas respostas):\n{data_context}"

        response_text = _call_llm_chat(provider, api_key, messages, system_prompt)
        return jsonify({'success': True, 'message': response_text})

    except Exception as e:
        logger.error(f"Erro chat LLM: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


def _call_llm_chat(provider, api_key, messages, system_prompt):
    """Chama a LLM escolhida (Gemini, OpenAI ou Anthropic) no modo chat."""
    if provider == 'gemini':
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash',
                                       system_instruction=system_prompt)
        # Converter messages para formato Gemini
        gemini_history = []
        for msg in messages[:-1]:
            role = 'user' if msg['role'] == 'user' else 'model'
            gemini_history.append({'role': role, 'parts': [msg['content']]})
        chat = model.start_chat(history=gemini_history)
        response = chat.send_message(messages[-1]['content'])
        return response.text

    elif provider == 'openai':
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        oai_messages = [{'role': 'system', 'content': system_prompt}]
        for msg in messages:
            oai_messages.append({'role': msg['role'], 'content': msg['content']})
        response = client.chat.completions.create(
            model='gpt-4o-mini', messages=oai_messages, temperature=0.3)
        return response.choices[0].message.content

    elif provider == 'anthropic':
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        anth_messages = [{'role': msg['role'], 'content': msg['content']} for msg in messages]
        response = client.messages.create(
            model='claude-sonnet-4-20250514', max_tokens=4096,
            system=system_prompt, messages=anth_messages)
        return response.content[0].text

    else:
        raise ValueError(f"Provedor LLM não suportado: {provider}")


def _generate_heuristic_insights(stats, by_industry, by_region, by_country,
                                  top_companies, bottom_companies, sector, fiscal_year):
    """Gera insights baseados em heurísticas (sem API externa)."""
    insights = []
    metric_labels = {'ev_ebitda': 'EV/EBITDA', 'ev_revenue': 'EV/Vendas',
                     'fcf_revenue': 'FCF/Vendas', 'fcf_ebitda': 'FCF/EBITDA'}

    def sg(obj, m, s):
        try: return obj.get(m, {}).get(s)
        except: return None

    ev_med = sg(stats, 'ev_ebitda', 'median')
    n = stats.get('n', 0)

    # Panorama
    if ev_med and n > 0:
        p25 = sg(stats, 'ev_ebitda', 'p25')
        p75 = sg(stats, 'ev_ebitda', 'p75')
        spread = f' (dispersão P25-P75: {p75-p25:.1f}x)' if p25 and p75 else ''
        evr = sg(stats, 'ev_revenue', 'median')
        evr_txt = f' EV/Vendas mediana {evr:.1f}x.' if evr else ''
        insights.append({'category': 'contexto', 'icon': 'fa-globe', 'severity': 'info',
            'title': f'Panorama {sector}',
            'text': f'{sector} apresenta EV/EBITDA mediana de {ev_med:.1f}x com {n} empresas{spread}.{evr_txt}'})

    # Indústrias outliers
    if ev_med and by_industry:
        p25 = sg(stats, 'ev_ebitda', 'p25')
        p75 = sg(stats, 'ev_ebitda', 'p75')
        iqr = (p75 - p25) if p25 and p75 and p75 > p25 else None
        if iqr:
            for ind in by_industry:
                iv = sg(ind, 'ev_ebitda', 'median')
                if iv and ind.get('n', 0) >= 3:
                    z = (iv - ev_med) / iqr
                    if abs(z) > 2:
                        d = 'acima' if z > 0 else 'abaixo'
                        sev = 'danger' if abs(z) > 3 else 'warning'
                        insights.append({'category': 'anomalia', 'icon': 'fa-exclamation-triangle',
                            'severity': sev, 'title': f'{ind["label"]} — múltiplo atípico',
                            'text': f'{ind["label"]} tem EV/EBITDA de {iv:.1f}x, significativamente {d} da mediana {ev_med:.1f}x do setor. (N={ind["n"]})'})

    # Regiões com destaque
    for reg in by_region:
        rv = sg(reg, 'ev_ebitda', 'median')
        if rv and ev_med and ev_med != 0 and reg.get('n', 0) >= 3:
            diff = ((rv - ev_med) / abs(ev_med)) * 100
            if abs(diff) > 20:
                d = 'prêmio' if diff > 0 else 'desconto'
                insights.append({'category': 'geo', 'icon': 'fa-map-marker-alt', 'severity': 'info',
                    'title': f'{reg["label"]} — {d} de {abs(diff):.0f}%',
                    'text': f'{reg["label"]} opera EV/EBITDA de {rv:.1f}x ({d} de {abs(diff):.0f}% vs {ev_med:.1f}x global). N={reg["n"]}.'})

    # Países com destaque
    for ctry in by_country[:10]:
        cv = sg(ctry, 'ev_ebitda', 'median')
        if cv and ev_med and ev_med != 0 and ctry.get('n', 0) >= 3:
            diff = ((cv - ev_med) / abs(ev_med)) * 100
            if abs(diff) > 25:
                d = 'prêmio' if diff > 0 else 'desconto'
                insights.append({'category': 'geo', 'icon': 'fa-flag', 'severity': 'warning' if abs(diff) > 40 else 'info',
                    'title': f'{ctry["label"]} — {d} de {abs(diff):.0f}%',
                    'text': f'{ctry["label"]} opera EV/EBITDA de {cv:.1f}x ({d} vs global {ev_med:.1f}x). N={ctry["n"]}.'})

    # Top empresas
    for c in top_companies[:5]:
        if c.get('ev_ebitda') and ev_med and ev_med != 0:
            pct = ((c['ev_ebitda'] - ev_med) / abs(ev_med)) * 100
            if pct > 50:
                insights.append({'category': 'empresa', 'icon': 'fa-building', 'severity': 'warning',
                    'title': f'{c["ticker"]} — EV/EBITDA {c["ev_ebitda"]:.1f}x',
                    'text': f'{c.get("company_name", c["ticker"])} ({c.get("country","?")}) opera {pct:.0f}% acima da mediana setorial. Indústria: {c.get("yahoo_industry","?")}.'})

    # Dispersão por indústria
    dispersions = [(ind['label'], sg(ind, 'ev_ebitda', 'p75') - sg(ind, 'ev_ebitda', 'p25'), ind['n'])
                   for ind in by_industry
                   if sg(ind, 'ev_ebitda', 'p25') is not None and sg(ind, 'ev_ebitda', 'p75') is not None and ind.get('n', 0) >= 5]
    dispersions.sort(key=lambda x: -x[1])
    for name, disp, nn in dispersions[:3]:
        if disp > 5:
            insights.append({'category': 'dispersao', 'icon': 'fa-arrows-alt-h', 'severity': 'info',
                'title': f'Alta dispersão em {name}',
                'text': f'{name} tem amplitude P25-P75 de {disp:.1f}x (N={nn}), indicando heterogeneidade nas empresas.'})

    severity_order = {'danger': 0, 'warning': 1, 'info': 2}
    insights.sort(key=lambda x: severity_order.get(x.get('severity', 'info'), 9))
    return insights


def _generate_llm_analysis(api_key, sector, fiscal_year, stats, by_industry,
                            by_region, by_country, top_companies, region, country,
                            provider='gemini'):
    """Gera análise estruturada usando LLM (Gemini, OpenAI ou Anthropic)."""

    # Preparar contexto compacto
    def fmt_stat(obj, metric):
        s = obj.get(metric)
        if not s: return 'N/D'
        return f"med={s.get('median','?'):.1f}x p25={s.get('p25','?'):.1f}x p75={s.get('p75','?'):.1f}x (n={s.get('n','?')})"

    context_lines = [
        f"Setor: {sector} | Ano: {fiscal_year} | Empresas: {stats.get('n',0)}",
        f"{'Região: ' + region if region else 'Global'}{' | País: ' + country if country else ''}",
        f"\nMÚLTIPLOS GLOBAIS:",
        f"  EV/EBITDA: {fmt_stat(stats, 'ev_ebitda')}",
        f"  EV/Vendas: {fmt_stat(stats, 'ev_revenue')}",
        f"  FCF/Vendas: {fmt_stat(stats, 'fcf_revenue')}",
        f"  FCF/EBITDA: {fmt_stat(stats, 'fcf_ebitda')}",
        f"\nPOR INDÚSTRIA (top 10):"
    ]
    for ind in by_industry[:10]:
        ev = ind.get('ev_ebitda')
        if ev:
            context_lines.append(f"  {ind['label']}: EV/EBITDA med={ev.get('median',0):.1f}x (n={ind['n']})")

    context_lines.append(f"\nPOR REGIÃO:")
    for reg in by_region[:8]:
        ev = reg.get('ev_ebitda')
        if ev:
            context_lines.append(f"  {reg['label']}: EV/EBITDA med={ev.get('median',0):.1f}x (n={reg['n']})")

    if by_country:
        context_lines.append(f"\nPOR PAÍS (top 10):")
        for ctry in by_country[:10]:
            ev = ctry.get('ev_ebitda')
            if ev:
                context_lines.append(f"  {ctry['label']}: EV/EBITDA med={ev.get('median',0):.1f}x (n={ctry['n']})")

    context_lines.append(f"\nTOP EMPRESAS (maior EV/EBITDA):")
    for c in top_companies[:8]:
        context_lines.append(f"  {c.get('ticker','?')} ({c.get('country','?')}): EV/EBITDA={c.get('ev_ebitda',0):.1f}x")

    context = "\n".join(context_lines)

    user_prompt = f"""Analise os dados abaixo do setor {sector} e gere um relatório conciso em português brasileiro.

DADOS:
{context}

Gere um JSON com a seguinte estrutura (responda SOMENTE o JSON, sem markdown):
{{
  "resumo_executivo": "2-3 parágrafos com panorama geral do setor, medianas, dispersão e contexto de mercado",
  "destaques": [
    {{"titulo": "string", "texto": "string", "tipo": "positivo|negativo|neutro"}}
  ],
  "analise_industrias": "1-2 parágrafos comparando indústrias, outliers e padrões",
  "analise_geografica": "1-2 parágrafos sobre diferenças regionais/por país",
  "empresas_destaque": "1 parágrafo sobre as empresas outliers e possíveis explicações",
  "riscos_oportunidades": [
    {{"tipo": "risco|oportunidade", "texto": "string"}}
  ],
  "conclusao": "1 parágrafo com conclusão e perspectivas"
}}

IMPORTANTE: Baseie-se EXCLUSIVAMENTE nos dados fornecidos. Não invente números. Seja objetivo e analítico."""

    system_prompt = "Você é um analista financeiro sênior especializado em valuation por múltiplos. Responda SOMENTE com JSON válido, sem markdown."

    messages = [{'role': 'user', 'content': user_prompt}]
    text = _call_llm_chat(provider, api_key, messages, system_prompt)

    # Parse JSON da resposta
    text = text.strip()
    if text.startswith('```'):
        text = text.split('\n', 1)[1] if '\n' in text else text[3:]
        if text.endswith('```'):
            text = text[:-3]
        if text.startswith('json'):
            text = text[4:]
        text = text.strip()

    import json as json_mod
    analysis = json_mod.loads(text)
    return analysis


# ==============================================================================
# ESTUDO ANLOC - RELAT&Oacute;RIO PERI&Oacute;DICO (Quarterly Market Study)
# ==============================================================================

LATAM_COUNTRIES = [
    'Brazil', 'Mexico', 'Argentina', 'Chile', 'Colombia', 'Peru', 'Uruguay',
    'Paraguay', 'Bolivia', 'Ecuador', 'Venezuela', 'Costa Rica', 'Panama',
    'Guatemala', 'Honduras', 'El Salvador', 'Nicaragua', 'Dominican Republic',
    'Puerto Rico', 'Cuba'
]


def _report_calc_sector_stats(conn, sector, fiscal_year, min_ev=100_000_000, max_ev_ebitda=60):
    """Calcula estatísticas de múltiplos para um setor, segmentado por geografia."""
    current_year = datetime.now().year
    is_current = (fiscal_year >= current_year)

    if is_current:
        query = """
            SELECT cbd.id as company_id, cbd.ticker, cbd.company_name,
                   cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country as country,
                   COALESCE(dg.sub_group, 'Other') as region,
                   cbd.enterprise_value as ev,
                   cfh.total_revenue as revenue, cfh.normalized_ebitda as ebitda,
                   cfh.free_cash_flow as fcf, cfh.period_type
            FROM company_basic_data cbd
            JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id
            LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
            WHERE cbd.yahoo_sector = ?
              AND cbd.enterprise_value IS NOT NULL AND cbd.enterprise_value > 0
              AND cfh.total_revenue IS NOT NULL AND cfh.total_revenue > 0
              AND (cfh.period_type = 'annual' OR (cfh.period_type = 'annual' AND cfh.fiscal_year = ?))
        """
        params = [sector, fiscal_year - 1]
    else:
        query = """
            SELECT cbd.id as company_id, cbd.ticker, cbd.company_name,
                   cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country as country,
                   COALESCE(dg.sub_group, 'Other') as region,
                   cfh.enterprise_value_estimated as ev,
                   cfh.total_revenue as revenue, cfh.normalized_ebitda as ebitda,
                   cfh.free_cash_flow as fcf, cfh.period_type
            FROM company_basic_data cbd
            JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id
            LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
            WHERE cbd.yahoo_sector = ?
              AND cfh.fiscal_year = ? AND cfh.period_type = 'annual'
              AND cfh.enterprise_value_estimated IS NOT NULL AND cfh.enterprise_value_estimated > 0
              AND cfh.total_revenue IS NOT NULL AND cfh.total_revenue > 0
        """
        params = [sector, fiscal_year]

    df = pd.read_sql_query(query, conn, params=params)
    if df.empty:
        return None

    if is_current:
        type_priority = {'quarterly': 0, 'annual': 1}
        df['_prio'] = df['period_type'].map(type_priority).fillna(2)
        df = df.sort_values('_prio').drop_duplicates(subset=['company_id'], keep='first').drop(columns=['_prio'])

    if min_ev:
        df = df[df['ev'] >= min_ev]

    df['ev_ebitda'] = np.where((df['ebitda'] > 0) & (df['ev'] > 0), df['ev'] / df['ebitda'], np.nan)
    df['ev_revenue'] = np.where(df['revenue'] > 0, df['ev'] / df['revenue'], np.nan)
    if max_ev_ebitda:
        df.loc[df['ev_ebitda'] > max_ev_ebitda, 'ev_ebitda'] = np.nan

    if len(df) == 0:
        return None

    def _stats(subset):
        result = {'n': len(subset)}
        for m in ['ev_ebitda', 'ev_revenue']:
            vals = subset[m].dropna()
            if len(vals) >= 1:
                result[m] = {
                    'median': round(float(np.median(vals)), 2),
                    'mean': round(float(np.mean(vals)), 2),
                    'p25': round(float(np.percentile(vals, 25)), 2),
                    'p75': round(float(np.percentile(vals, 75)), 2),
                    'n': int(len(vals))
                }
            else:
                result[m] = None
        return result

    # Global
    global_stats = _stats(df)
    global_stats['label'] = 'Global'

    # LATAM
    df_latam = df[df['country'].isin(LATAM_COUNTRIES)]
    latam_stats = _stats(df_latam) if len(df_latam) >= 2 else {'n': len(df_latam), 'ev_ebitda': None, 'ev_revenue': None}
    latam_stats['label'] = 'LATAM'

    # Brasil
    df_br = df[df['country'] == 'Brazil']
    br_stats = _stats(df_br) if len(df_br) >= 2 else {'n': len(df_br), 'ev_ebitda': None, 'ev_revenue': None}
    br_stats['label'] = 'Brasil'

    # Spreads
    def _spread(local, ref, metric):
        lv = local.get(metric, {})
        rv = ref.get(metric, {})
        if lv and rv and lv.get('median') and rv.get('median') and rv['median'] != 0:
            diff = lv['median'] - rv['median']
            pct = round((diff / abs(rv['median'])) * 100, 1)
            return {'diff': round(diff, 2), 'pct': pct, 'direction': 'premium' if pct > 0 else 'discount'}
        return None

    spreads = {
        'latam_vs_global': {m: _spread(latam_stats, global_stats, m) for m in ['ev_ebitda', 'ev_revenue']},
        'br_vs_global': {m: _spread(br_stats, global_stats, m) for m in ['ev_ebitda', 'ev_revenue']},
        'br_vs_latam': {m: _spread(br_stats, latam_stats, m) for m in ['ev_ebitda', 'ev_revenue']},
    }

    # Por industria
    by_industry = []
    for ind, grp in df.groupby('yahoo_industry'):
        if len(grp) >= 2:
            s = _stats(grp)
            s['label'] = ind
            # Também calcular para Brasil
            grp_br = grp[grp['country'] == 'Brazil']
            if len(grp_br) >= 1:
                br_s = _stats(grp_br)
                s['brazil_ev_ebitda'] = br_s.get('ev_ebitda')
                s['brazil_ev_revenue'] = br_s.get('ev_revenue')
                s['brazil_n'] = br_s['n']
            else:
                s['brazil_ev_ebitda'] = None
                s['brazil_ev_revenue'] = None
                s['brazil_n'] = 0
            by_industry.append(s)
    by_industry.sort(key=lambda x: x['n'], reverse=True)

    # Top empresas brasileiras
    df_br_top = df_br.dropna(subset=['ev_ebitda']).sort_values('ev_ebitda', ascending=False)
    top_br = df_br_top.head(10)[['ticker', 'company_name', 'yahoo_industry', 'ev_ebitda', 'ev_revenue', 'ev']].to_dict('records')

    return {
        'sector': sector,
        'global': global_stats,
        'latam': latam_stats,
        'brazil': br_stats,
        'spreads': spreads,
        'by_industry': by_industry,
        'top_brazil_companies': top_br,
        'total_companies': len(df)
    }


def _report_evolution_sector(conn, sector, year_start=2021, year_end=2025, min_ev=100_000_000, max_ev_ebitda=60):
    """Calcula evolução temporal de múltiplos para um setor (Global, LATAM, Brasil)."""
    results = []
    for yr in range(year_start, year_end + 1):
        query = """
            SELECT cbd.id as company_id, cbd.yahoo_country as country,
                   cfh.enterprise_value_estimated as ev,
                   cfh.total_revenue as revenue, cfh.normalized_ebitda as ebitda
            FROM company_basic_data cbd
            JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id
            WHERE cbd.yahoo_sector = ? AND cfh.fiscal_year = ? AND cfh.period_type = 'annual'
              AND cfh.enterprise_value_estimated > 0 AND cfh.total_revenue > 0
        """
        df = pd.read_sql_query(query, conn, params=[sector, yr])
        if min_ev:
            df = df[df['ev'] >= min_ev]

        df['ev_ebitda'] = np.where((df['ebitda'] > 0) & (df['ev'] > 0), df['ev'] / df['ebitda'], np.nan)
        df['ev_revenue'] = np.where(df['revenue'] > 0, df['ev'] / df['revenue'], np.nan)
        if max_ev_ebitda:
            df.loc[df['ev_ebitda'] > max_ev_ebitda, 'ev_ebitda'] = np.nan

        def _med(subset, metric):
            vals = subset[metric].dropna()
            return round(float(np.median(vals)), 2) if len(vals) >= 2 else None

        yr_data = {
            'year': yr,
            'global': {'ev_ebitda': _med(df, 'ev_ebitda'), 'ev_revenue': _med(df, 'ev_revenue'), 'n': len(df)},
            'latam': {'ev_ebitda': _med(df[df['country'].isin(LATAM_COUNTRIES)], 'ev_ebitda'),
                      'ev_revenue': _med(df[df['country'].isin(LATAM_COUNTRIES)], 'ev_revenue'),
                      'n': len(df[df['country'].isin(LATAM_COUNTRIES)])},
            'brazil': {'ev_ebitda': _med(df[df['country'] == 'Brazil'], 'ev_ebitda'),
                       'ev_revenue': _med(df[df['country'] == 'Brazil'], 'ev_revenue'),
                       'n': len(df[df['country'] == 'Brazil'])}
        }
        results.append(yr_data)
    return results


@app.route('/estudoanloc/relatorio')
def estudoanloc_relatorio_page():
    """Página do Relatório Periódico - Estudo Anloc de Múltiplos de Mercado."""
    return render_template('estudoanloc_relatorio.html')


@app.route('/api/estudoanloc/check_ai', methods=['GET'])
def api_estudoanloc_check_ai():
    """Verifica se a API key da IA está configurada e funcional."""
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return jsonify({'connected': False, 'provider': None, 'message': 'API key não configurada'})
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        # Teste rápido com mensagem mínima
        resp = client.messages.create(
            model='claude-sonnet-4-20250514',
            max_tokens=10,
            messages=[{'role': 'user', 'content': 'ping'}]
        )
        return jsonify({'connected': True, 'provider': 'Claude (Anthropic)', 'message': 'Conectado com sucesso'})
    except Exception as e:
        return jsonify({'connected': False, 'provider': 'Claude (Anthropic)', 'message': f'Erro: {str(e)}'})


@app.route('/api/estudoanloc/generate_report', methods=['POST'])
def api_estudoanloc_generate_report():
    """Gera dados completos do relatório periódico para todos os setores."""
    try:
        data = request.get_json() or {}
        fiscal_year = data.get('fiscal_year', datetime.now().year)
        selected_sectors = data.get('sectors', [])  # vazio = todos
        use_llm = data.get('use_llm', True)
        api_key = os.environ.get('ANTHROPIC_API_KEY', '') or data.get('api_key', '')

        conn = get_db()

        # Listar setores disponíveis
        all_sectors = [r[0] for r in conn.execute(
            "SELECT DISTINCT yahoo_sector FROM company_basic_data WHERE yahoo_sector IS NOT NULL ORDER BY yahoo_sector"
        ).fetchall()]

        target_sectors = selected_sectors if selected_sectors else all_sectors

        # Dados por setor
        sectors_data = []
        for sector in target_sectors:
            sector_stats = _report_calc_sector_stats(conn, sector, fiscal_year)
            if sector_stats:
                sector_stats['evolution'] = _report_evolution_sector(conn, sector)
                sectors_data.append(sector_stats)

        if not sectors_data:
            return jsonify({'success': False, 'error': 'Sem dados disponíveis para os setores selecionados'}), 404

        # Ranking geral cross-sector
        ranking = []
        for sd in sectors_data:
            g = sd.get('global', {})
            ev = g.get('ev_ebitda', {})
            evr = g.get('ev_revenue', {})
            ranking.append({
                'sector': sd['sector'],
                'ev_ebitda_median': ev.get('median') if ev else None,
                'ev_revenue_median': evr.get('median') if evr else None,
                'n_companies': g.get('n', 0),
                'brazil_n': sd.get('brazil', {}).get('n', 0),
                'br_ev_ebitda': sd.get('brazil', {}).get('ev_ebitda', {}).get('median') if sd.get('brazil', {}).get('ev_ebitda') else None,
                'br_spread_pct': sd.get('spreads', {}).get('br_vs_global', {}).get('ev_ebitda', {}).get('pct') if sd.get('spreads', {}).get('br_vs_global', {}).get('ev_ebitda') else None
            })
        ranking.sort(key=lambda x: x.get('ev_ebitda_median') or 0, reverse=True)

        # Gerar narrativa com Claude se solicitado
        llm_narratives = {}
        graph_comments = {}
        if use_llm and api_key:
            try:
                llm_narratives = _generate_report_narratives(api_key, sectors_data, ranking, fiscal_year)
            except Exception as e:
                logger.warning(f"LLM narrative generation failed: {e}")
            try:
                graph_comments = _generate_graph_comments(api_key, sectors_data, ranking, fiscal_year)
            except Exception as e:
                logger.warning(f"Graph comments generation failed: {e}")

        # Consultar datas de atualização dos dados
        data_freshness = {}
        try:
            r = conn.execute("SELECT MAX(created_at) FROM damodaran_global WHERE year = ?", (fiscal_year,)).fetchone()
            data_freshness['damodaran_base'] = r[0] if r and r[0] else None
            r = conn.execute("SELECT MAX(updated_at) FROM company_basic_data WHERE enterprise_value IS NOT NULL").fetchone()
            data_freshness['yahoo_market_data'] = r[0] if r and r[0] else None
            r = conn.execute("SELECT MAX(updated_at) FROM company_basic_data").fetchone()
            data_freshness['last_any_update'] = r[0] if r and r[0] else None
        except Exception:
            pass

        generated_at = datetime.now().isoformat()
        quarter = f'Q{(datetime.now().month - 1) // 3 + 1}/{datetime.now().year}'

        report = {
            'success': True,
            'metadata': {
                'title': f'Estudo Anloc - M\u00faltiplos de Mercado',
                'subtitle': f'An\u00e1lise Peri\u00f3dica de Valuation por M\u00faltiplos',
                'fiscal_year': fiscal_year,
                'generated_at': generated_at,
                'quarter': quarter,
                'total_sectors': len(sectors_data),
                'total_companies': sum(s.get('total_companies', 0) for s in sectors_data),
                'data_freshness': data_freshness
            },
            'ranking': ranking,
            'sectors': sectors_data,
            'narratives': llm_narratives,
            'graph_comments': graph_comments
        }

        # Salvar no cache SQLite
        try:
            import json as json_mod
            cache_conn = get_cache_db()
            _ensure_report_cache_table()
            report_data_json = json_mod.dumps({
                'metadata': report['metadata'],
                'ranking': ranking,
                'sectors': sectors_data
            }, ensure_ascii=False)
            narratives_json = json_mod.dumps(llm_narratives, ensure_ascii=False) if llm_narratives else None
            graph_json = json_mod.dumps(graph_comments, ensure_ascii=False) if graph_comments else None
            cache_conn.execute(
                "INSERT INTO report_cache (generated_at, fiscal_year, quarter, report_data, narratives, graph_comments) VALUES (?, ?, ?, ?, ?, ?)",
                (generated_at, fiscal_year, quarter, report_data_json, narratives_json, graph_json)
            )
            cache_conn.commit()
            # Recuperar o ID do cache inserido
            cache_id = cache_conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            cache_conn.close()
            report['cache_id'] = cache_id
        except Exception as e:
            logger.warning(f"Falha ao salvar cache do relatório: {e}")

        return jsonify(report)

    except Exception as e:
        logger.error(f"Erro generate_report: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


def _validate_and_tag_urls(data_dict):
    """Valida URLs nas narrativas/comentários gerados pela LLM.
    Marca URLs com 'validated' e 'status' para cada fonte.
    Remove URLs inválidas (não-HTTP) e sinaliza as que falham."""
    import concurrent.futures
    import urllib.request
    import urllib.error

    TRUSTED_DOMAINS = {
        'bcb.gov.br', 'gov.br', 'federalreserve.gov', 'sec.gov', 'imf.org',
        'worldbank.org', 'bloomberg.com', 'reuters.com', 'spglobal.com',
        'moodys.com', 'fitchratings.com', 'ibge.gov.br', 'cvm.gov.br',
        'b3.com.br', 'anbima.com.br', 'economist.com', 'ft.com',
        'wsj.com', 'cnbc.com', 'valor.globo.com', 'infomoney.com.br',
        'tradingeconomics.com', 'statista.com', 'mckinsey.com',
        'deloitte.com', 'pwc.com', 'ey.com', 'kpmg.com',
        'damodaran.com', 'pages.stern.nyu.edu', 'yahoo.com',
        'ipea.gov.br', 'reit.com', 'refinitiv.com', 'iea.org',
        'oecd.org', 'bis.org', 'goldmansachs.com', 'morganstanley.com',
    }

    # Collect all fonte arrays from the dict
    fonte_arrays = []
    fonte_keys = [k for k in data_dict.keys() if 'fontes' in k.lower() and isinstance(data_dict[k], list)]
    for k in fonte_keys:
        fonte_arrays.append((k, data_dict[k]))

    # Also check destaques_setoriais fontes
    if 'destaques_setoriais' in data_dict and isinstance(data_dict['destaques_setoriais'], list):
        for i, d in enumerate(data_dict['destaques_setoriais']):
            if isinstance(d, dict) and 'fontes' in d and isinstance(d['fontes'], list):
                fonte_arrays.append((f'destaques_setoriais[{i}].fontes', d['fontes']))

    # Flatten all URLs
    all_urls = []
    for key, arr in fonte_arrays:
        for j, f in enumerate(arr):
            if isinstance(f, dict) and f.get('url', '').startswith('http'):
                all_urls.append((key, j, f))

    if not all_urls:
        return data_dict

    def check_url(info):
        key, idx, fonte = info
        url = fonte.get('url', '')
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            trusted = any(domain.endswith(td) for td in TRUSTED_DOMAINS)
        except Exception:
            trusted = False

        try:
            req = urllib.request.Request(url, method='HEAD',
                headers={'User-Agent': 'AnlocLinkValidator/1.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                status = 'ok' if resp.status < 400 else 'error'
        except urllib.error.HTTPError as e:
            status = 'blocked' if e.code in (403, 405, 406) else 'error'
        except Exception:
            status = 'unreachable'

        fonte['_validated'] = True
        fonte['_status'] = status
        fonte['_trusted'] = trusted
        return (key, idx, status, trusted)

    # Validate in parallel (max 8 workers, timeout-safe)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(check_url, u) for u in all_urls]
            concurrent.futures.wait(futures, timeout=30)
    except Exception as e:
        logger.warning(f"URL validation partial failure: {e}")

    # Count results for logging
    total = len(all_urls)
    ok = sum(1 for _, arr in fonte_arrays for f in arr if isinstance(f, dict) and f.get('_status') == 'ok')
    errors = sum(1 for _, arr in fonte_arrays for f in arr if isinstance(f, dict) and f.get('_status') in ('error', 'unreachable'))
    logger.info(f"URL validation: {ok}/{total} OK, {errors} errors")

    return data_dict


def _repair_truncated_json(text):
    """Tenta reparar JSON truncado pelo limite de tokens do LLM."""
    import json as json_mod
    # Fechar strings, arrays e objetos abertos
    fixed = text
    # Contar delimitadores abertos
    in_string = False
    escape = False
    stack = []
    for ch in fixed:
        if escape:
            escape = False
            continue
        if ch == '\\':
            escape = True
            continue
        if ch == '"' and (not stack or stack[-1] != '"'):
            in_string = True
            stack.append('"')
        elif ch == '"' and stack and stack[-1] == '"':
            in_string = False
            stack.pop()
        elif not in_string:
            if ch in ('{', '['):
                stack.append(ch)
            elif ch == '}' and stack and stack[-1] == '{':
                stack.pop()
            elif ch == ']' and stack and stack[-1] == '[':
                stack.pop()
    # Fechar tudo que ficou aberto
    if in_string:
        fixed += '"'
        if stack and stack[-1] == '"':
            stack.pop()
    while stack:
        top = stack.pop()
        if top == '{':
            fixed += '}'
        elif top == '[':
            fixed += ']'
    try:
        return json_mod.loads(fixed)
    except json_mod.JSONDecodeError:
        logger.warning("JSON repair failed, returning partial result")
        # Última tentativa: encontrar último objeto/array válido
        for i in range(len(text), 0, -1):
            try:
                return json_mod.loads(text[:i] + '}' * text[:i].count('{') + ']' * text[:i].count('['))
            except Exception:
                continue
        raise


def _generate_report_narratives(api_key, sectors_data, ranking, fiscal_year):
    """Gera narrativas analíticas usando Claude para o relatório."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    # Preparar contexto compacto
    ctx_lines = [f"ESTUDO DE MULTIPLOS DE MERCADO - Ano fiscal: {fiscal_year}",
                 f"Total de setores: {len(sectors_data)}\n"]

    ctx_lines.append("RANKING DE SETORES (EV/EBITDA mediana global):")
    for r in ranking:
        br_info = f" | Brasil: {r['br_ev_ebitda']:.1f}x (spread {r['br_spread_pct']:+.1f}%)" if r.get('br_ev_ebitda') and r.get('br_spread_pct') is not None else ""
        ctx_lines.append(f"  {r['sector']}: {r.get('ev_ebitda_median','N/D')}x (n={r['n_companies']}){br_info}")

    ctx_lines.append("\nDETALHES POR SETOR:")
    for sd in sectors_data:
        g = sd['global']
        ev = g.get('ev_ebitda', {})
        evr = g.get('ev_revenue', {})
        ctx_lines.append(f"\n--- {sd['sector']} ({g['n']} empresas) ---")
        if ev:
            ctx_lines.append(f"  Global EV/EBITDA: med={ev.get('median')}x p25={ev.get('p25')}x p75={ev.get('p75')}x")
        if evr:
            ctx_lines.append(f"  Global EV/Revenue: med={evr.get('median')}x")

        # Spreads
        sp = sd.get('spreads', {})
        for geo_pair, label in [('latam_vs_global', 'LATAM vs Global'), ('br_vs_global', 'Brasil vs Global'), ('br_vs_latam', 'Brasil vs LATAM')]:
            sp_ev = sp.get(geo_pair, {}).get('ev_ebitda')
            if sp_ev:
                ctx_lines.append(f"  {label}: {sp_ev['pct']:+.1f}% ({sp_ev['direction']})")

        # Top indústrias
        for ind in sd.get('by_industry', [])[:5]:
            ie = ind.get('ev_ebitda', {})
            if ie:
                br_txt = f" | BR: {ind['brazil_ev_ebitda'].get('median')}x" if ind.get('brazil_ev_ebitda') else ""
                ctx_lines.append(f"  Ind: {ind['label']}: {ie.get('median')}x (n={ind['n']}){br_txt}")

        # Evolução
        evo = sd.get('evolution', [])
        if evo:
            evo_vals = [f"{e['year']}:{e['global'].get('ev_ebitda','?')}x" for e in evo if e['global'].get('ev_ebitda')]
            if evo_vals:
                ctx_lines.append(f"  Evo\u00e7\u00e3o Global EV/EBITDA: {' \u2192 '.join(evo_vals)}")

    context = "\n".join(ctx_lines)

    prompt = f"""Você é um analista financeiro sênior do Anloc, empresa especializada em valuation e benchmark.
Analise os dados abaixo e gere as narrativas para o relatório periódico "Estudo Anloc de Múltiplos de Mercado".

DADOS:
{context}

Gere um JSON com a seguinte estrutura (responda SOMENTE o JSON, sem markdown):
{{
  "resumo_executivo": "3-4 parágrafos: panorama geral dos múltiplos de mercado, quais setores estão mais caros/baratos, tendências observadas, posicionamento do Brasil vs global. Tom profissional para C-level e investidores.",
  "resumo_executivo_fontes": [{{"nome": "Nome da fonte", "url": "https://url-da-fonte.com"}}],
  "analise_macro": "2-3 parágrafos: análise macro dos fatores que influenciam os múltiplos atuais (juros, inflação, sentimento de mercado, ciclo econômico). Use seu conhecimento atualizado. Ao citar fatores externos, indique a fonte inline entre parênteses.",
  "analise_macro_fontes": [{{"nome": "BCB - Taxa Selic", "url": "https://www.bcb.gov.br/controleinflacao/taxaselic"}}, {{"nome": "FED - Federal Funds Rate", "url": "https://www.federalreserve.gov/monetarypolicy/openmarket.htm"}}],
  "analise_brasil": "2-3 parágrafos: análise específica do Brasil - desconto/prêmio em relação ao global e LATAM, fatores locais (Selic, câmbio, ambiente político), setores com maior spread. Cite fontes inline.",
  "analise_brasil_fontes": [{{"nome": "fonte", "url": "https://..."}}],
  "destaques_setoriais": [
    {{
      "setor": "nome do setor",
      "ev_ebitda_global": 12.5,
      "ev_ebitda_br": 8.3,
      "ev_revenue_global": 2.1,
      "spread_pct": -33.6,
      "n_empresas": 450,
      "tendencia": "expansão|compressão|estável",
      "insight": "2-3 frases com análise profunda do setor: o que os dados revelam, fatores causais, comparações relevantes. Cite dados específicos. Comece SEMPRE pelos dados e depois tire conclusões.",
      "citacao": {{"autor": "Nome do especialista ou entidade", "cargo": "Cargo/Instituição", "frase": "Citação relevante sobre o setor entre aspas", "contexto": "Onde/quando foi dito", "url": "https://url-da-fonte.com"}},
      "fontes": [{{"nome": "fonte", "url": "https://..."}}]
    }}
  ],
  "perspectivas": "1-2 parágrafos com perspectivas e tendências futuras para múltiplos. Cite fontes.",
  "perspectivas_fontes": [{{"nome": "fonte", "url": "https://..."}}],
  "citacoes_especialistas": [
    {{"autor": "Nome", "cargo": "Cargo/Instituição", "frase": "Citação marcante sobre o mercado", "contexto": "Fonte/evento/data", "url": "https://url-da-fonte-original.com", "secao": "resumo_executivo|analise_macro|analise_brasil|perspectivas"}}
  ],
  "fontes_contexto": [{{"nome": "Nome completo da fonte", "url": "https://url-da-fonte.com", "tipo": "institucional|relatório|dados|mídia"}}]
}}

IMPORTANTE:
- Use os dados fornecidos como base. Complemente com seu conhecimento sobre macroeconomia e mercados.
- Cite números específicos dos dados.
- Para CADA seção, liste as fontes como objetos com nome e URL real/plausível.
- Fontes devem ser específicas: "BCB - Relatório Focus", "FED - FOMC Minutes", "Bloomberg", "S&P Global", etc.
- Em destaques_setoriais, preencha os campos numéricos com os dados reais do setor (extraia do contexto).
- Em destaques_setoriais, o campo "tendencia" deve ser "expansão", "compressão" ou "estável" baseado na evolução.
- Em destaques_setoriais, o "insight" deve COMEÇAR pelos dados concretos e DEPOIS apresentar conclusões.
- Inclua citações de especialistas/entidades relevantes (analistas, bancos, instituições) em citacoes_especialistas.
- Para cada destaque setorial, se possível inclua uma citação de especialista no campo "citacao".
- Tom profissional, analítico, objetivo.
- Português brasileiro.

DIRETRIZES DE COMPLIANCE (OBRIGATÓRIAS):
- NÃO recomendar compra ou venda de ativos. NÃO sugerir investimentos ou alocação de portfólio.
- NÃO usar termos como: "recomendamos", "invista em", "compre", "venda", "oportunidade imperdível", "preço-alvo".
- NÃO gerar previsões de preço ou retorno garantido. NÃO afirmar que algo "vai subir" ou "vai cair".
- NÃO inventar dados, números ou estatísticas que não estejam no dataset fornecido.
- NÃO inventar nomes de analistas individuais. Atribuir citações a ENTIDADES (bancos, consultorias, relatórios institucionais).
- NÃO mencionar nomes de pessoas físicas, exceto porta-vozes oficiais em citações verificáveis (CEO em earnings call, etc.).
- Use linguagem condicional: "os dados indicam", "sugere", "observa-se", "pode refletir".
- Distinguir explicitamente entre fatos (dados) e interpretação analítica.
- Este material é informativo e educacional. Não constitui recomendação de investimento."""

    response = client.messages.create(
        model='claude-sonnet-4-20250514',
        max_tokens=8192,
        system="Voc\u00ea \u00e9 um analista financeiro s\u00eanior. Responda SOMENTE com JSON v\u00e1lido, sem markdown.",
        messages=[{'role': 'user', 'content': prompt}]
    )

    text = response.content[0].text.strip()
    if text.startswith('```'):
        text = text.split('\n', 1)[1] if '\n' in text else text[3:]
        if text.endswith('```'):
            text = text[:-3]
        if text.startswith('json'):
            text = text[4:]
        text = text.strip()

    import json as json_mod
    try:
        result = json_mod.loads(text)
    except json_mod.JSONDecodeError:
        result = _repair_truncated_json(text)
    try:
        _validate_and_tag_urls(result)
    except Exception as e:
        logger.warning(f"URL validation failed for narratives: {e}")
    return result


# ========================================================================
# REPORT CACHE - Tabela e funções de cache para relatórios
# ========================================================================

def _ensure_report_cache_table():
    """Cria a tabela report_cache se não existir."""
    conn = get_cache_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS report_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            generated_at TEXT NOT NULL UNIQUE,
            fiscal_year INTEGER NOT NULL,
            quarter TEXT NOT NULL,
            report_data TEXT NOT NULL,
            narratives TEXT,
            graph_comments TEXT,
            chat_history TEXT,
            label TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_report_cache_year
        ON report_cache(fiscal_year, created_at DESC)
    """)
    # Migrações: adicionar colunas se não existirem
    for col in ['label TEXT', 'deep_analyses TEXT']:
        try:
            conn.execute(f"ALTER TABLE report_cache ADD COLUMN {col}")
        except Exception:
            pass  # Coluna já existe
    conn.commit()
    conn.close()

# Criar tabela ao inicializar
try:
    _ensure_report_cache_table()
except Exception:
    pass


@app.route('/api/estudoanloc/report_cache/list', methods=['GET'])
def api_report_cache_list():
    """Lista relatórios cacheados."""
    fiscal_year = request.args.get('fiscal_year', type=int)
    conn = get_cache_db()
    if fiscal_year:
        rows = conn.execute(
            "SELECT id, generated_at, fiscal_year, quarter, label FROM report_cache WHERE fiscal_year = ? ORDER BY created_at DESC LIMIT 50",
            (fiscal_year,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, generated_at, fiscal_year, quarter, label FROM report_cache ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
    conn.close()
    return jsonify({'success': True, 'reports': [
        {'id': r[0], 'generated_at': r[1], 'fiscal_year': r[2], 'quarter': r[3], 'label': r[4]} for r in rows
    ]})


@app.route('/api/estudoanloc/report_cache/load', methods=['GET'])
def api_report_cache_load():
    """Carrega um relatório do cache por ID ou pelo mais recente do ano fiscal."""
    import json as json_mod
    cache_id = request.args.get('id', type=int)
    fiscal_year = request.args.get('fiscal_year', type=int)
    _cols = "id, generated_at, fiscal_year, quarter, report_data, narratives, graph_comments, chat_history, deep_analyses"
    conn = get_cache_db()
    if cache_id:
        row = conn.execute(
            f"SELECT {_cols} FROM report_cache WHERE id = ?",
            (cache_id,)
        ).fetchone()
    elif fiscal_year:
        row = conn.execute(
            f"SELECT {_cols} FROM report_cache WHERE fiscal_year = ? ORDER BY created_at DESC LIMIT 1",
            (fiscal_year,)
        ).fetchone()
    else:
        row = conn.execute(
            f"SELECT {_cols} FROM report_cache ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    conn.close()
    if not row:
        return jsonify({'success': False, 'error': 'Nenhum relatório em cache'}), 404
    return jsonify({
        'success': True,
        'cache_id': row[0],
        'generated_at': row[1],
        'fiscal_year': row[2],
        'quarter': row[3],
        'report_data': json_mod.loads(row[4]) if row[4] else {},
        'narratives': json_mod.loads(row[5]) if row[5] else {},
        'graph_comments': json_mod.loads(row[6]) if row[6] else {},
        'chat_history': json_mod.loads(row[7]) if row[7] else [],
        'deep_analyses': json_mod.loads(row[8]) if row[8] else {}
    })


@app.route('/api/estudoanloc/report_cache/rename', methods=['POST'])
def api_report_cache_rename():
    """Renomeia/atribui label a um relatório cacheado."""
    data = request.get_json() or {}
    cache_id = data.get('cache_id')
    label = data.get('label', '').strip()
    if not cache_id:
        return jsonify({'success': False, 'error': 'cache_id obrigatório'}), 400
    conn = get_cache_db()
    conn.execute("UPDATE report_cache SET label = ? WHERE id = ?", (label or None, cache_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/estudoanloc/report_cache/delete', methods=['POST'])
def api_report_cache_delete():
    """Remove um relatório do cache."""
    data = request.get_json() or {}
    cache_id = data.get('cache_id')
    if not cache_id:
        return jsonify({'success': False, 'error': 'cache_id obrigatório'}), 400
    try:
        conn = get_cache_db()
        conn.execute("DELETE FROM report_cache WHERE id = ?", (cache_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Erro ao excluir cache {cache_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/estudoanloc/report_cache/save_deep_analysis', methods=['POST'])
def api_report_cache_save_deep_analysis():
    """Salva as análises profundas no cache do relatório."""
    import json as json_mod
    data = request.get_json() or {}
    cache_id = data.get('cache_id')
    deep_analyses = data.get('deep_analyses', {})
    if not cache_id:
        return jsonify({'success': False, 'error': 'cache_id obrigatório'}), 400
    conn = get_cache_db()
    conn.execute("UPDATE report_cache SET deep_analyses = ? WHERE id = ?",
                 (json_mod.dumps(deep_analyses, ensure_ascii=False), cache_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/estudoanloc/report_cache/save_chat', methods=['POST'])
def api_report_cache_save_chat():
    """Salva o histórico de chat no cache do relatório."""
    import json as json_mod
    data = request.get_json() or {}
    cache_id = data.get('cache_id')
    chat_history = data.get('chat_history', [])
    if not cache_id:
        return jsonify({'success': False, 'error': 'cache_id obrigatório'}), 400
    conn = get_cache_db()
    conn.execute("UPDATE report_cache SET chat_history = ? WHERE id = ?",
                 (json_mod.dumps(chat_history, ensure_ascii=False), cache_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/estudoanloc/relatorio/validate_links', methods=['POST'])
def api_estudoanloc_validate_links():
    """Valida URLs citadas nas narrativas do relatório via HTTP HEAD requests."""
    import concurrent.futures
    import urllib.request
    import urllib.error

    data = request.get_json() or {}
    urls = data.get('urls', [])
    if not urls or not isinstance(urls, list):
        return jsonify({'success': False, 'error': 'Lista de URLs obrigatória'}), 400

    # Limitar a 50 URLs por chamada para evitar abuso
    urls = urls[:50]

    # Domínios institucionais conhecidos (confiáveis)
    TRUSTED_DOMAINS = {
        'bcb.gov.br', 'gov.br', 'federalreserve.gov', 'sec.gov', 'imf.org',
        'worldbank.org', 'bloomberg.com', 'reuters.com', 'spglobal.com',
        'moodys.com', 'fitchratings.com', 'ibge.gov.br', 'cvm.gov.br',
        'b3.com.br', 'anbima.com.br', 'economist.com', 'ft.com',
        'wsj.com', 'cnbc.com', 'valor.globo.com', 'infomoney.com.br',
        'tradingeconomics.com', 'statista.com', 'mckinsey.com',
        'deloitte.com', 'pwc.com', 'ey.com', 'kpmg.com',
        'damodaran.com', 'pages.stern.nyu.edu', 'yahoo.com',
    }

    def validate_url(url_info):
        """Valida uma URL individual."""
        url = url_info.get('url', '')
        nome = url_info.get('nome', '')
        result = {'url': url, 'nome': nome, 'status': 'unknown', 'code': 0, 'trusted_domain': False}

        if not url or not url.startswith('http'):
            result['status'] = 'invalid'
            result['message'] = 'URL inválida ou ausente'
            return result

        # Verificar domínio confiável
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            for td in TRUSTED_DOMAINS:
                if domain.endswith(td):
                    result['trusted_domain'] = True
                    break
        except Exception:
            pass

        try:
            req = urllib.request.Request(url, method='HEAD',
                headers={'User-Agent': 'AnlocLinkValidator/1.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                result['code'] = resp.status
                result['status'] = 'ok' if resp.status < 400 else 'error'
        except urllib.error.HTTPError as e:
            result['code'] = e.code
            # 403/405 podem significar que o site bloqueia HEAD mas existe
            if e.code in (403, 405, 406):
                result['status'] = 'blocked'
                result['message'] = 'Site bloqueia verificação automática'
            else:
                result['status'] = 'error'
                result['message'] = f'HTTP {e.code}'
        except urllib.error.URLError as e:
            result['status'] = 'unreachable'
            result['message'] = 'Site inacessível'
        except Exception as e:
            result['status'] = 'error'
            result['message'] = str(e)[:100]

        return result

    # Verificar em paralelo (max 10 workers)
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {executor.submit(validate_url, u): u for u in urls}
        for future in concurrent.futures.as_completed(future_map):
            results.append(future.result())

    # Estatísticas
    total = len(results)
    ok_count = sum(1 for r in results if r['status'] == 'ok')
    blocked_count = sum(1 for r in results if r['status'] == 'blocked')
    error_count = sum(1 for r in results if r['status'] in ('error', 'unreachable', 'invalid'))
    trusted_count = sum(1 for r in results if r['trusted_domain'])

    return jsonify({
        'success': True,
        'summary': {
            'total': total,
            'ok': ok_count,
            'blocked': blocked_count,
            'errors': error_count,
            'trusted_domains': trusted_count
        },
        'results': sorted(results, key=lambda r: (r['status'] != 'error', r['status'] != 'unreachable', r['status']))
    })


# ========================================================================
# CHAT DO RELATÓRIO - Endpoint com contexto completo do relatório
# ========================================================================

@app.route('/api/estudoanloc/relatorio/chat', methods=['POST'])
def api_estudoanloc_relatorio_chat():
    """Chat conversacional com contexto completo do relatório gerado."""
    try:
        data = request.get_json() or {}
        messages = data.get('messages', [])
        report_context = data.get('report_context', '')

        if not messages:
            return jsonify({'success': False, 'error': 'Nenhuma mensagem enviada'}), 400

        api_key = os.environ.get('ANTHROPIC_API_KEY', '')
        if not api_key:
            return jsonify({'success': False, 'error': 'API Key Anthropic não configurada no servidor'}), 400

        system_prompt = """Você é um analista financeiro sênior do Anloc, empresa especializada em valuation e benchmark com mais de 30 anos de expertise.
Você está no contexto de um Estudo Periódico de Múltiplos de Mercado que compara EV/EBITDA e EV/Revenue entre setores globais, LATAM e Brasil.

REGRAS:
- Responda SEMPRE em português brasileiro, de forma profissional e analítica.
- Baseie-se nos dados do relatório fornecidos no contexto. Cite números específicos.
- Quando usar conhecimento externo (macro, tendências), indique a fonte entre parênteses.
- Formate com parágrafos curtos. Use **negrito** para destaques.
- Se o usuário perguntar sobre algo fora do escopo dos dados, informe as limitações.
- Ao final de cada resposta que use fontes externas, adicione uma linha "Fontes: ..." listando as referências.

DIRETRIZES DE COMPLIANCE (OBRIGATÓRIAS):
- NÃO recomendar compra ou venda de ativos. NÃO sugerir investimentos ou alocação de portfólio.
- Se perguntado sobre investimento, responda: "Este relatório é informativo. Para decisões de investimento, consulte profissional habilitado (CVM)."
- Se perguntado sobre previsão de preço: "Não possuímos capacidade preditiva. Os dados refletem o momento atual do mercado."
- RECUSAR responder sobre: recomendações de compra/venda, previsões de preço/retorno, comparação com ofertas de corretoras.
- NÃO inventar dados ou citações de pessoas. Usar linguagem condicional ("os dados indicam", "sugere").
- Este material é informativo e educacional. Não constitui recomendação de investimento."""

        if report_context:
            system_prompt += f"\n\nCONTEXTO DO RELATÓRIO (dados e narrativas gerados):\n{report_context}"

        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        anth_messages = [{'role': msg['role'], 'content': msg['content']} for msg in messages]
        response = client.messages.create(
            model='claude-sonnet-4-20250514', max_tokens=4096,
            system=system_prompt, messages=anth_messages)
        response_text = response.content[0].text

        return jsonify({'success': True, 'message': response_text})

    except Exception as e:
        logger.error(f"Erro chat relatório: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ========================================================================
# AI GRAPH COMMENTS - Análises específicas por gráfico
# ========================================================================

def _generate_graph_comments(api_key, sectors_data, ranking, fiscal_year):
    """Gera comentários analíticos específicos para cada gráfico do relatório."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    # Preparar dados resumidos
    ranking_txt = "\n".join([
        f"  {r['sector']}: Global {r.get('ev_ebitda_median','N/D')}x | BR {r.get('br_ev_ebitda','N/D')}x | Spread {r.get('br_spread_pct','N/D')}%"
        for r in ranking
    ])

    geo_txt = ""
    for sd in sectors_data:
        g = sd.get('global', {}).get('ev_ebitda', {})
        l = sd.get('latam', {}).get('ev_ebitda', {}) if sd.get('latam') else {}
        b = sd.get('brazil', {}).get('ev_ebitda', {}) if sd.get('brazil') else {}
        geo_txt += f"  {sd['sector']}: Global {g.get('median','?')}x | LATAM {l.get('median','?')}x | BR {b.get('median','?')}x\n"

    evo_txt = ""
    for sd in sectors_data:
        evo = sd.get('evolution', [])
        if evo:
            vals = [f"{e['year']}:{e['global'].get('ev_ebitda','?')}x" for e in evo if e.get('global', {}).get('ev_ebitda')]
            if vals:
                evo_txt += f"  {sd['sector']}: {' → '.join(vals)}\n"

    prompt = f"""Você é um analista financeiro sênior do Anloc. Analise os dados e gere comentários ESPECÍFICOS para cada gráfico.

RANKING (EV/EBITDA por setor - Global vs Brasil):
{ranking_txt}

GEOGRAFIA (Global vs LATAM vs Brasil por setor):
{geo_txt}

EVOLUÇÃO (2021-2025 EV/EBITDA global por setor):
{evo_txt}

Gere um JSON (SOMENTE o JSON, sem markdown) com esta estrutura:
{{
  "ranking": {{
    "titulo": "título curto e analítico para o gráfico",
    "analise": "2-3 parágrafos analisando o ranking: quais setores estão mais caros/baratos, padrões observados, onde Brasil tem prêmio ou desconto significativo. Cite números.",
    "destaque": "1 frase de destaque principal (insight mais importante)",
    "fontes": [{{"nome": "Nome da fonte", "url": "https://url-da-fonte.com"}}]
  }},
  "geografia": {{
    "titulo": "título curto",
    "analise": "2-3 parágrafos: como os múltiplos se comparam entre regiões, quais setores têm maior diferencial, hipóteses para os descontos/prêmios Brasil e LATAM. Mencione fatores como risco país, liquidez, câmbio.",
    "destaque": "1 frase de destaque",
    "fontes": [{{"nome": "fonte", "url": "https://..."}}]
  }},
  "evolucao": {{
    "titulo": "título curto",
    "analise": "2-3 parágrafos: tendências de compressão/expansão de múltiplos 2021-2025, impacto do ciclo de juros global, quais setores foram mais resilientes e quais sofreram mais. Contextualize com eventos de mercado.",
    "destaque": "1 frase de destaque",
    "fontes": [{{"nome": "fonte", "url": "https://..."}}]
  }}
}}

IMPORTANTE:
- Cite números específicos dos dados.
- Quando usar conhecimento externo (tendências macro, eventos), indique a fonte.
- Tom profissional, analítico.
- Português brasileiro.

DIRETRIZES DE COMPLIANCE (OBRIGATÓRIAS):
- NÃO recomendar compra ou venda de ativos. NÃO sugerir investimentos.
- NÃO interpretar desconto como "oportunidade de compra" ou "subvalorização injusta".
- NÃO prever continuação de tendências. Descrever o observado, não o futuro.
- NÃO usar linguagem sensacionalista ("disparou", "desabou").
- Use linguagem condicional: "os dados indicam", "observa-se", "pode refletir".
- Este material é informativo e não constitui recomendação de investimento."""

    response = client.messages.create(
        model='claude-sonnet-4-20250514',
        max_tokens=8192,
        system="Você é um analista financeiro sênior. Responda SOMENTE com JSON válido, sem markdown.",
        messages=[{'role': 'user', 'content': prompt}]
    )

    text = response.content[0].text.strip()
    if text.startswith('```'):
        text = text.split('\n', 1)[1] if '\n' in text else text[3:]
        if text.endswith('```'):
            text = text[:-3]
        if text.startswith('json'):
            text = text[4:]
        text = text.strip()

    import json as json_mod
    try:
        result = json_mod.loads(text)
    except json_mod.JSONDecodeError:
        result = _repair_truncated_json(text)
    try:
        _validate_and_tag_urls(result)
    except Exception as e:
        logger.warning(f"URL validation failed for graph_comments: {e}")
    return result


# ========================================================================
# SECTOR DEEP ANALYSIS - Análise aprofundada por setor com IA
# ========================================================================

@app.route('/api/estudoanloc/relatorio/sector_deep_analysis', methods=['POST'])
def api_estudoanloc_sector_deep_analysis():
    """Gera análise aprofundada de um setor específico usando IA."""
    try:
        data = request.get_json() or {}
        sector_name = data.get('sector', '')
        sector_data = data.get('sector_data', {})
        fiscal_year = data.get('fiscal_year', datetime.now().year)

        if not sector_name:
            return jsonify({'success': False, 'error': 'Setor não especificado'}), 400

        api_key = os.environ.get('ANTHROPIC_API_KEY', '')
        if not api_key:
            return jsonify({'success': False, 'error': 'API key não configurada'}), 400

        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        # Construir contexto detalhado do setor
        ctx_parts = [f"ANÁLISE APROFUNDADA DO SETOR: {sector_name}", f"Ano fiscal: {fiscal_year}\n"]

        g = sector_data.get('global', {})
        l = sector_data.get('latam', {})
        b = sector_data.get('brazil', {})

        gev = g.get('ev_ebitda', {}) if isinstance(g.get('ev_ebitda'), dict) else {}
        gevr = g.get('ev_revenue', {}) if isinstance(g.get('ev_revenue'), dict) else {}
        lev = l.get('ev_ebitda', {}) if isinstance(l.get('ev_ebitda'), dict) else {}
        bev = b.get('ev_ebitda', {}) if isinstance(b.get('ev_ebitda'), dict) else {}

        ctx_parts.append(f"DADOS GLOBAIS: EV/EBITDA med={gev.get('median','?')}x p25={gev.get('p25','?')}x p75={gev.get('p75','?')}x | EV/Revenue med={gevr.get('median','?')}x | n={g.get('n', 0)}")
        ctx_parts.append(f"DADOS LATAM: EV/EBITDA med={lev.get('median','?')}x | n={l.get('n', 0)}")
        ctx_parts.append(f"DADOS BRASIL: EV/EBITDA med={bev.get('median','?')}x | n={b.get('n', 0)}")

        sp = sector_data.get('spreads', {})
        for geo_pair, label in [('br_vs_global', 'Brasil vs Global'), ('br_vs_latam', 'Brasil vs LATAM'), ('latam_vs_global', 'LATAM vs Global')]:
            sp_ev = sp.get(geo_pair, {}).get('ev_ebitda')
            if sp_ev:
                ctx_parts.append(f"SPREAD {label}: {sp_ev.get('pct', '?')}% ({sp_ev.get('direction', '?')})")

        # Indústrias
        industries = sector_data.get('by_industry', [])
        if industries:
            ctx_parts.append("\nINDÚSTRIAS DO SETOR:")
            for ind in industries[:10]:
                ie = ind.get('ev_ebitda', {})
                br_ie = ind.get('brazil_ev_ebitda', {})
                br_txt = f" | BR: {br_ie.get('median','?')}x" if br_ie and br_ie.get('median') else ""
                ctx_parts.append(f"  {ind.get('label','?')}: Global {ie.get('median','?')}x (n={ind.get('n', 0)}){br_txt}")

        # Top BR companies
        top_br = sector_data.get('top_brazil_companies', [])
        if top_br:
            ctx_parts.append("\nTOP EMPRESAS BRASILEIRAS:")
            for c in top_br[:10]:
                ctx_parts.append(f"  {c.get('ticker','')} - {c.get('company_name','')}: EV/EBITDA={c.get('ev_ebitda','?')}x, EV/Rev={c.get('ev_revenue','?')}x ({c.get('yahoo_industry','')})")

        # Evolução
        evo = sector_data.get('evolution', [])
        if evo:
            evo_vals = [f"{e.get('year','?')}:{e.get('global',{}).get('ev_ebitda','?')}x" for e in evo if e.get('global',{}).get('ev_ebitda')]
            if evo_vals:
                ctx_parts.append(f"\nEVOLUÇÃO GLOBAL EV/EBITDA: {' → '.join(evo_vals)}")

        context = "\n".join(ctx_parts)

        prompt = f"""Você é um analista financeiro sênior do Anloc, especializado no setor {sector_name}.
Com base nos dados abaixo, faça uma análise aprofundada e abrangente deste setor.

{context}

Responda SOMENTE um JSON com esta estrutura:
{{
  "titulo": "Título analítico para a análise do setor",
  "panorama": "2-3 parágrafos: visão geral do setor globalmente, principais drivers de valuação, posicionamento relativo. Cite números dos dados.",
  "analise_brasil": "2-3 parágrafos: como o Brasil se posiciona neste setor vs global/LATAM, fatores locais que impactam, empresas de destaque e seus diferenciais.",
  "industrias_destaque": "1-2 parágrafos: quais sub-indústrias se destacam e por quê, diferenças de múltiplos entre elas.",
  "tendencias": "1-2 parágrafos: evolução recente dos múltiplos, o que mudou e expectativas futuras para o setor.",
  "riscos_oportunidades": "1-2 parágrafos: principais riscos e oportunidades DO SETOR (não de investimento). Descreva fatores que podem impactar margens, crescimento e múltiplos.",
  "conclusao": "1 parágrafo: síntese executiva analítica do posicionamento atual do setor. NÃO recomendar posição comprada/vendida.",
  "citacoes": [
    {{"autor": "Nome", "cargo": "Cargo/Instituição", "frase": "Citação relevante", "contexto": "Fonte", "url": "https://url-da-fonte.com"}}
  ],
  "fontes": [{{"nome": "Nome da fonte", "url": "https://url"}}]
}}

IMPORTANTE:
- Utilize os dados fornecidos como base principal.
- Complemente com seu conhecimento sobre o setor, tendências, players relevantes.
- Cite números específicos dos dados.
- Inclua 2-3 citações de entidades/instituições relevantes para o setor (NÃO inventar nomes de analistas individuais).
- Tom profissional, aprofundado, analítico.
- Português brasileiro.

DIRETRIZES DE COMPLIANCE (OBRIGATÓRIAS):
- NÃO recomendar compra ou venda de ativos. NÃO sugerir investimentos ou alocação.
- Em "riscos_oportunidades": descrever riscos e oportunidades DO SETOR, não de investimento.
- Em "conclusao": síntese analítica, NÃO recomendação de posicionamento comprado/vendido.
- NÃO usar termos como: "recomendamos", "invista em", "preço-alvo", "compre", "venda".
- NÃO inventar dados ou estatísticas ausentes do dataset.
- NÃO inventar citações de pessoas físicas. Atribuir a entidades verificáveis.
- Use linguagem condicional: "os dados indicam", "sugere", "pode refletir".
- Este material é informativo e não constitui recomendação de investimento."""

        response = client.messages.create(
            model='claude-sonnet-4-20250514',
            max_tokens=8192,
            system="Você é um analista financeiro sênior especializado em valuation setorial. Responda SOMENTE com JSON válido, sem markdown.",
            messages=[{'role': 'user', 'content': prompt}]
        )

        text = response.content[0].text.strip()
        if text.startswith('```'):
            text = text.split('\n', 1)[1] if '\n' in text else text[3:]
            if text.endswith('```'):
                text = text[:-3]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()

        import json as json_mod
        try:
            result = json_mod.loads(text)
        except json_mod.JSONDecodeError:
            result = _repair_truncated_json(text)
        return jsonify({'success': True, 'analysis': result})

    except Exception as e:
        logger.error(f"Erro sector deep analysis: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ========================================================================
# INICIALIZAÇÃO DA APLICAÇÃO
# ========================================================================
if __name__ == '__main__':
    # Criar diretórios necessários
    Path("templates").mkdir(exist_ok=True)
    Path("static").mkdir(exist_ok=True)
    Path("static/css").mkdir(exist_ok=True)
    Path("static/js").mkdir(exist_ok=True)
    
    logger.info("Iniciando aplicação WACC Calculator")
    
    # Executar aplicação
    app.run(
        host=os.environ.get('FLASK_HOST', '127.0.0.1'),
        port=int(os.environ.get('FLASK_PORT', 5000)),
        debug=os.environ.get('FLASK_DEBUG', '0') == '1',
        threaded=True
    )