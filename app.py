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
import sqlite3
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
app.config['SECRET_KEY'] = 'wacc-calculator-2025'
app.config['JSON_AS_ASCII'] = False

# Configurar logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Inicializar calculadora WACC
calculator = WACCCalculator()
data_manager = WACCDataManager()
wacc_connector = WACCDataConnector()
field_manager = FieldCategoriesManager()
data_source_mgr = DataSourceManager()

# Configurações globais
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

# Classe para análise de empresas
class CompanyAnalyzer:
    def __init__(self, db_path='data/damodaran_data_new.db'):
        self.db_path = db_path
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
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
        
        conn = sqlite3.connect('data/damodaran_data_new.db')
        
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
        
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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

@app.route('/data-yahoo')
def data_yahoo_page():
    """Dashboard analítico dos dados obtidos do Yahoo Finance."""
    return render_template('data_yahoo.html')


@app.route('/api/yahoo_dashboard_summary')
def api_yahoo_dashboard_summary():
    """Retorna resumo geral dos dados Yahoo para os KPI cards."""
    try:
        conn = sqlite3.connect('data/damodaran_data_new.db')
        cur = conn.cursor()

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

        conn.close()
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_sectors')
def api_yahoo_dashboard_sectors():
    """Métricas agregadas por setor Yahoo."""
    try:
        conn = sqlite3.connect('data/damodaran_data_new.db')
        query = """
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
                MEDIAN(CAST(dg.pe_ratio AS REAL)) AS med_pe
            FROM damodaran_global dg
            LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
            WHERE cbd.yahoo_sector IS NOT NULL
            GROUP BY cbd.yahoo_sector
            ORDER BY COUNT(*) DESC
        """
        # SQLite doesn't have MEDIAN, use a simpler approach
        query = query.replace("MEDIAN(CAST(dg.pe_ratio AS REAL)) AS med_pe", "0 AS med_pe")
        df = pd.read_sql_query(query, conn)
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
        params = []
        where = "WHERE cbd.yahoo_industry IS NOT NULL"
        if sector:
            where += " AND cbd.yahoo_sector = ?"
            params.append(sector)

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
        conn = sqlite3.connect('data/damodaran_data_new.db')
        query = """
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
            WHERE cbd.yahoo_country IS NOT NULL
            GROUP BY cbd.yahoo_country
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return jsonify({'success': True, 'countries': df.to_dict('records')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/yahoo_dashboard_atividades')
def api_yahoo_dashboard_atividades():
    """Métricas agregadas por atividade Anloc."""
    try:
        conn = sqlite3.connect('data/damodaran_data_new.db')
        query = """
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
            WHERE dg.atividade_anloc IS NOT NULL AND dg.atividade_anloc != ''
            GROUP BY dg.atividade_anloc
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
        """
        df = pd.read_sql_query(query, conn)
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
        params = []
        where = "WHERE cbd.yahoo_sector IS NOT NULL AND cbd.yahoo_country IS NOT NULL"
        if sector:
            where += " AND cbd.yahoo_sector = ?"
            params.append(sector)
        if country:
            where += " AND cbd.yahoo_country = ?"
            params.append(country)

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

        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')

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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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

@app.route('/data-yahoo-historico')
def data_yahoo_historico_page():
    """Dashboard de dados financeiros históricos do Yahoo Finance."""
    return render_template('data_yahoo_historico.html')


@app.route('/api/historico/summary')
def api_historico_summary():
    """Resumo geral dos dados históricos para KPI cards."""
    try:
        conn = sqlite3.connect('data/damodaran_data_new.db')
        cur = conn.cursor()
        stats = {}
        cur.execute("SELECT COUNT(*) FROM company_financials_historical")
        stats['total_records'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT yahoo_code) FROM company_financials_historical")
        stats['total_companies'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT yahoo_code) FROM company_financials_historical WHERE period_type='annual'")
        stats['annual_companies'] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT yahoo_code) FROM company_financials_historical WHERE period_type='quarterly'")
        stats['quarterly_companies'] = cur.fetchone()[0]
        cur.execute("SELECT MIN(fiscal_year), MAX(fiscal_year) FROM company_financials_historical WHERE period_type='annual'")
        r = cur.fetchone()
        stats['min_year'] = r[0]
        stats['max_year'] = r[1]
        cur.execute("SELECT COUNT(DISTINCT original_currency) FROM company_financials_historical WHERE original_currency IS NOT NULL")
        stats['distinct_currencies'] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(DISTINCT cfh.yahoo_code) 
            FROM company_financials_historical cfh
            WHERE cfh.enterprise_value_estimated IS NOT NULL
        """)
        stats['with_ev'] = cur.fetchone()[0]
        # Cobertura por setor
        cur.execute("""
            SELECT cbd.yahoo_sector, COUNT(DISTINCT cfh.yahoo_code) AS n
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cbd.yahoo_sector IS NOT NULL
            GROUP BY cbd.yahoo_sector ORDER BY n DESC
        """)
        stats['sectors'] = [{'sector': r[0], 'count': r[1]} for r in cur.fetchall()]
        # Cobertura por país
        cur.execute("""
            SELECT cbd.yahoo_country, COUNT(DISTINCT cfh.yahoo_code) AS n
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cbd.yahoo_country IS NOT NULL AND cbd.yahoo_country != ''
            GROUP BY cbd.yahoo_country ORDER BY n DESC
        """)
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


@app.route('/api/historico/search')
def api_historico_search():
    """Busca empresas com dados históricos. Params: q, sector, country, region, subregion, limit."""
    try:
        q = request.args.get('q', '').strip()
        sector = request.args.get('sector', '').strip()
        country = request.args.get('country', '').strip()
        region = request.args.get('region', '').strip()
        subregion = request.args.get('subregion', '').strip()
        limit = int(request.args.get('limit', 50))

        conn = sqlite3.connect('data/damodaran_data_new.db')
        query = """
            SELECT DISTINCT cfh.yahoo_code, cfh.company_name,
                   cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country,
                   cfh.original_currency,
                   COUNT(*) AS periods,
                   MIN(cfh.fiscal_year) AS min_year,
                   MAX(cfh.fiscal_year) AS max_year
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cfh.period_type = 'annual'
        """
        params = []
        if q:
            query += " AND (cfh.yahoo_code LIKE ? OR cfh.company_name LIKE ?)"
            params.extend([f'%{q}%', f'%{q}%'])
        if sector:
            query += " AND cbd.yahoo_sector = ?"
            params.append(sector)
        if country:
            query += " AND cbd.yahoo_country = ?"
            params.append(country)
        if region:
            # Filtrar por região usando geographic_mappings
            region_countries = [c for c, info in GEOGRAPHIC_MAPPING.items() if info['region'] == region]
            if region_countries:
                placeholders = ','.join(['?'] * len(region_countries))
                query += f" AND cbd.yahoo_country IN ({placeholders})"
                params.extend(region_countries)
        if subregion:
            sub_countries = [c for c, info in GEOGRAPHIC_MAPPING.items() if info['subregion'] == subregion]
            if sub_countries:
                placeholders = ','.join(['?'] * len(sub_countries))
                query += f" AND cbd.yahoo_country IN ({placeholders})"
                params.extend(sub_countries)
        query += " GROUP BY cfh.yahoo_code ORDER BY cfh.company_name LIMIT ?"
        params.append(limit)

        cur = conn.execute(query, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'companies': rows})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/historico/company/<yahoo_code>')
def api_historico_company(yahoo_code):
    """Retorna todos os dados históricos de uma empresa. Params: period_type (annual/quarterly)."""
    try:
        period_type = request.args.get('period_type', 'annual')
        conn = sqlite3.connect('data/damodaran_data_new.db')
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

        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        region = request.args.get('region', '')
        subregion = request.args.get('subregion', '')
        search = request.args.get('search', '')
        has_ev = request.args.get('has_ev', '')
        currency = request.args.get('currency', '')
        sort = request.args.get('sort', 'company_name')
        order = request.args.get('order', 'asc')
        page = max(1, int(request.args.get('page', 1)))
        per_page = min(100, max(10, int(request.args.get('per_page', 50))))

        conn = sqlite3.connect('data/damodaran_data_new.db')
        params = []
        conditions = ["cfh.period_type = 'annual'"]

        if sector:
            conditions.append("cbd.yahoo_sector = ?")
            params.append(sector)
        if country:
            conditions.append("cbd.yahoo_country = ?")
            params.append(country)
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
                   cfh.original_currency,
                   COUNT(*) AS periods,
                   MIN(cfh.fiscal_year) AS min_year,
                   MAX(cfh.fiscal_year) AS max_year
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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
        if not codes:
            return jsonify({'success': False, 'error': 'Nenhuma empresa selecionada'}), 400

        placeholders = ','.join(['?'] * len(codes))
        conn = sqlite3.connect('data/damodaran_data_new.db')

        # Buscar todos os registros das empresas
        query = f"""
            SELECT cfh.*, cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
            WHERE cfh.yahoo_code IN ({placeholders}) AND cfh.period_type = ?
            ORDER BY cfh.fiscal_year, cfh.yahoo_code
        """
        df = pd.read_sql_query(query, conn, params=codes + [period_type])
        conn.close()

        if df.empty:
            return jsonify({'success': True, 'years': [], 'companies': [], 'aggregated': {}, 'ranking': []})

        df = df.replace({np.nan: None})

        # Métricas para agregar
        metrics = [
            'total_revenue_usd', 'ebitda_usd', 'net_income_usd', 'free_cash_flow_usd',
            'enterprise_value_usd', 'ebitda_margin', 'ebit_margin', 'gross_margin', 'net_margin',
            'ev_ebitda', 'ev_ebit', 'ev_revenue', 'debt_equity', 'debt_ebitda',
            'fcf_revenue_ratio', 'fcf_ebitda_ratio', 'capex_revenue',
            'total_revenue', 'ebitda', 'net_income', 'free_cash_flow',
            'market_cap_estimated', 'enterprise_value_estimated',
            'total_debt', 'cash_and_equivalents', 'stockholders_equity'
        ]

        # Agregar por ano
        years = sorted(df['fiscal_year'].dropna().unique().tolist())
        aggregated = {}
        for metric in metrics:
            if metric not in df.columns:
                continue
            agg = {}
            for year in years:
                year_data = df[df['fiscal_year'] == year][metric].dropna()
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
        for code in df['yahoo_code'].unique():
            cdf = df[df['yahoo_code'] == code]
            latest = cdf.sort_values('fiscal_year', ascending=False).iloc[0]
            companies_info.append({
                'yahoo_code': code,
                'company_name': latest.get('company_name'),
                'sector': latest.get('yahoo_sector'),
                'industry': latest.get('yahoo_industry'),
                'country': latest.get('yahoo_country'),
                'currency': latest.get('original_currency'),
                'periods': int(len(cdf)),
            })

        # Ranking: última período por empresa com métricas chave
        latest_year = max(years)
        latest_df = df[df['fiscal_year'] == latest_year].copy()
        ranking = []
        for _, row in latest_df.iterrows():
            ranking.append({
                'yahoo_code': row.get('yahoo_code'),
                'company_name': row.get('company_name'),
                'sector': row.get('yahoo_sector'),
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
            })

        # Limpar NaN dos rankings
        for item in ranking:
            for k, v in item.items():
                if isinstance(v, float) and (v != v):
                    item[k] = None

        # Dados detalhados por empresa (opcional)
        detail_records = []
        if include_detail:
            detail_cols = ['yahoo_code', 'company_name', 'fiscal_year', 'original_currency',
                           'yahoo_sector', 'yahoo_industry', 'yahoo_country'] + metrics
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
        conn = sqlite3.connect('data/damodaran_data_new.db')
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

        conn.close()
        return jsonify({
            'success': True,
            'sectors': sectors,
            'regions': regions,
            'subregions': subregions,
            'countries': countries,
            'country_to_region': country_to_region,
            'years': years
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

        conn = sqlite3.connect('data/damodaran_data_new.db')
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

        query = f"""
            SELECT {group_col} AS {group_alias},
                   cfh.fiscal_year,
                   COUNT(DISTINCT cfh.company_basic_data_id) AS num_companies,
                   COUNT(*) AS num_records,
                   {metric_sql}
            FROM company_financials_historical cfh
            JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
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

        conn = sqlite3.connect('data/damodaran_data_new.db')
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

        where = ' AND '.join(conditions)

        # Total count
        count_q = f"""SELECT COUNT(*) FROM company_financials_historical cfh
                      JOIN company_basic_data cbd ON cfh.company_basic_data_id = cbd.id
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


if __name__ == '__main__':
    # Criar diretórios necessários
    Path("templates").mkdir(exist_ok=True)
    Path("static").mkdir(exist_ok=True)
    Path("static/css").mkdir(exist_ok=True)
    Path("static/js").mkdir(exist_ok=True)
    
    logger.info("Iniciando aplicação WACC Calculator")
    
    # Executar aplicação
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True,
        threaded=True
    )