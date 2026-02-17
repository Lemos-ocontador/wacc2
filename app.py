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
               cbd.about
        FROM damodaran_global dg
        LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
        WHERE dg.industry = ?
          AND dg.beta IS NOT NULL AND dg.beta != ''
          AND CAST(dg.beta AS REAL) > 0
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
        
        # Calcular beta desalavancado para cada empresa
        tax_rate = 0.34
        df['unlevered_beta'] = df.apply(
            lambda r: round(r['beta'] / (1 + (1 - tax_rate) * r['debt_equity']), 4)
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
        tax_rate = data.get('tax_rate', 0.34)
        
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
               cbd.about
        FROM damodaran_global dg
        LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
        WHERE dg.ticker IN ({placeholders})
          AND dg.beta IS NOT NULL AND CAST(dg.beta AS REAL) > 0
        """
        df = pd.read_sql_query(query, conn, params=tickers)
        conn.close()
        
        if df.empty:
            return jsonify({'success': False, 'error': 'Nenhuma empresa encontrada'})
        
        # Calcular beta desalavancado
        df['debt_equity'] = df['debt_equity'].fillna(0).clip(lower=0)
        df['unlevered_beta'] = df['beta'] / (1 + (1 - tax_rate) * df['debt_equity'])
        
        if method == 'weighted' and df['market_cap'].sum() > 0:
            total_mc = df['market_cap'].sum()
            df['weight'] = df['market_cap'] / total_mc
            avg_bu = (df['unlevered_beta'] * df['weight']).sum()
            avg_de = (df['debt_equity'] * df['weight']).sum()
            avg_bl = (df['beta'] * df['weight']).sum()
        else:
            df['weight'] = 1.0 / len(df)
            avg_bu = df['unlevered_beta'].mean()
            avg_de = df['debt_equity'].mean()
            avg_bl = df['beta'].mean()
        
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
                'weight': round(row['weight'], 4),
            })
        
        return jsonify({
            'success': True,
            'benchmark': {
                'unlevered_beta': round(avg_bu, 4),
                'levered_beta_avg': round(avg_bl, 4),
                'debt_equity_avg': round(avg_de, 4),
                'companies_used': len(df),
                'method': method,
                'tax_rate': tax_rate,
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
        
        conn.close()
        
        return jsonify({
            'success': True,
            'countries': countries,
            'industries': industries,
            'geographic_hierarchy': geographic_hierarchy,
            'industry_hierarchy': industry_hierarchy
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


@app.route('/api/get_category_fields/<category_id>')
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