from flask import Flask, render_template, request, jsonify
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import json
from geographic_mappings import (
    get_geographic_hierarchy, 
    get_industry_hierarchy, 
    get_country_region, 
    get_industry_sector
)
from field_categories_manager import FieldCategoriesManager

app = Flask(__name__)

class CompanyAnalyzer:
    def __init__(self, db_path='data/damodaran_data_new.db'):
        self.db_path = db_path
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def get_unique_values(self, column):
        """Retorna valores únicos de uma coluna"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT {column} FROM damodaran_global WHERE {column} IS NOT NULL ORDER BY {column}")
        values = [row[0] for row in cursor.fetchall()]
        conn.close()
        return values
    
    def get_market_cap_stats(self):
        """Retorna estatísticas de market cap"""
        conn = self.get_connection()
        df = pd.read_sql_query("SELECT market_cap FROM damodaran_global WHERE market_cap IS NOT NULL", conn)
        conn.close()
        
        if df.empty:
            return {'min': 0, 'max': 1000000, 'median': 1000}
        
        return {
            'min': float(df['market_cap'].min()),
            'max': float(df['market_cap'].max()),
            'median': float(df['market_cap'].median())
        }
    
    def get_countries(self):
        """Retorna lista de países disponíveis"""
        return self.get_unique_values('country')
    
    def get_industries(self):
        """Retorna lista de setores disponíveis"""
        return self.get_unique_values('industry')
    
    def get_filters(self):
        """Retorna filtros disponíveis incluindo hierarquias geográficas e setoriais"""
        try:
            # Filtros básicos existentes
            countries = self.get_unique_values('country')
            industries = self.get_unique_values('industry')
            
            # Hierarquias geográficas e setoriais
            geographic_hierarchy = get_geographic_hierarchy()
            industry_hierarchy = get_industry_hierarchy()
            
            # Calcular estatísticas de market cap para ranges
            market_cap_stats = self.get_market_cap_stats()
            
            return {
                'countries': countries,
                'industries': industries,
                'geographic_hierarchy': geographic_hierarchy,
                'industry_hierarchy': industry_hierarchy,
                'market_cap_ranges': {
                    'min': market_cap_stats['min'],
                    'max': market_cap_stats['max'],
                    'median': market_cap_stats['median'],
                    'suggested_ranges': [
                        {'label': 'Micro Cap (< $300M)', 'min': 0, 'max': 300},
                        {'label': 'Small Cap ($300M - $2B)', 'min': 300, 'max': 2000},
                        {'label': 'Mid Cap ($2B - $10B)', 'min': 2000, 'max': 10000},
                        {'label': 'Large Cap ($10B - $200B)', 'min': 10000, 'max': 200000},
                        {'label': 'Mega Cap (> $200B)', 'min': 200000, 'max': market_cap_stats['max']}
                    ]
                }
            }
        except Exception as e:
            print(f"Erro ao obter filtros: {e}")
            return {
                'countries': [],
                'industries': [],
                'geographic_hierarchy': {},
                'industry_hierarchy': {},
                'market_cap_ranges': {'min': 0, 'max': 1000000, 'median': 1000}
            }
    
    def get_companies_data(self, filters=None):
        """Retorna dados das empresas com filtros aplicados, incluindo filtros hierárquicos"""
        try:
            # Query base
            query = """
            SELECT 
                company_name, ticker, country, industry, market_cap, enterprise_value,
                revenue, net_income, ebitda, pe_ratio, beta, debt_equity, roe, roa,
                dividend_yield, revenue_growth, operating_margin
            FROM damodaran_global 
            WHERE 1=1
            """
            
            params = []
            
            if filters:
                # Filtro por país específico
                if filters.get('country'):
                    query += " AND country = ?"
                    params.append(filters['country'])
                
                # Filtro por região geográfica
                elif filters.get('region'):
                    region_countries = []
                    geographic_hierarchy = get_geographic_hierarchy()
                    
                    if filters['region'] in geographic_hierarchy:
                        for subregion_countries in geographic_hierarchy[filters['region']].values():
                            region_countries.extend(subregion_countries)
                    
                    if region_countries:
                        placeholders = ','.join(['?' for _ in region_countries])
                        query += f" AND country IN ({placeholders})"
                        params.extend(region_countries)
                
                # Filtro por sub-região geográfica
                elif filters.get('subregion'):
                    subregion_countries = []
                    geographic_hierarchy = get_geographic_hierarchy()
                    
                    for region, subregions in geographic_hierarchy.items():
                        if filters['subregion'] in subregions:
                            subregion_countries = subregions[filters['subregion']]
                            break
                    
                    if subregion_countries:
                        placeholders = ','.join(['?' for _ in subregion_countries])
                        query += f" AND country IN ({placeholders})"
                        params.extend(subregion_countries)
                
                # Filtro por indústria específica
                if filters.get('industry'):
                    query += " AND industry = ?"
                    params.append(filters['industry'])
                
                # Filtro por setor
                elif filters.get('sector'):
                    sector_industries = []
                    industry_hierarchy = get_industry_hierarchy()
                    
                    if filters['sector'] in industry_hierarchy:
                        for subsector_industries in industry_hierarchy[filters['sector']].values():
                            sector_industries.extend(subsector_industries)
                    
                    if sector_industries:
                        placeholders = ','.join(['?' for _ in sector_industries])
                        query += f" AND industry IN ({placeholders})"
                        params.extend(sector_industries)
                
                # Filtro por subsetor
                elif filters.get('subsector'):
                    subsector_industries = []
                    industry_hierarchy = get_industry_hierarchy()
                    
                    for sector, subsectors in industry_hierarchy.items():
                        if filters['subsector'] in subsectors:
                            subsector_industries = subsectors[filters['subsector']]
                            break
                    
                    if subsector_industries:
                        placeholders = ','.join(['?' for _ in subsector_industries])
                        query += f" AND industry IN ({placeholders})"
                        params.extend(subsector_industries)
                
                # Filtros de market cap
                if filters.get('min_market_cap'):
                    query += " AND market_cap >= ?"
                    params.append(float(filters['min_market_cap']))
                
                if filters.get('max_market_cap'):
                    query += " AND market_cap <= ?"
                    params.append(float(filters['max_market_cap']))
            
            query += " ORDER BY market_cap DESC LIMIT 1000"
            
            conn = self.get_connection()
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            # Adicionar informações hierárquicas aos dados
            if not df.empty:
                df['region'] = df['country'].apply(lambda x: get_country_region(x).get('region', 'Other'))
                df['subregion'] = df['country'].apply(lambda x: get_country_region(x).get('subregion', 'Other'))
                df['sector'] = df['industry'].apply(lambda x: get_industry_sector(x).get('sector', 'Other'))
                df['subsector'] = df['industry'].apply(lambda x: get_industry_sector(x).get('subsector', 'Other'))
            
            return df
            
        except Exception as e:
            print(f"Erro ao obter dados das empresas: {e}")
            return pd.DataFrame()
    
    def calculate_benchmarks(self, df, group_by='industry'):
        """Calcula benchmarks por grupo (setor ou país)"""
        if df.empty:
            return {}
        
        # Métricas numéricas para calcular estatísticas
        numeric_cols = ['market_cap', 'enterprise_value', 'revenue', 'net_income', 
                       'ebitda', 'pe_ratio', 'beta', 'debt_equity', 'roe', 'roa',
                       'dividend_yield', 'revenue_growth', 'operating_margin']
        
        # Remove valores nulos e infinitos
        df_clean = df.copy()
        for col in numeric_cols:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # Calcula estatísticas por grupo
        benchmarks = {}
        
        if group_by in df_clean.columns:
            grouped = df_clean.groupby(group_by)
            
            for group_name, group_data in grouped:
                if len(group_data) >= 3:  # Mínimo 3 empresas para benchmark
                    group_stats = {}
                    
                    for col in numeric_cols:
                        if col in group_data.columns:
                            values = group_data[col].dropna()
                            if len(values) > 0:
                                group_stats[col] = {
                                    'mean': float(values.mean()),
                                    'median': float(values.median()),
                                    'std': float(values.std()) if len(values) > 1 else 0,
                                    'min': float(values.min()),
                                    'max': float(values.max()),
                                    'q25': float(values.quantile(0.25)),
                                    'q75': float(values.quantile(0.75)),
                                    'count': len(values)
                                }
                    
                    benchmarks[group_name] = group_stats
        
        return benchmarks
    
    def get_company_ranking(self, company_name, df, metric='market_cap'):
        """Retorna o ranking da empresa em uma métrica específica"""
        if df.empty or metric not in df.columns:
            return None
        
        df_metric = df[df[metric].notna()].copy()
        df_metric = df_metric.sort_values(metric, ascending=False).reset_index(drop=True)
        
        company_row = df_metric[df_metric['company_name'] == company_name]
        if not company_row.empty:
            rank = company_row.index[0] + 1
            total = len(df_metric)
            percentile = (total - rank) / total * 100
            return {
                'rank': rank,
                'total': total,
                'percentile': round(percentile, 1),
                'value': float(company_row[metric].iloc[0])
            }
        
        return None

# Instância global do analisador
analyzer = CompanyAnalyzer()
field_manager = FieldCategoriesManager()

@app.route('/')
def index():
    """Página inicial"""
    return render_template('company_analysis.html')


@app.route('/company/<ticker>')
def company_profile(ticker):
    """Página de perfil da empresa por ticker, incluindo campo about."""
    try:
        ticker = (ticker or '').strip()
        if not ticker:
            return render_template(
                'company_profile.html',
                company=None,
                ticker=ticker,
                error='Ticker não informado.'
            ), 400

        conn = analyzer.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = """
        SELECT
            dg.*, 
            cbd.cod_anloc,
            cbd.yahoo_code,
            cbd.about,
            cbd.etf_sector,
            cbd.updated_at AS basic_data_updated_at
        FROM damodaran_global dg
        LEFT JOIN company_basic_data cbd ON cbd.ticker = dg.ticker
        WHERE dg.ticker = ? OR dg.ticker LIKE ?
        ORDER BY CASE WHEN dg.ticker = ? THEN 0 ELSE 1 END, dg.market_cap DESC
        LIMIT 1
        """

        cursor.execute(query, (ticker, f'%:{ticker}', ticker))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return render_template(
                'company_profile.html',
                company=None,
                ticker=ticker,
                error='Empresa não encontrada para o ticker informado.'
            ), 404

        company = dict(row)

        # Normalizar valores para renderização no template
        for key, value in list(company.items()):
            if pd.isna(value):
                company[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                company[key] = float(value)

        # Tentar parsear ETF sector quando vier em JSON string
        etf_sector_list = []
        raw_etf_sector = company.get('etf_sector')
        if isinstance(raw_etf_sector, str) and raw_etf_sector.strip():
            try:
                parsed = json.loads(raw_etf_sector)
                if isinstance(parsed, list):
                    etf_sector_list = parsed
            except Exception:
                etf_sector_list = [raw_etf_sector]

        return render_template(
            'company_profile.html',
            company=company,
            ticker=ticker,
            etf_sector_list=etf_sector_list
        )

    except Exception as e:
        return render_template(
            'company_profile.html',
            company=None,
            ticker=ticker,
            error=f'Erro ao carregar empresa: {e}'
        ), 500

@app.route('/api/filters')
def get_filters():
    """API para obter opções de filtros"""
    try:
        filters = analyzer.get_filters()
        filters['success'] = True
        return jsonify(filters)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/get_field_categories')
def api_get_field_categories():
    """API endpoint para obter categorias de campos disponíveis."""
    try:
        categories_dict = field_manager.get_all_categories()
        categories_array = []
        for category_name, category_data in categories_dict.items():
            categories_array.append({
                'id': category_name,
                'name': category_name,
                'icon': category_data.get('icon', '📊'),
                'description': category_data.get('description', ''),
                'field_count': len(category_data.get('fields', {}))
            })

        categories_array.sort(key=lambda x: x['name'])

        return jsonify({
            'success': True,
            'categories': categories_array
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_category_fields/<category_id>')
def api_get_category_fields(category_id):
    """API endpoint para obter campos de uma categoria específica."""
    try:
        fields_dict = field_manager.get_category_fields(category_id)
        available_fields = set(field_manager.get_available_fields_from_db())
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

        fields_array.sort(key=lambda x: x['label'])

        return jsonify({
            'success': True,
            'fields': fields_array
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


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
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/companies')
def get_companies():
    """Endpoint para obter dados das empresas com filtros hierárquicos"""
    try:
        filters = {}
        
        # Filtros geográficos hierárquicos
        if request.args.get('country'):
            filters['country'] = request.args.get('country')
        elif request.args.get('subregion'):
            filters['subregion'] = request.args.get('subregion')
        elif request.args.get('region'):
            filters['region'] = request.args.get('region')
        
        # Filtros de setor hierárquicos
        if request.args.get('industry'):
            filters['industry'] = request.args.get('industry')
        elif request.args.get('subsector'):
            filters['subsector'] = request.args.get('subsector')
        elif request.args.get('sector'):
            filters['sector'] = request.args.get('sector')
        
        # Filtros de market cap
        if request.args.get('min_market_cap'):
            filters['min_market_cap'] = request.args.get('min_market_cap')
        if request.args.get('max_market_cap'):
            filters['max_market_cap'] = request.args.get('max_market_cap')
        
        df = analyzer.get_companies_data(filters)
        
        if df.empty:
            return jsonify([])

        # Garantir JSON válido: substituir NaN/NaT por None
        df = df.replace({np.nan: None})

        return jsonify(df.to_dict('records'))
        
    except Exception as e:
        print(f"Erro no endpoint /api/companies: {e}")
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
        
        df = analyzer.get_companies_data(filters)
        benchmarks = analyzer.calculate_benchmarks(df, group_by)
        
        return jsonify({
            'success': True,
            'benchmarks': benchmarks,
            'group_by': group_by
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/company/<company_name>/analysis')
def get_company_analysis(company_name):
    """API para análise detalhada de uma empresa específica"""
    try:
        # Busca dados da empresa
        df = analyzer.get_companies_data()
        company_data = df[df['company_name'] == company_name]
        
        if company_data.empty:
            return jsonify({'success': False, 'error': 'Empresa não encontrada'})
        
        company = company_data.iloc[0]
        
        # Busca benchmarks do setor
        industry_df = df[df['industry'] == company['industry']]
        industry_benchmarks = analyzer.calculate_benchmarks(industry_df, 'industry')
        
        # Busca benchmarks do país
        country_df = df[df['country'] == company['country']]
        country_benchmarks = analyzer.calculate_benchmarks(country_df, 'country')
        
        # Calcula rankings
        rankings = {}
        key_metrics = ['market_cap', 'revenue', 'net_income', 'roe', 'roa', 'pe_ratio']
        
        for metric in key_metrics:
            # Ranking no setor
            sector_ranking = analyzer.get_company_ranking(company_name, industry_df, metric)
            # Ranking no país
            country_ranking = analyzer.get_company_ranking(company_name, country_df, metric)
            # Ranking global
            global_ranking = analyzer.get_company_ranking(company_name, df, metric)
            
            rankings[metric] = {
                'sector': sector_ranking,
                'country': country_ranking,
                'global': global_ranking
            }
        
        # Converte dados da empresa para formato JSON
        company_dict = {}
        for col in company_data.columns:
            value = company[col]
            if pd.isna(value):
                company_dict[col] = None
            elif isinstance(value, (np.integer, np.floating)):
                company_dict[col] = float(value) if not np.isnan(value) else None
            else:
                company_dict[col] = str(value)
        
        return jsonify({
            'success': True,
            'company': company_dict,
            'industry_benchmarks': industry_benchmarks.get(company['industry'], {}),
            'country_benchmarks': country_benchmarks.get(company['country'], {}),
            'rankings': rankings
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, port=5001)