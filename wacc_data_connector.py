import sqlite3
import json
import pandas as pd
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

class WACCDataConnector:
    """
    Conector para dados WACC integrados.
    Conecta com bancos de dados SQLite e arquivos JSON para fornecer
    todos os componentes necessários para cálculo do WACC.
    """
    
    def __init__(self, 
                 damodaran_db_path: str = "data/damodaran_data_new.db",
                 country_risk_db_path: str = "data/damodaran_data_new.db",
                 wacc_json_path: str = "BDWACC.json"):
        """
        Inicializar o conector WACC.
        
        Args:
            damodaran_db_path: Caminho para banco de dados Damodaran global
            country_risk_db_path: Caminho para banco de dados de risco país
            wacc_json_path: Caminho para arquivo JSON com componentes WACC
        """
        self.damodaran_db = damodaran_db_path
        self.country_risk_db = country_risk_db_path
        self.wacc_json = wacc_json_path
        
        # Cache para dados frequentemente acessados
        self._wacc_components_cache = None
        self._sectors_cache = None
        self._countries_cache = None
        
        logger.info("WACCDataConnector inicializado")
    
    def _load_wacc_components(self) -> Dict[str, Any]:
        """
        Carregar componentes WACC do arquivo JSON.
        
        Returns:
            Dict com componentes WACC
        """
        if self._wacc_components_cache is None:
            try:
                with open(self.wacc_json, 'r', encoding='utf-8') as f:
                    self._wacc_components_cache = json.load(f)
                logger.info("Componentes WACC carregados do JSON")
            except Exception as e:
                logger.error(f"Erro ao carregar componentes WACC: {e}")
                self._wacc_components_cache = {}
        
        return self._wacc_components_cache
    
    def get_risk_free_rate_options(self) -> Dict[str, Any]:
        """
        Obter opções disponíveis para taxa livre de risco.
        
        Returns:
            Dict com opções de taxa livre de risco
        """
        try:
            wacc_data = self._load_wacc_components()
            
            # Extrair taxa livre de risco do JSON
            rf_rate = wacc_data.get('RF', 4.14)  # Default fallback
            
            return {
                'success': True,
                'options': [
                    {
                        'id': '10y',
                        'name': 'US Treasury 10Y',
                        'description': 'Taxa do Tesouro Americano 10 anos',
                        'current_rate': rf_rate,
                        'source': 'FRED/Damodaran'
                    },
                    {
                        'id': '30y',
                        'name': 'US Treasury 30Y',
                        'description': 'Taxa do Tesouro Americano 30 anos',
                        'current_rate': rf_rate + 0.5,  # Aproximação
                        'source': 'FRED/Damodaran'
                    },
                    {
                        'id': 'custom',
                        'name': 'Taxa Personalizada',
                        'description': 'Inserir taxa manualmente',
                        'current_rate': None,
                        'source': 'Manual'
                    }
                ],
                'default': '10y',
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter opções de taxa livre de risco: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_risk_free_rate(self, term: str = '10y') -> Dict[str, Any]:
        """
        Obter taxa livre de risco específica.
        
        Args:
            term: Prazo da taxa (10y, 30y)
        
        Returns:
            Dict com taxa livre de risco
        """
        try:
            wacc_data = self._load_wacc_components()
            rf_rate = wacc_data.get('RF', 4.14)
            
            # Ajustar taxa baseado no prazo
            if term == '30y':
                rate = rf_rate + 0.5  # Aproximação para 30Y
            else:
                rate = rf_rate  # 10Y é o padrão
            
            return {
                'success': True,
                'term': term,
                'rate_decimal': rate / 100,
                'rate_percentage': rate,
                'source': 'FRED/Damodaran',
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter taxa livre de risco: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_available_sectors(self) -> Dict[str, Any]:
        """
        Obter setores disponíveis para cálculo de beta.
        
        Returns:
            Dict com lista de setores
        """
        if self._sectors_cache is not None:
            return self._sectors_cache
        
        try:
            conn = sqlite3.connect(self.damodaran_db)
            
            query = """
            SELECT 
                industry,
                COUNT(*) as company_count,
                AVG(CASE WHEN beta IS NOT NULL AND beta != '' AND beta != 'None' 
                    THEN CAST(beta as REAL) ELSE NULL END) as avg_beta,
                AVG(CASE WHEN debt_equity IS NOT NULL AND debt_equity != '' AND debt_equity != 'None' 
                    THEN CAST(debt_equity as REAL) ELSE NULL END) as avg_debt_equity
            FROM damodaran_global 
            WHERE industry IS NOT NULL
                AND industry != ''
                AND industry != 'None'
            GROUP BY industry
            HAVING COUNT(*) >= 3
                AND AVG(CASE WHEN beta IS NOT NULL AND beta != '' AND beta != 'None' 
                    THEN CAST(beta as REAL) ELSE NULL END) IS NOT NULL
            ORDER BY company_count DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            sectors = []
            for _, row in df.iterrows():
                avg_beta = round(float(row['avg_beta']), 3) if pd.notna(row['avg_beta']) else None
                sectors.append({
                    'sector': row['industry'],
                    'value': row['industry'],
                    'label': row['industry'],
                    'company_count': int(row['company_count']),
                    'companies_count': int(row['company_count']),
                    'avg_beta': avg_beta,
                    'avg_debt_equity': round(float(row['avg_debt_equity']), 3) if pd.notna(row['avg_debt_equity']) else None
                })
            
            result = {
                'success': True,
                'sectors': sectors,
                'total_sectors': len(sectors),
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
            self._sectors_cache = result
            logger.info(f"Carregados {len(sectors)} setores")
            return result
            
        except Exception as e:
            logger.error(f"Erro ao obter setores: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_sector_beta(self, sector: str, region: str = 'global') -> Dict[str, Any]:
        """
        Obter beta de um setor específico.
        
        Args:
            sector: Nome do setor
            region: Região (global, emkt)
        
        Returns:
            Dict com beta alavancado e desalavancado
        """
        try:
            conn = sqlite3.connect(self.damodaran_db)
            
            # Query base
            base_query = """
            SELECT 
                CAST(beta as REAL) as beta,
                CAST(debt_equity as REAL) as debt_equity,
                country
            FROM damodaran_global 
            WHERE industry = ?
                AND beta IS NOT NULL 
                AND beta != '' 
                AND CAST(beta as REAL) > 0
            """
            
            # Filtrar por região se necessário
            if region == 'emkt':
                # Países emergentes (lista simplificada)
                emkt_countries = ['Brazil', 'Mexico', 'Argentina', 'Chile', 'Colombia', 
                                'India', 'China', 'South Korea', 'Taiwan', 'Thailand',
                                'Malaysia', 'Indonesia', 'Philippines', 'Turkey', 'Russia',
                                'South Africa', 'Egypt', 'Poland', 'Czech Republic']
                
                placeholders = ','.join(['?' for _ in emkt_countries])
                query = base_query + f" AND country IN ({placeholders})"
                params = [sector] + emkt_countries
            else:
                query = base_query
                params = [sector]
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            if df.empty:
                return {
                    'success': False,
                    'error': f'Setor "{sector}" não encontrado na região "{region}"'
                }
            
            # Calcular estatísticas
            levered_beta = df['beta'].mean()
            avg_debt_equity = df['debt_equity'].mean() if df['debt_equity'].notna().any() else 0.3
            
            # Calcular beta desalavancado
            # Assumir taxa de imposto padrão de 34%
            tax_rate = 0.34
            unlevered_beta = levered_beta / (1 + (1 - tax_rate) * avg_debt_equity)
            
            return {
                'success': True,
                'sector': sector,
                'region': region,
                'levered_beta': round(levered_beta, 4),
                'unlevered_beta': round(unlevered_beta, 4),
                'avg_debt_equity': round(avg_debt_equity, 4),
                'company_count': len(df),
                'tax_rate_used': tax_rate,
                'formula': 'βU = βL / [1 + (1 - T) × (D/E)]',
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter beta do setor: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_available_countries(self) -> Dict[str, Any]:
        """
        Obter países disponíveis para prêmio de risco.
        
        Returns:
            Dict com lista de países
        """
        if self._countries_cache is not None:
            return self._countries_cache
        
        try:
            conn = sqlite3.connect(self.country_risk_db)
            
            query = """
            SELECT 
                country,
                CAST(risk_premium as REAL) as risk_premium
            FROM country_risk 
            WHERE risk_premium IS NOT NULL 
                AND risk_premium != ''
                AND CAST(risk_premium as REAL) >= 0
            ORDER BY country
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            countries = []
            for _, row in df.iterrows():
                countries.append({
                    'country': row['country'],
                    'risk_premium_decimal': round(float(row['risk_premium']) / 100, 4),
                    'risk_premium_percentage': round(float(row['risk_premium']), 2)
                })
            
            result = {
                'success': True,
                'countries': countries,
                'total_countries': len(countries),
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
            self._countries_cache = result
            logger.info(f"Carregados {len(countries)} países")
            return result
            
        except Exception as e:
            logger.error(f"Erro ao obter países: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_country_risk(self, country: str) -> Dict[str, Any]:
        """
        Obter prêmio de risco de um país específico.
        
        Args:
            country: Nome do país
        
        Returns:
            Dict com prêmio de risco do país
        """
        try:
            conn = sqlite3.connect(self.country_risk_db)
            
            query = """
            SELECT 
                country,
                CAST(risk_premium as REAL) as risk_premium,
                created_at
            FROM country_risk 
            WHERE country = ?
                AND risk_premium IS NOT NULL 
                AND risk_premium != ''
            """
            
            df = pd.read_sql_query(query, conn, params=[country])
            conn.close()
            
            if df.empty:
                return {
                    'success': False,
                    'error': f'País "{country}" não encontrado'
                }
            
            risk_premium = float(df.iloc[0]['risk_premium'])
            
            return {
                'success': True,
                'country': country,
                'risk_premium_decimal': round(risk_premium, 4),
                'risk_premium_percentage': round(risk_premium * 100, 2),
                'source': 'Damodaran Country Risk',
                'last_updated': df.iloc[0]['created_at'] if 'created_at' in df.columns else pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter risco do país: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_market_risk_premium(self) -> Dict[str, Any]:
        """
        Obter prêmio de risco de mercado.
        
        Returns:
            Dict com prêmio de risco de mercado (ERP)
        """
        try:
            wacc_data = self._load_wacc_components()
            
            # Extrair prêmio de risco de mercado do JSON
            market_risk_premium = wacc_data.get('RM', 4.61)  # Default fallback
            
            return {
                'success': True,
                'market_risk_premium_decimal': round(market_risk_premium / 100, 4),
                'market_risk_premium_percentage': round(market_risk_premium, 2),
                'source': 'Damodaran ERP',
                'methodology': 'Historical US Market Premium',
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter prêmio de risco de mercado: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_wacc_components(self, sector: str, country: str = 'Brazil', region: str = 'global') -> Dict[str, Any]:
        """
        Obter todos os componentes WACC de uma vez.
        
        Args:
            sector: Setor da empresa
            country: País da empresa
            region: Região para beta (global, emkt)
        
        Returns:
            Dict com todos os componentes WACC
        """
        try:
            # Obter todos os componentes
            rf_data = self.get_risk_free_rate('10y')
            beta_data = self.get_sector_beta(sector, region)
            country_risk_data = self.get_country_risk(country)
            market_risk_data = self.get_market_risk_premium()
            
            # Carregar outros componentes do JSON
            wacc_data = self._load_wacc_components()
            
            # Verificar se todos os componentes foram obtidos com sucesso
            if not all([rf_data['success'], beta_data['success'], 
                       country_risk_data['success'], market_risk_data['success']]):
                errors = []
                if not rf_data['success']: errors.append(f"Taxa livre de risco: {rf_data.get('error', 'Erro desconhecido')}")
                if not beta_data['success']: errors.append(f"Beta: {beta_data.get('error', 'Erro desconhecido')}")
                if not country_risk_data['success']: errors.append(f"Risco país: {country_risk_data.get('error', 'Erro desconhecido')}")
                if not market_risk_data['success']: errors.append(f"Prêmio de mercado: {market_risk_data.get('error', 'Erro desconhecido')}")
                
                return {
                    'success': False,
                    'error': 'Erro ao obter componentes: ' + '; '.join(errors)
                }
            
            # Extrair valores
            risk_free_rate = rf_data['rate_percentage']
            beta = beta_data['unlevered_beta']  # Usar beta desalavancado
            market_risk_premium = market_risk_data['market_risk_premium_percentage']
            country_risk_premium = country_risk_data['risk_premium_percentage']
            
            # Outros componentes do JSON
            tax_rate = wacc_data.get('IR', 34.0)
            cost_of_debt = wacc_data.get('CT', 13.5)
            
            # Calcular custo do patrimônio
            # Ke = Rf + β × (Rm + Rp)
            cost_of_equity = risk_free_rate + beta * (market_risk_premium + country_risk_premium)
            
            # Para WACC simplificado (assumindo 100% patrimônio)
            wacc = cost_of_equity
            
            return {
                'success': True,
                'components': {
                    'risk_free_rate': {
                        'value_percentage': risk_free_rate,
                        'value_decimal': risk_free_rate / 100,
                        'source': 'FRED/Damodaran'
                    },
                    'beta': {
                        'levered_beta': beta_data['levered_beta'],
                        'unlevered_beta': beta,
                        'sector': sector,
                        'region': region,
                        'company_count': beta_data['company_count'],
                        'source': 'Damodaran Global'
                    },
                    'market_risk_premium': {
                        'value_percentage': market_risk_premium,
                        'value_decimal': market_risk_premium / 100,
                        'source': 'Damodaran ERP'
                    },
                    'country_risk_premium': {
                        'value_percentage': country_risk_premium,
                        'value_decimal': country_risk_premium / 100,
                        'country': country,
                        'source': 'Damodaran Country Risk'
                    },
                    'tax_rate': {
                        'value_percentage': tax_rate,
                        'value_decimal': tax_rate / 100,
                        'source': 'BDWACC.json'
                    },
                    'cost_of_debt': {
                        'value_percentage': cost_of_debt,
                        'value_decimal': cost_of_debt / 100,
                        'source': 'BDWACC.json'
                    }
                },
                'calculated': {
                    'cost_of_equity_percentage': round(cost_of_equity, 2),
                    'cost_of_equity_decimal': round(cost_of_equity / 100, 4),
                    'wacc_percentage': round(wacc, 2),
                    'wacc_decimal': round(wacc / 100, 4),
                    'formula': 'Ke = Rf + β × (Rm + Rp)'
                },
                'metadata': {
                    'sector': sector,
                    'country': country,
                    'region': region,
                    'calculation_date': pd.Timestamp.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter componentes WACC: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_size_premium(self, market_cap: float = None) -> Dict[str, Any]:
        """
        Obter prêmio de tamanho baseado no valor de mercado da empresa.
        
        Args:
            market_cap: Valor de mercado da empresa em reais
        
        Returns:
            Dict com prêmio de tamanho aplicável
        """
        try:
            conn = sqlite3.connect(self.damodaran_db)
            
            if market_cap is None:
                # Retornar todos os decis disponíveis
                query = """
                SELECT 
                    size_decile,
                    market_cap_min,
                    market_cap_max,
                    premium_decimal,
                    premium_percentage,
                    reference_year
                FROM size_premium 
                ORDER BY size_decile
                """
                
                df = pd.read_sql_query(query, conn)
                conn.close()
                
                if df.empty:
                    return {
                        'success': False,
                        'error': 'Dados de size premium não encontrados'
                    }
                
                size_data = []
                for _, row in df.iterrows():
                    size_data.append({
                        'decile': int(row['size_decile']),
                        'market_cap_min': float(row['market_cap_min']),
                        'market_cap_max': float(row['market_cap_max']),
                        'premium_decimal': float(row['premium_decimal']),
                        'premium_percentage': float(row['premium_decimal'] * 100),
                        'premium_display': row['premium_percentage']
                    })
                
                return {
                    'success': True,
                    'size_premiums': size_data,
                    'reference_year': int(df.iloc[0]['reference_year']),
                    'source': 'BDSize.json',
                    'total_deciles': len(size_data)
                }
            
            else:
                # Encontrar o decil apropriado para o valor de mercado fornecido
                query = """
                SELECT 
                    size_decile,
                    market_cap_min,
                    market_cap_max,
                    premium_decimal,
                    premium_percentage,
                    reference_year
                FROM size_premium 
                WHERE ? >= market_cap_min AND ? <= market_cap_max
                """
                
                df = pd.read_sql_query(query, conn, params=[market_cap, market_cap])
                conn.close()
                
                if df.empty:
                    return {
                        'success': False,
                        'error': f'Nenhum decil encontrado para valor de mercado: R$ {market_cap:,.0f}'
                    }
                
                row = df.iloc[0]
                
                return {
                    'success': True,
                    'market_cap': market_cap,
                    'size_decile': int(row['size_decile']),
                    'market_cap_range': {
                        'min': float(row['market_cap_min']),
                        'max': float(row['market_cap_max'])
                    },
                    'size_premium': {
                        'decimal': float(row['premium_decimal']),
                        'percentage': float(row['premium_decimal'] * 100),
                        'display': row['premium_percentage']
                    },
                    'reference_year': int(row['reference_year']),
                    'source': 'BDSize.json'
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter size premium: {e}")
            return {
                'success': False,
                'error': str(e)
            }