#!/usr/bin/env python3
"""
Extrator de dados do Professor Aswath Damodaran (NYU Stern).
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import json
import io
from .base_extractor import BaseExtractor

class DamodaranExtractor(BaseExtractor):
    """Extrator para dados do Professor Damodaran (NYU Stern)."""
    
    def __init__(self):
        """Inicializa o extrator Damodaran."""
        super().__init__(
            name="Damodaran",
            base_url="https://pages.stern.nyu.edu/~adamodar",
            cache_duration=86400  # 24 horas
        )
        
        # URLs específicas dos datasets do Damodaran
        self.data_urls = {
            'country_risk': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html',
            'industry_betas': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/Betas.html',
            'market_risk_premium': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/histretSP.html',
            'cost_of_capital': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/wacc.html',
            'size_premium': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/spearn.html',
            'global_companies': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/globfirm.html'
        }
        
        # URLs diretas para arquivos Excel/CSV
        self.direct_data_urls = {
            'country_risk_excel': 'https://pages.stern.nyu.edu/~adamodar/pc/datasets/ctryprem.xls',
            'industry_betas_excel': 'https://pages.stern.nyu.edu/~adamodar/pc/datasets/betas.xls',
            'market_risk_premium_excel': 'https://pages.stern.nyu.edu/~adamodar/pc/datasets/histretSP.xls',
            'cost_of_capital_excel': 'https://pages.stern.nyu.edu/~adamodar/pc/datasets/wacc.xls',
            'size_premium_excel': 'https://pages.stern.nyu.edu/~adamodar/pc/datasets/spearn.xls'
        }
    
    def extract_data(self, data_type: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados de um tipo específico do Damodaran."""
        cache_key = f"damodaran_{data_type}"
        
        # Verificar cache primeiro
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            if data_type in self.direct_data_urls:
                data = self._extract_excel_data(data_type, **kwargs)
            else:
                data = self._extract_html_data(data_type, **kwargs)
            
            if self.validate_data(data):
                self._save_to_cache(cache_key, data)
                return data
            else:
                raise ValueError("Dados inválidos extraídos")
                
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados {data_type}: {e}")
            raise
    
    def _extract_excel_data(self, data_type: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados de arquivos Excel do Damodaran."""
        excel_key = f"{data_type}_excel"
        if excel_key not in self.direct_data_urls:
            raise ValueError(f"Tipo de dados Excel não suportado: {data_type}")
        
        url = self.direct_data_urls[excel_key]
        
        try:
            # Fazer download do arquivo Excel
            response = self._retry_request(url)
            
            # Ler Excel em memória
            excel_data = pd.read_excel(io.BytesIO(response.content), sheet_name=None)
            
            # Processar baseado no tipo de dados
            if data_type == 'country_risk':
                return self._process_country_risk_data(excel_data)
            elif data_type == 'industry_betas':
                return self._process_industry_betas_data(excel_data)
            elif data_type == 'market_risk_premium':
                return self._process_market_risk_premium_data(excel_data)
            elif data_type == 'cost_of_capital':
                return self._process_cost_of_capital_data(excel_data)
            elif data_type == 'size_premium':
                return self._process_size_premium_data(excel_data)
            else:
                return self._process_generic_excel_data(excel_data, data_type)
                
        except Exception as e:
            self.logger.error(f"Erro ao processar Excel {data_type}: {e}")
            raise
    
    def _process_country_risk_data(self, excel_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Processa dados de risco país."""
        # Assumindo que os dados estão na primeira sheet
        sheet_name = list(excel_data.keys())[0]
        df = excel_data[sheet_name]
        
        # Procurar pelo Brasil
        brazil_data = None
        for idx, row in df.iterrows():
            if any('brazil' in str(cell).lower() for cell in row if pd.notna(cell)):
                brazil_data = row
                break
        
        # Extrair dados gerais
        countries_data = []
        for idx, row in df.iterrows():
            if pd.notna(row.iloc[0]) and isinstance(row.iloc[0], str):
                country_info = {
                    'country': row.iloc[0],
                    'data': row.to_dict()
                }
                countries_data.append(country_info)
        
        return {
            'data_type': 'country_risk',
            'brazil_risk': brazil_data.to_dict() if brazil_data is not None else None,
            'all_countries': countries_data[:50],  # Limitar para evitar dados excessivos
            'total_countries': len(countries_data),
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Country Risk',
            'sheet_names': list(excel_data.keys())
        }
    
    def _process_industry_betas_data(self, excel_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Processa dados de betas setoriais."""
        sheet_name = list(excel_data.keys())[0]
        df = excel_data[sheet_name]
        
        # Extrair dados setoriais
        industries_data = []
        for idx, row in df.iterrows():
            if pd.notna(row.iloc[0]) and isinstance(row.iloc[0], str):
                industry_info = {
                    'industry': row.iloc[0],
                    'data': row.to_dict()
                }
                industries_data.append(industry_info)
        
        return {
            'data_type': 'industry_betas',
            'industries': industries_data,
            'total_industries': len(industries_data),
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Industry Betas',
            'sheet_names': list(excel_data.keys())
        }
    
    def _process_market_risk_premium_data(self, excel_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Processa dados de prêmio de risco de mercado."""
        sheet_name = list(excel_data.keys())[0]
        df = excel_data[sheet_name]
        
        # Extrair dados históricos
        historical_data = []
        for idx, row in df.iterrows():
            if pd.notna(row.iloc[0]):
                year_data = {
                    'year': row.iloc[0],
                    'data': row.to_dict()
                }
                historical_data.append(year_data)
        
        # Obter dados mais recentes
        latest_data = historical_data[-1] if historical_data else None
        
        return {
            'data_type': 'market_risk_premium',
            'latest_data': latest_data,
            'historical_data': historical_data[-10:],  # Últimos 10 anos
            'total_years': len(historical_data),
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Market Risk Premium',
            'sheet_names': list(excel_data.keys())
        }
    
    def _process_cost_of_capital_data(self, excel_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Processa dados de custo de capital."""
        sheet_name = list(excel_data.keys())[0]
        df = excel_data[sheet_name]
        
        # Extrair dados setoriais de custo de capital
        sectors_data = []
        for idx, row in df.iterrows():
            if pd.notna(row.iloc[0]) and isinstance(row.iloc[0], str):
                sector_info = {
                    'sector': row.iloc[0],
                    'data': row.to_dict()
                }
                sectors_data.append(sector_info)
        
        return {
            'data_type': 'cost_of_capital',
            'sectors': sectors_data,
            'total_sectors': len(sectors_data),
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Cost of Capital',
            'sheet_names': list(excel_data.keys())
        }
    
    def _process_size_premium_data(self, excel_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Processa dados de prêmio de tamanho."""
        sheet_name = list(excel_data.keys())[0]
        df = excel_data[sheet_name]
        
        # Extrair dados de prêmio por tamanho
        size_data = []
        for idx, row in df.iterrows():
            if pd.notna(row.iloc[0]):
                size_info = {
                    'size_category': row.iloc[0],
                    'data': row.to_dict()
                }
                size_data.append(size_info)
        
        return {
            'data_type': 'size_premium',
            'size_categories': size_data,
            'total_categories': len(size_data),
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Size Premium',
            'sheet_names': list(excel_data.keys())
        }
    
    def _process_generic_excel_data(self, excel_data: Dict[str, pd.DataFrame], data_type: str) -> Dict[str, Any]:
        """Processa dados Excel genéricos."""
        processed_sheets = {}
        
        for sheet_name, df in excel_data.items():
            processed_sheets[sheet_name] = {
                'shape': df.shape,
                'columns': df.columns.tolist(),
                'sample_data': df.head(5).to_dict('records')
            }
        
        return {
            'data_type': data_type,
            'sheets': processed_sheets,
            'extracted_at': datetime.now().isoformat(),
            'source': f'Damodaran {data_type}'
        }
    
    def _extract_html_data(self, data_type: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados de páginas HTML do Damodaran."""
        if data_type not in self.data_urls:
            raise ValueError(f"Tipo de dados HTML não suportado: {data_type}")
        
        url = self.data_urls[data_type]
        
        try:
            response = self._retry_request(url)
            
            # Tentar extrair tabelas HTML
            tables = pd.read_html(response.text)
            
            processed_tables = []
            for i, table in enumerate(tables):
                processed_tables.append({
                    'table_index': i,
                    'shape': table.shape,
                    'columns': table.columns.tolist(),
                    'sample_data': table.head(5).to_dict('records')
                })
            
            return {
                'data_type': data_type,
                'tables': processed_tables,
                'total_tables': len(tables),
                'extracted_at': datetime.now().isoformat(),
                'source': f'Damodaran {data_type} HTML',
                'url': url
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao processar HTML {data_type}: {e}")
            raise
    
    def get_brazil_country_risk(self) -> Dict[str, Any]:
        """Obtém o risco país do Brasil."""
        data = self.extract_data('country_risk')
        
        brazil_risk = data.get('brazil_risk')
        if not brazil_risk:
            # Tentar encontrar Brasil nos dados gerais
            for country in data.get('all_countries', []):
                if 'brazil' in country['country'].lower():
                    brazil_risk = country['data']
                    break
        
        return {
            'country': 'Brazil',
            'risk_data': brazil_risk,
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Country Risk'
        }
    
    def get_industry_beta(self, industry: str) -> Dict[str, Any]:
        """Obtém o beta de uma indústria específica."""
        data = self.extract_data('industry_betas')
        
        industry_data = None
        for ind in data.get('industries', []):
            if industry.lower() in ind['industry'].lower():
                industry_data = ind
                break
        
        return {
            'industry': industry,
            'beta_data': industry_data,
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Industry Betas'
        }
    
    def get_market_risk_premium(self) -> Dict[str, Any]:
        """Obtém o prêmio de risco de mercado."""
        data = self.extract_data('market_risk_premium')
        
        return {
            'market_risk_premium': data.get('latest_data'),
            'historical_data': data.get('historical_data'),
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Market Risk Premium'
        }
    
    def get_size_premium(self, company_size: str = 'small') -> Dict[str, Any]:
        """Obtém o prêmio de tamanho."""
        data = self.extract_data('size_premium')
        
        size_data = None
        for size_cat in data.get('size_categories', []):
            if company_size.lower() in str(size_cat['size_category']).lower():
                size_data = size_cat
                break
        
        return {
            'company_size': company_size,
            'size_premium_data': size_data,
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Size Premium'
        }
    
    def get_wacc_components_for_sector(self, sector: str) -> Dict[str, Any]:
        """Obtém todos os componentes WACC para um setor específico."""
        components = {}
        
        try:
            # Beta setorial
            beta_data = self.get_industry_beta(sector)
            components['beta'] = beta_data
            
            # Custo de capital setorial
            cost_data = self.extract_data('cost_of_capital')
            sector_cost = None
            for sect in cost_data.get('sectors', []):
                if sector.lower() in sect['sector'].lower():
                    sector_cost = sect
                    break
            components['cost_of_capital'] = sector_cost
            
            # Prêmio de risco de mercado
            market_risk = self.get_market_risk_premium()
            components['market_risk_premium'] = market_risk
            
            # Risco país (Brasil)
            country_risk = self.get_brazil_country_risk()
            components['country_risk'] = country_risk
            
        except Exception as e:
            self.logger.error(f"Erro ao obter componentes WACC para {sector}: {e}")
        
        return {
            'sector': sector,
            'wacc_components': components,
            'extracted_at': datetime.now().isoformat(),
            'source': 'Damodaran Multiple Sources'
        }
    
    def get_latest_data(self, data_type: str) -> Dict[str, Any]:
        """Obtém os dados mais recentes para um tipo específico."""
        return self.extract_data(data_type)
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """Valida dados específicos do Damodaran."""
        if not super().validate_data(data):
            return False
        
        # Validações específicas do Damodaran
        if 'data_type' not in data:
            self.logger.error("Tipo de dados não especificado")
            return False
        
        if 'source' not in data or 'damodaran' not in data['source'].lower():
            self.logger.error("Fonte Damodaran não identificada")
            return False
        
        return True
    
    def __str__(self) -> str:
        return "Damodaran Extractor (NYU Stern)"