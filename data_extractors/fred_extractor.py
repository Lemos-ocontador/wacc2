#!/usr/bin/env python3
"""
Extrator de dados do FRED (Federal Reserve Economic Data).
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import json
from .base_extractor import BaseExtractor

class FREDExtractor(BaseExtractor):
    """Extrator para dados do Federal Reserve Economic Data (FRED)."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inicializa o extrator FRED.
        
        Args:
            api_key: Chave da API do FRED (opcional, mas recomendado)
        """
        super().__init__(
            name="FRED",
            base_url="https://api.stlouisfed.org/fred",
            cache_duration=3600  # 1 hora
        )
        self.api_key = api_key
        self.web_base_url = "https://fred.stlouisfed.org"
        
        # Séries importantes para WACC
        self.series_map = {
            'treasury_10y': 'DGS10',  # 10-Year Treasury Constant Maturity Rate
            'treasury_30y': 'DGS30',  # 30-Year Treasury Constant Maturity Rate
            'treasury_3m': 'DGS3MO',  # 3-Month Treasury Constant Maturity Rate
            'cpi': 'FPCPITOTLZGUSA',  # Consumer Price Index for All Urban Consumers
            'gdp': 'GDP',  # Gross Domestic Product
            'unemployment': 'UNRATE',  # Unemployment Rate
            'fed_funds': 'FEDFUNDS'  # Federal Funds Effective Rate
        }
    
    def extract_data(self, series_id: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados de uma série específica do FRED."""
        cache_key = f"fred_{series_id}"
        
        # Verificar cache primeiro
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            if self.api_key:
                data = self._extract_via_api(series_id, **kwargs)
            else:
                data = self._extract_via_web_scraping(series_id, **kwargs)
            
            if self.validate_data(data):
                self._save_to_cache(cache_key, data)
                return data
            else:
                raise ValueError("Dados inválidos extraídos")
                
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados da série {series_id}: {e}")
            raise
    
    def _extract_via_api(self, series_id: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados usando a API oficial do FRED."""
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json'
        }
        
        # Adicionar parâmetros opcionais
        if 'start_date' in kwargs:
            params['observation_start'] = kwargs['start_date']
        if 'end_date' in kwargs:
            params['observation_end'] = kwargs['end_date']
        if 'limit' in kwargs:
            params['limit'] = kwargs['limit']
        
        # Obter informações da série
        series_url = f"{self.base_url}/series"
        series_response = self._retry_request(series_url, params)
        series_info = series_response.json()
        
        # Obter observações da série
        obs_params = params.copy()
        obs_url = f"{self.base_url}/series/observations"
        obs_response = self._retry_request(obs_url, obs_params)
        observations = obs_response.json()
        
        if 'observations' not in observations:
            raise ValueError("Nenhuma observação encontrada")
        
        # Processar dados
        latest_obs = observations['observations'][-1]
        
        return {
            'series_id': series_id,
            'title': series_info['seriess'][0]['title'],
            'units': series_info['seriess'][0]['units'],
            'frequency': series_info['seriess'][0]['frequency'],
            'value': float(latest_obs['value']) if latest_obs['value'] != '.' else None,
            'date': latest_obs['date'],
            'last_updated': series_info['seriess'][0]['last_updated'],
            'source': 'FRED API',
            'all_observations': observations['observations'][-10:]  # Últimas 10 observações
        }
    
    def _extract_via_web_scraping(self, series_id: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados via web scraping (fallback quando não há API key)."""
        url = f"{self.web_base_url}/series/{series_id}"
        
        try:
            response = self._retry_request(url)
            
            # Aqui implementaríamos o parsing do HTML
            # Por simplicidade, vamos retornar dados mock
            self.logger.warning(f"Web scraping não totalmente implementado para {series_id}")
            
            return {
                'series_id': series_id,
                'title': f'Series {series_id}',
                'value': None,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'source': 'FRED Web Scraping',
                'note': 'Implementação de web scraping pendente'
            }
            
        except Exception as e:
            self.logger.error(f"Erro no web scraping para {series_id}: {e}")
            raise
    
    def get_latest_data(self, data_type: str) -> Dict[str, Any]:
        """Obtém os dados mais recentes para um tipo específico."""
        if data_type not in self.series_map:
            raise ValueError(f"Tipo de dados não suportado: {data_type}")
        
        series_id = self.series_map[data_type]
        return self.extract_data(series_id)
    
    def get_treasury_rates(self) -> Dict[str, Any]:
        """Obtém todas as taxas do tesouro americano."""
        rates = {}
        treasury_series = ['treasury_3m', 'treasury_10y', 'treasury_30y']
        
        for rate_type in treasury_series:
            try:
                data = self.get_latest_data(rate_type)
                rates[rate_type] = {
                    'value': data.get('value'),
                    'date': data.get('date'),
                    'title': data.get('title')
                }
            except Exception as e:
                self.logger.error(f"Erro ao obter {rate_type}: {e}")
                rates[rate_type] = None
        
        return {
            'treasury_rates': rates,
            'extracted_at': datetime.now().isoformat(),
            'source': 'FRED'
        }
    
    def get_inflation_data(self) -> Dict[str, Any]:
        """Obtém dados de inflação (CPI)."""
        return self.get_latest_data('cpi')
    
    def get_historical_data(self, data_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Obtém dados históricos para análise."""
        if data_type not in self.series_map:
            raise ValueError(f"Tipo de dados não suportado: {data_type}")
        
        series_id = self.series_map[data_type]
        
        try:
            data = self.extract_data(
                series_id,
                start_date=start_date,
                end_date=end_date
            )
            
            if 'all_observations' in data:
                df = pd.DataFrame(data['all_observations'])
                df['date'] = pd.to_datetime(df['date'])
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.dropna(subset=['value'])
                df = df.sort_values('date')
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Erro ao obter dados históricos: {e}")
            return pd.DataFrame()
    
    def calculate_risk_free_rate(self, term: str = '10y') -> Dict[str, Any]:
        """Calcula a taxa livre de risco baseada nos títulos do tesouro."""
        rate_type = f'treasury_{term}'
        
        if rate_type not in self.series_map:
            raise ValueError(f"Prazo não suportado: {term}")
        
        data = self.get_latest_data(rate_type)
        
        return {
            'risk_free_rate': data.get('value', 0) / 100,  # Converter para decimal
            'rate_percent': data.get('value'),
            'date': data.get('date'),
            'term': term,
            'source': 'US Treasury via FRED',
            'title': data.get('title')
        }
    
    def get_economic_indicators(self) -> Dict[str, Any]:
        """Obtém principais indicadores econômicos."""
        indicators = {}
        indicator_types = ['fed_funds', 'unemployment', 'cpi']
        
        for indicator in indicator_types:
            try:
                data = self.get_latest_data(indicator)
                indicators[indicator] = {
                    'value': data.get('value'),
                    'date': data.get('date'),
                    'title': data.get('title')
                }
            except Exception as e:
                self.logger.error(f"Erro ao obter {indicator}: {e}")
                indicators[indicator] = None
        
        return {
            'economic_indicators': indicators,
            'extracted_at': datetime.now().isoformat(),
            'source': 'FRED'
        }
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """Valida dados específicos do FRED."""
        if not super().validate_data(data):
            return False
        
        # Validações específicas do FRED
        if 'series_id' not in data:
            self.logger.error("Series ID não encontrado")
            return False
        
        if data.get('value') is not None and not isinstance(data['value'], (int, float)):
            self.logger.error("Valor deve ser numérico")
            return False
        
        return True
    
    def __str__(self) -> str:
        api_status = "with API" if self.api_key else "without API"
        return f"FRED Extractor ({api_status})"