#!/usr/bin/env python3
"""
Extrator de dados do Banco Central do Brasil (BCB).
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import json
from .base_extractor import BaseExtractor

class BCBExtractor(BaseExtractor):
    """Extrator para dados do Banco Central do Brasil."""
    
    def __init__(self):
        """Inicializa o extrator BCB."""
        super().__init__(
            name="BCB",
            base_url="https://api.bcb.gov.br/dados/serie/bcdata.sgs",
            cache_duration=1800  # 30 minutos
        )
        
        # URLs específicas do BCB
        self.sgs_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
        self.focus_url = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata"
        
        # Códigos das séries do SGS (Sistema Gerenciador de Séries Temporais)
        self.series_codes = {
            'selic': 432,  # Taxa Selic
            'ipca': 433,   # IPCA
            'igpm': 189,   # IGP-M
            'cdi': 4389,   # CDI
            'pib': 4380,   # PIB mensal
            'cambio_usd': 1,  # Taxa de câmbio USD/BRL
            'inflacao_12m': 13522,  # IPCA acumulado 12 meses
            'meta_selic': 432,  # Meta Selic
            'reservas': 3546  # Reservas internacionais
        }
        
        # Indicadores do Relatório Focus
        self.focus_indicators = {
            'ipca': 'IPCA',
            'selic': 'Selic',
            'pib': 'PIB Total',
            'cambio': 'Câmbio',
            'igpm': 'IGP-M'
        }
    
    def extract_data(self, series_code: int, **kwargs) -> Dict[str, Any]:
        """Extrai dados de uma série específica do SGS."""
        cache_key = f"bcb_sgs_{series_code}"
        
        # Verificar cache primeiro
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            # Construir URL
            url = f"{self.sgs_url}/{series_code}/dados"
            
            # Parâmetros opcionais
            params = {}
            if 'start_date' in kwargs:
                params['dataInicial'] = kwargs['start_date']
            if 'end_date' in kwargs:
                params['dataFinal'] = kwargs['end_date']
            
            # Fazer requisição
            response = self._retry_request(url, params)
            data_list = response.json()
            
            if not data_list:
                raise ValueError("Nenhum dado encontrado")
            
            # Processar dados
            latest_data = data_list[-1]
            
            result = {
                'series_code': series_code,
                'value': float(latest_data['valor']),
                'date': latest_data['data'],
                'source': 'BCB SGS',
                'extracted_at': datetime.now().isoformat(),
                'all_data': data_list[-10:]  # Últimos 10 registros
            }
            
            if self.validate_data(result):
                self._save_to_cache(cache_key, result)
                return result
            else:
                raise ValueError("Dados inválidos extraídos")
                
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados da série {series_code}: {e}")
            raise
    
    def get_latest_data(self, indicator: str) -> Dict[str, Any]:
        """Obtém os dados mais recentes para um indicador específico."""
        if indicator not in self.series_codes:
            raise ValueError(f"Indicador não suportado: {indicator}")
        
        series_code = self.series_codes[indicator]
        return self.extract_data(series_code)
    
    def get_selic_rate(self) -> Dict[str, Any]:
        """Obtém a taxa Selic atual."""
        data = self.get_latest_data('selic')
        
        return {
            'selic_rate': data['value'] / 100,  # Converter para decimal
            'rate_percent': data['value'],
            'date': data['date'],
            'source': 'BCB',
            'description': 'Taxa Selic'
        }
    
    def get_ipca_data(self) -> Dict[str, Any]:
        """Obtém dados do IPCA."""
        # IPCA mensal
        ipca_monthly = self.get_latest_data('ipca')
        
        # IPCA acumulado 12 meses
        try:
            ipca_12m = self.get_latest_data('inflacao_12m')
        except:
            ipca_12m = None
        
        return {
            'ipca_monthly': {
                'value': ipca_monthly['value'],
                'date': ipca_monthly['date']
            },
            'ipca_12m': {
                'value': ipca_12m['value'] if ipca_12m else None,
                'date': ipca_12m['date'] if ipca_12m else None
            },
            'source': 'BCB',
            'extracted_at': datetime.now().isoformat()
        }
    
    def get_exchange_rate(self) -> Dict[str, Any]:
        """Obtém a taxa de câmbio USD/BRL."""
        data = self.get_latest_data('cambio_usd')
        
        return {
            'usd_brl_rate': data['value'],
            'date': data['date'],
            'source': 'BCB',
            'description': 'Taxa de câmbio USD/BRL'
        }
    
    def get_focus_expectations(self, indicator: str, reference_date: Optional[str] = None) -> Dict[str, Any]:
        """Obtém expectativas do Relatório Focus."""
        if indicator not in self.focus_indicators:
            raise ValueError(f"Indicador Focus não suportado: {indicator}")
        
        cache_key = f"bcb_focus_{indicator}_{reference_date or 'current'}"
        
        # Verificar cache
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            # Construir URL do Focus
            indicator_name = self.focus_indicators[indicator]
            url = f"{self.focus_url}/ExpectativasMercadoTop5Mensais"
            
            params = {
                '$filter': f"Indicador eq '{indicator_name}'",
                '$orderby': 'Data desc',
                '$top': 10,
                '$format': 'json'
            }
            
            if reference_date:
                params['$filter'] += f" and DataReferencia eq '{reference_date}'"
            
            response = self._retry_request(url, params)
            data = response.json()
            
            if 'value' not in data or not data['value']:
                raise ValueError("Nenhuma expectativa encontrada")
            
            # Processar expectativas
            expectations = data['value']
            latest = expectations[0] if expectations else None
            
            result = {
                'indicator': indicator,
                'indicator_name': indicator_name,
                'median': latest['Mediana'] if latest else None,
                'mean': latest['Media'] if latest else None,
                'reference_date': latest['DataReferencia'] if latest else None,
                'data_date': latest['Data'] if latest else None,
                'source': 'BCB Focus',
                'all_expectations': expectations[:5]  # Top 5
            }
            
            if self.validate_data(result):
                self._save_to_cache(cache_key, result)
                return result
            else:
                raise ValueError("Dados de expectativas inválidos")
                
        except Exception as e:
            self.logger.error(f"Erro ao obter expectativas Focus para {indicator}: {e}")
            raise
    
    def get_brazilian_risk_indicators(self) -> Dict[str, Any]:
        """Obtém indicadores de risco do Brasil."""
        indicators = {}
        
        try:
            # Taxa Selic
            selic = self.get_selic_rate()
            indicators['selic'] = selic
            
            # IPCA
            ipca = self.get_ipca_data()
            indicators['ipca'] = ipca
            
            # Câmbio
            exchange = self.get_exchange_rate()
            indicators['exchange_rate'] = exchange
            
            # Expectativas Focus
            try:
                focus_selic = self.get_focus_expectations('selic')
                indicators['selic_expectations'] = focus_selic
            except:
                self.logger.warning("Não foi possível obter expectativas Selic")
            
            try:
                focus_ipca = self.get_focus_expectations('ipca')
                indicators['ipca_expectations'] = focus_ipca
            except:
                self.logger.warning("Não foi possível obter expectativas IPCA")
            
        except Exception as e:
            self.logger.error(f"Erro ao obter indicadores de risco: {e}")
        
        return {
            'brazilian_indicators': indicators,
            'extracted_at': datetime.now().isoformat(),
            'source': 'BCB'
        }
    
    def get_historical_data(self, indicator: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Obtém dados históricos para análise."""
        if indicator not in self.series_codes:
            raise ValueError(f"Indicador não suportado: {indicator}")
        
        series_code = self.series_codes[indicator]
        
        try:
            data = self.extract_data(
                series_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if 'all_data' in data:
                df = pd.DataFrame(data['all_data'])
                df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
                df = df.dropna(subset=['valor'])
                df = df.sort_values('data')
                df = df.rename(columns={'data': 'date', 'valor': 'value'})
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Erro ao obter dados históricos: {e}")
            return pd.DataFrame()
    
    def calculate_real_interest_rate(self) -> Dict[str, Any]:
        """Calcula a taxa de juros real (Selic - IPCA)."""
        try:
            selic_data = self.get_selic_rate()
            ipca_data = self.get_ipca_data()
            
            selic_rate = selic_data['rate_percent']
            ipca_12m = ipca_data['ipca_12m']['value']
            
            if selic_rate is not None and ipca_12m is not None:
                # Fórmula: (1 + Selic) / (1 + IPCA) - 1
                real_rate = ((1 + selic_rate/100) / (1 + ipca_12m/100) - 1) * 100
                
                return {
                    'real_interest_rate': real_rate / 100,  # Decimal
                    'real_rate_percent': real_rate,
                    'selic_rate': selic_rate,
                    'ipca_12m': ipca_12m,
                    'calculation_date': datetime.now().isoformat(),
                    'source': 'BCB (calculated)'
                }
            else:
                raise ValueError("Dados insuficientes para cálculo")
                
        except Exception as e:
            self.logger.error(f"Erro ao calcular taxa real: {e}")
            raise
    
    def get_cost_of_debt_brazil(self) -> Dict[str, Any]:
        """Obtém o custo da dívida no Brasil (baseado em Selic + spread)."""
        try:
            selic_data = self.get_selic_rate()
            
            # Spread típico para empresas (pode ser ajustado)
            corporate_spread = 2.0  # 2% de spread
            
            cost_of_debt = selic_data['rate_percent'] + corporate_spread
            
            return {
                'cost_of_debt': cost_of_debt / 100,  # Decimal
                'cost_of_debt_percent': cost_of_debt,
                'selic_rate': selic_data['rate_percent'],
                'corporate_spread': corporate_spread,
                'date': selic_data['date'],
                'source': 'BCB + Corporate Spread'
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao calcular custo da dívida: {e}")
            raise
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """Valida dados específicos do BCB."""
        if not super().validate_data(data):
            return False
        
        # Validações específicas do BCB
        if 'series_code' in data and not isinstance(data['series_code'], int):
            self.logger.error("Código da série deve ser inteiro")
            return False
        
        if 'value' in data and data['value'] is not None:
            if not isinstance(data['value'], (int, float)):
                self.logger.error("Valor deve ser numérico")
                return False
        
        return True
    
    def __str__(self) -> str:
        return "BCB Extractor (Banco Central do Brasil)"