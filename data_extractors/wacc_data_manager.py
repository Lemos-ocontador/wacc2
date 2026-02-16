#!/usr/bin/env python3
"""
Gerenciador central para extração de dados WACC.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from .fred_extractor import FREDExtractor
from .bcb_extractor import BCBExtractor
from .damodaran_extractor import DamodaranExtractor
from .web_scraper import WebScraper

class WACCDataManager:
    """Gerenciador central para extração e coordenação de dados WACC."""
    
    def __init__(self, fred_api_key: Optional[str] = None, cache_dir: str = "cache"):
        """Inicializa o gerenciador de dados WACC."""
        self.logger = logging.getLogger(__name__)
        self.cache_dir = cache_dir
        
        # Inicializar extratores
        self.fred = FREDExtractor(api_key=fred_api_key)
        self.bcb = BCBExtractor()
        self.damodaran = DamodaranExtractor()
        self.web_scraper = WebScraper()
        
        # Configurar cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        # Mapeamento de componentes WACC para extratores
        self.wacc_components = {
            'risk_free_rate': {
                'primary': self.fred,
                'fallback': self.web_scraper,
                'method': 'get_treasury_rates'
            },
            'market_risk_premium': {
                'primary': self.damodaran,
                'fallback': None,
                'method': 'get_market_risk_premium'
            },
            'country_risk_premium': {
                'primary': self.damodaran,
                'fallback': None,
                'method': 'get_brazil_country_risk'
            },
            'industry_beta': {
                'primary': self.damodaran,
                'fallback': None,
                'method': 'get_industry_beta'
            },
            'size_premium': {
                'primary': self.damodaran,
                'fallback': None,
                'method': 'get_size_premium'
            },
            'cost_of_debt': {
                'primary': self.bcb,
                'fallback': None,
                'method': 'get_cost_of_debt_brazil'
            },
            'tax_rate': {
                'primary': None,  # Será obtido da base de dados existente
                'fallback': None,
                'method': None
            },
            'inflation_us': {
                'primary': self.fred,
                'fallback': None,
                'method': 'get_inflation_data'
            },
            'inflation_brazil': {
                'primary': self.bcb,
                'fallback': None,
                'method': 'get_ipca_data'
            },
            'selic_rate': {
                'primary': self.bcb,
                'fallback': None,
                'method': 'get_selic_rate'
            }
        }
    
    def extract_all_wacc_components(self, sector: Optional[str] = None, 
                                  company_size: str = 'medium') -> Dict[str, Any]:
        """Extrai todos os componentes necessários para o cálculo do WACC."""
        self.logger.info("Iniciando extração de todos os componentes WACC")
        
        results = {
            'extraction_timestamp': datetime.now().isoformat(),
            'sector': sector,
            'company_size': company_size,
            'components': {},
            'errors': {},
            'summary': {}
        }
        
        # Extrair componentes em paralelo
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_component = {}
            
            for component, config in self.wacc_components.items():
                if config['primary'] and config['method']:
                    future = executor.submit(
                        self._extract_component_safe,
                        component,
                        config,
                        sector=sector,
                        company_size=company_size
                    )
                    future_to_component[future] = component
            
            # Coletar resultados
            for future in as_completed(future_to_component):
                component = future_to_component[future]
                try:
                    component_data = future.result()
                    results['components'][component] = component_data
                    self.logger.info(f"Componente {component} extraído com sucesso")
                except Exception as e:
                    error_msg = f"Erro ao extrair {component}: {str(e)}"
                    results['errors'][component] = error_msg
                    self.logger.error(error_msg)
        
        # Gerar resumo
        results['summary'] = self._generate_extraction_summary(results)
        
        # Salvar resultados
        self._save_extraction_results(results)
        
        return results
    
    def _extract_component_safe(self, component: str, config: Dict[str, Any], 
                               **kwargs) -> Dict[str, Any]:
        """Extrai um componente de forma segura com fallback."""
        extractor = config['primary']
        method_name = config['method']
        
        try:
            # Tentar extrator primário
            method = getattr(extractor, method_name)
            
            # Chamar método com parâmetros apropriados
            if component == 'industry_beta' and 'sector' in kwargs:
                return method(kwargs['sector'])
            elif component == 'size_premium':
                return method(kwargs.get('company_size', 'medium'))
            else:
                return method()
                
        except Exception as e:
            self.logger.warning(f"Extrator primário falhou para {component}: {e}")
            
            # Tentar fallback se disponível
            if config['fallback']:
                try:
                    fallback_extractor = config['fallback']
                    fallback_method = getattr(fallback_extractor, method_name)
                    return fallback_method()
                except Exception as fallback_error:
                    self.logger.error(f"Fallback também falhou para {component}: {fallback_error}")
            
            raise e
    
    def get_risk_free_rate(self, term: str = '10y') -> Dict[str, Any]:
        """Obtém a taxa livre de risco."""
        try:
            # Tentar FRED primeiro
            return self.fred.calculate_risk_free_rate(term)
        except Exception as e:
            self.logger.warning(f"FRED falhou, tentando web scraping: {e}")
            # Fallback para web scraping
            return self.web_scraper.extract_treasury_rates()
    
    def get_market_risk_premium(self) -> Dict[str, Any]:
        """Obtém o prêmio de risco de mercado."""
        return self.damodaran.get_market_risk_premium()
    
    def get_country_risk_premium(self, country: str = 'Brazil') -> Dict[str, Any]:
        """Obtém o prêmio de risco país."""
        if country.lower() == 'brazil':
            return self.damodaran.get_brazil_country_risk()
        else:
            # Para outros países, usar método genérico
            data = self.damodaran.extract_data('country_risk')
            for country_data in data.get('all_countries', []):
                if country.lower() in country_data['country'].lower():
                    return {
                        'country': country,
                        'risk_data': country_data['data'],
                        'source': 'Damodaran'
                    }
            raise ValueError(f"País não encontrado: {country}")
    
    def get_industry_beta(self, sector: str) -> Dict[str, Any]:
        """Obtém o beta setorial."""
        return self.damodaran.get_industry_beta(sector)
    
    def get_cost_of_debt(self, country: str = 'Brazil') -> Dict[str, Any]:
        """Obtém o custo da dívida."""
        if country.lower() == 'brazil':
            return self.bcb.get_cost_of_debt_brazil()
        else:
            # Para outros países, usar taxa livre de risco + spread
            risk_free = self.get_risk_free_rate()
            spread = 3.0  # Spread padrão de 3%
            
            return {
                'cost_of_debt': (risk_free['risk_free_rate'] + spread/100),
                'risk_free_rate': risk_free['risk_free_rate'],
                'spread': spread/100,
                'country': country,
                'source': 'Calculated'
            }
    
    def get_inflation_data(self, country: str = 'Brazil') -> Dict[str, Any]:
        """Obtém dados de inflação."""
        if country.lower() == 'brazil':
            return self.bcb.get_ipca_data()
        elif country.lower() == 'us' or country.lower() == 'usa':
            return self.fred.get_inflation_data()
        else:
            raise ValueError(f"País não suportado para inflação: {country}")
    
    def calculate_wacc(self, components: Dict[str, Any], 
                      market_value_equity: float,
                      market_value_debt: float,
                      tax_rate: float) -> Dict[str, Any]:
        """Calcula o WACC baseado nos componentes extraídos."""
        try:
            # Extrair valores dos componentes
            risk_free_rate = self._extract_numeric_value(components.get('risk_free_rate'), 'risk_free_rate')
            market_risk_premium = self._extract_numeric_value(components.get('market_risk_premium'), 'market_risk_premium')
            country_risk_premium = self._extract_numeric_value(components.get('country_risk_premium'), 'country_risk_premium')
            beta = self._extract_numeric_value(components.get('industry_beta'), 'beta')
            cost_of_debt = self._extract_numeric_value(components.get('cost_of_debt'), 'cost_of_debt')
            
            # Calcular pesos
            total_value = market_value_equity + market_value_debt
            weight_equity = market_value_equity / total_value
            weight_debt = market_value_debt / total_value
            
            # Calcular custo do patrimônio líquido (CAPM)
            cost_of_equity = risk_free_rate + beta * (market_risk_premium + country_risk_premium)
            
            # Calcular WACC
            wacc = (weight_equity * cost_of_equity) + (weight_debt * cost_of_debt * (1 - tax_rate))
            
            return {
                'wacc': wacc,
                'wacc_percent': wacc * 100,
                'components': {
                    'cost_of_equity': cost_of_equity,
                    'cost_of_debt': cost_of_debt,
                    'weight_equity': weight_equity,
                    'weight_debt': weight_debt,
                    'tax_rate': tax_rate,
                    'risk_free_rate': risk_free_rate,
                    'market_risk_premium': market_risk_premium,
                    'country_risk_premium': country_risk_premium,
                    'beta': beta
                },
                'market_values': {
                    'equity': market_value_equity,
                    'debt': market_value_debt,
                    'total': total_value
                },
                'calculation_date': datetime.now().isoformat(),
                'source': 'WACC Data Manager'
            }
            
        except Exception as e:
            self.logger.error(f"Erro no cálculo do WACC: {e}")
            raise
    
    def _extract_numeric_value(self, component_data: Dict[str, Any], key: str) -> float:
        """Extrai valor numérico de dados de componente."""
        if not component_data:
            raise ValueError(f"Dados do componente não disponíveis para {key}")
        
        # Tentar diferentes chaves possíveis
        possible_keys = [key, 'value', 'rate', 'premium', 'latest_value']
        
        for possible_key in possible_keys:
            if possible_key in component_data:
                value = component_data[possible_key]
                if isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, str):
                    try:
                        return float(value.replace('%', '').replace(',', ''))
                    except:
                        continue
        
        # Se não encontrou valor direto, procurar em sub-dicionários
        for v in component_data.values():
            if isinstance(v, dict):
                try:
                    return self._extract_numeric_value(v, key)
                except:
                    continue
        
        raise ValueError(f"Valor numérico não encontrado para {key} em {component_data}")
    
    def _generate_extraction_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Gera resumo da extração."""
        total_components = len(self.wacc_components)
        successful_extractions = len(results['components'])
        failed_extractions = len(results['errors'])
        
        return {
            'total_components': total_components,
            'successful_extractions': successful_extractions,
            'failed_extractions': failed_extractions,
            'success_rate': successful_extractions / total_components * 100,
            'extraction_duration': 'calculated_separately',  # Seria calculado com timestamps
            'data_sources_used': list(set([comp.get('source', 'Unknown') 
                                         for comp in results['components'].values()]))
        }
    
    def _save_extraction_results(self, results: Dict[str, Any]) -> None:
        """Salva resultados da extração em arquivo."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"wacc_extraction_{timestamp}.json"
        filepath = os.path.join(self.cache_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"Resultados salvos em {filepath}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar resultados: {e}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Verifica o status de saúde de todos os extratores."""
        status = {
            'timestamp': datetime.now().isoformat(),
            'extractors': {},
            'overall_status': 'healthy'
        }
        
        extractors = {
            'FRED': self.fred,
            'BCB': self.bcb,
            'Damodaran': self.damodaran,
            'WebScraper': self.web_scraper
        }
        
        for name, extractor in extractors.items():
            try:
                health = extractor.health_check()
                status['extractors'][name] = health
                if not health.get('healthy', False):
                    status['overall_status'] = 'degraded'
            except Exception as e:
                status['extractors'][name] = {
                    'healthy': False,
                    'error': str(e)
                }
                status['overall_status'] = 'degraded'
        
        return status
    
    def clear_cache(self) -> Dict[str, Any]:
        """Limpa o cache de todos os extratores."""
        results = {
            'timestamp': datetime.now().isoformat(),
            'cleared_extractors': [],
            'errors': []
        }
        
        extractors = [self.fred, self.bcb, self.damodaran, self.web_scraper]
        
        for extractor in extractors:
            try:
                extractor.clear_cache()
                results['cleared_extractors'].append(str(extractor))
            except Exception as e:
                results['errors'].append(f"Erro ao limpar cache de {extractor}: {e}")
        
        return results
    
    def __str__(self) -> str:
        return "WACC Data Manager (Central Coordinator)"