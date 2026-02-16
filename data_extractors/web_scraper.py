#!/usr/bin/env python3
"""
Extrator genérico para web scraping de dados financeiros.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union
import json
import re
from bs4 import BeautifulSoup
from .base_extractor import BaseExtractor

class WebScraper(BaseExtractor):
    """Extrator genérico para web scraping."""
    
    def __init__(self, name: str = "WebScraper", base_url: str = ""):
        """Inicializa o web scraper."""
        super().__init__(
            name=name,
            base_url=base_url,
            cache_duration=3600  # 1 hora
        )
        
        # Configurações específicas para diferentes sites
        self.site_configs = {
            'investing.com': {
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'selectors': {
                    'price': '[data-test="instrument-price-last"]',
                    'change': '[data-test="instrument-price-change"]'
                }
            },
            'yahoo.finance': {
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'selectors': {
                    'price': '[data-symbol] [data-field="regularMarketPrice"]',
                    'change': '[data-symbol] [data-field="regularMarketChange"]'
                }
            },
            'marketwatch.com': {
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'selectors': {
                    'price': '.intraday__price .value',
                    'change': '.change--point--q .value'
                }
            }
        }
    
    def extract_data(self, url: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados de uma URL específica."""
        cache_key = f"webscraper_{hash(url)}"
        
        # Verificar cache primeiro
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            # Determinar configuração do site
            site_config = self._get_site_config(url)
            
            # Fazer requisição
            headers = site_config.get('headers', {})
            response = self._retry_request(url, headers=headers)
            
            # Processar baseado no tipo de extração
            extraction_type = kwargs.get('type', 'html')
            
            if extraction_type == 'table':
                data = self._extract_table_data(response.text, **kwargs)
            elif extraction_type == 'json':
                data = self._extract_json_data(response.text, **kwargs)
            elif extraction_type == 'regex':
                data = self._extract_regex_data(response.text, **kwargs)
            else:
                data = self._extract_html_data(response.text, site_config, **kwargs)
            
            result = {
                'url': url,
                'data': data,
                'extracted_at': datetime.now().isoformat(),
                'source': 'Web Scraping',
                'extraction_type': extraction_type
            }
            
            if self.validate_data(result):
                self._save_to_cache(cache_key, result)
                return result
            else:
                raise ValueError("Dados inválidos extraídos")
                
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados de {url}: {e}")
            raise
    
    def _get_site_config(self, url: str) -> Dict[str, Any]:
        """Obtém configuração específica do site."""
        for site, config in self.site_configs.items():
            if site in url.lower():
                return config
        
        # Configuração padrão
        return {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            'selectors': {}
        }
    
    def _extract_html_data(self, html: str, site_config: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Extrai dados de HTML usando BeautifulSoup."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Usar seletores específicos se fornecidos
        selectors = kwargs.get('selectors', site_config.get('selectors', {}))
        
        extracted_data = {}
        
        for key, selector in selectors.items():
            try:
                element = soup.select_one(selector)
                if element:
                    extracted_data[key] = element.get_text(strip=True)
                else:
                    extracted_data[key] = None
            except Exception as e:
                self.logger.warning(f"Erro ao extrair {key} com seletor {selector}: {e}")
                extracted_data[key] = None
        
        # Se não há seletores específicos, tentar extrair dados gerais
        if not extracted_data:
            extracted_data = self._extract_general_financial_data(soup)
        
        return extracted_data
    
    def _extract_general_financial_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extrai dados financeiros gerais de uma página."""
        data = {}
        
        # Procurar por preços (padrões comuns)
        price_patterns = [
            r'\$?([0-9,]+\.?[0-9]*)',
            r'([0-9,]+\.?[0-9]*)%',
            r'R\$\s*([0-9,]+\.?[0-9]*)'
        ]
        
        text = soup.get_text()
        
        for i, pattern in enumerate(price_patterns):
            matches = re.findall(pattern, text)
            if matches:
                data[f'pattern_{i}_matches'] = matches[:5]  # Primeiros 5 matches
        
        # Procurar por tabelas
        tables = soup.find_all('table')
        if tables:
            data['tables_found'] = len(tables)
            data['first_table_rows'] = len(tables[0].find_all('tr')) if tables else 0
        
        # Procurar por elementos com classes relacionadas a finanças
        financial_classes = ['price', 'value', 'rate', 'percent', 'change']
        for cls in financial_classes:
            elements = soup.find_all(class_=re.compile(cls, re.I))
            if elements:
                data[f'{cls}_elements'] = [el.get_text(strip=True) for el in elements[:3]]
        
        return data
    
    def _extract_table_data(self, html: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados de tabelas HTML."""
        try:
            # Usar pandas para extrair tabelas
            tables = pd.read_html(html)
            
            table_index = kwargs.get('table_index', 0)
            if table_index >= len(tables):
                table_index = 0
            
            df = tables[table_index]
            
            return {
                'table_data': df.to_dict('records'),
                'columns': df.columns.tolist(),
                'shape': df.shape,
                'total_tables': len(tables)
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao extrair tabela: {e}")
            return {'error': str(e)}
    
    def _extract_json_data(self, html: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados JSON embutidos no HTML."""
        try:
            # Procurar por JSON em scripts
            soup = BeautifulSoup(html, 'html.parser')
            scripts = soup.find_all('script')
            
            json_data = []
            
            for script in scripts:
                if script.string:
                    # Procurar por padrões JSON
                    json_matches = re.findall(r'\{[^{}]*\}', script.string)
                    for match in json_matches:
                        try:
                            parsed = json.loads(match)
                            json_data.append(parsed)
                        except:
                            continue
            
            return {
                'json_objects': json_data[:10],  # Primeiros 10 objetos JSON
                'total_found': len(json_data)
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao extrair JSON: {e}")
            return {'error': str(e)}
    
    def _extract_regex_data(self, html: str, **kwargs) -> Dict[str, Any]:
        """Extrai dados usando expressões regulares."""
        patterns = kwargs.get('patterns', {})
        
        if not patterns:
            # Padrões padrão para dados financeiros
            patterns = {
                'prices': r'\$?([0-9,]+\.?[0-9]*)',
                'percentages': r'([0-9,]+\.?[0-9]*)%',
                'dates': r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                'currencies': r'([A-Z]{3})\s*([0-9,]+\.?[0-9]*)'
            }
        
        extracted_data = {}
        
        for key, pattern in patterns.items():
            try:
                matches = re.findall(pattern, html)
                extracted_data[key] = matches[:10]  # Primeiros 10 matches
            except Exception as e:
                self.logger.warning(f"Erro ao aplicar padrão {key}: {e}")
                extracted_data[key] = []
        
        return extracted_data
    
    def extract_treasury_rates(self, source: str = 'investing') -> Dict[str, Any]:
        """Extrai taxas do tesouro de diferentes fontes."""
        urls = {
            'investing': 'https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield',
            'marketwatch': 'https://www.marketwatch.com/investing/bond/tmubmusd10y',
            'yahoo': 'https://finance.yahoo.com/quote/%5ETNX'
        }
        
        if source not in urls:
            raise ValueError(f"Fonte não suportada: {source}")
        
        url = urls[source]
        
        try:
            data = self.extract_data(url, type='html')
            
            return {
                'treasury_10y': data,
                'source': source,
                'extracted_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao extrair taxas do tesouro de {source}: {e}")
            raise
    
    def extract_market_data(self, symbol: str, source: str = 'yahoo') -> Dict[str, Any]:
        """Extrai dados de mercado para um símbolo específico."""
        urls = {
            'yahoo': f'https://finance.yahoo.com/quote/{symbol}',
            'investing': f'https://www.investing.com/search/?q={symbol}',
            'marketwatch': f'https://www.marketwatch.com/investing/stock/{symbol}'
        }
        
        if source not in urls:
            raise ValueError(f"Fonte não suportada: {source}")
        
        url = urls[source]
        
        try:
            data = self.extract_data(url, type='html')
            
            return {
                'symbol': symbol,
                'market_data': data,
                'source': source,
                'extracted_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados de mercado para {symbol}: {e}")
            raise
    
    def extract_economic_calendar(self, source: str = 'investing') -> Dict[str, Any]:
        """Extrai dados do calendário econômico."""
        urls = {
            'investing': 'https://www.investing.com/economic-calendar/',
            'marketwatch': 'https://www.marketwatch.com/economy-politics/calendar'
        }
        
        if source not in urls:
            raise ValueError(f"Fonte não suportada: {source}")
        
        url = urls[source]
        
        try:
            data = self.extract_data(url, type='table')
            
            return {
                'economic_calendar': data,
                'source': source,
                'extracted_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao extrair calendário econômico de {source}: {e}")
            raise
    
    def batch_extract(self, urls: List[str], **kwargs) -> Dict[str, Any]:
        """Extrai dados de múltiplas URLs em lote."""
        results = {}
        
        for i, url in enumerate(urls):
            try:
                self.logger.info(f"Extraindo dados de {url} ({i+1}/{len(urls)})")
                data = self.extract_data(url, **kwargs)
                results[f'url_{i}'] = data
            except Exception as e:
                self.logger.error(f"Erro ao extrair {url}: {e}")
                results[f'url_{i}'] = {'error': str(e)}
        
        return {
            'batch_results': results,
            'total_urls': len(urls),
            'successful_extractions': len([r for r in results.values() if 'error' not in r]),
            'extracted_at': datetime.now().isoformat()
        }
    
    def get_latest_data(self, url: str) -> Dict[str, Any]:
        """Obtém os dados mais recentes de uma URL específica."""
        return self.extract_data(url)
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """Valida dados específicos do web scraper."""
        if not super().validate_data(data):
            return False
        
        # Validações específicas do web scraper
        if 'url' not in data:
            self.logger.error("URL não especificada")
            return False
        
        if 'data' not in data:
            self.logger.error("Dados não extraídos")
            return False
        
        return True
    
    def __str__(self) -> str:
        return f"Web Scraper ({self.name})"