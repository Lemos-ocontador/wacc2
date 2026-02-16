#!/usr/bin/env python3
"""
Classe base para extratores de dados do sistema WACC.
"""

import logging
import requests
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import time
import json

class BaseExtractor(ABC):
    """Classe base abstrata para todos os extratores de dados."""
    
    def __init__(self, name: str, base_url: str, cache_duration: int = 3600):
        """
        Inicializa o extrator base.
        
        Args:
            name: Nome do extrator
            base_url: URL base da fonte de dados
            cache_duration: Duração do cache em segundos (padrão: 1 hora)
        """
        self.name = name
        self.base_url = base_url
        self.cache_duration = cache_duration
        self.cache = {}
        self.session = requests.Session()
        self.logger = self._setup_logger()
        
        # Configurações padrão da sessão
        self.session.headers.update({
            'User-Agent': 'WACC-Automation-System/1.0 (Educational Purpose)'
        })
    
    def _setup_logger(self) -> logging.Logger:
        """Configura o logger para o extrator."""
        logger = logging.getLogger(f"extractor.{self.name.lower()}")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f'%(asctime)s - {self.name} - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Verifica se o cache ainda é válido."""
        if cache_key not in self.cache:
            return False
        
        cached_time = self.cache[cache_key].get('timestamp')
        if not cached_time:
            return False
        
        return (datetime.now() - cached_time).seconds < self.cache_duration
    
    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Recupera dados do cache se válidos."""
        if self._is_cache_valid(cache_key):
            self.logger.info(f"Dados recuperados do cache: {cache_key}")
            return self.cache[cache_key]['data']
        return None
    
    def _save_to_cache(self, cache_key: str, data: Any) -> None:
        """Salva dados no cache."""
        self.cache[cache_key] = {
            'data': data,
            'timestamp': datetime.now()
        }
        self.logger.info(f"Dados salvos no cache: {cache_key}")
    
    def _make_request(self, url: str, params: Dict = None, timeout: int = 30) -> requests.Response:
        """Faz uma requisição HTTP com tratamento de erros."""
        try:
            self.logger.info(f"Fazendo requisição para: {url}")
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro na requisição para {url}: {e}")
            raise
    
    def _retry_request(self, url: str, params: Dict = None, max_retries: int = 3, delay: int = 1) -> requests.Response:
        """Faz requisição com retry automático."""
        for attempt in range(max_retries):
            try:
                return self._make_request(url, params)
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"Tentativa {attempt + 1} falhou, tentando novamente em {delay}s...")
                time.sleep(delay)
                delay *= 2  # Backoff exponencial
    
    @abstractmethod
    def extract_data(self, **kwargs) -> Dict[str, Any]:
        """Método abstrato para extrair dados da fonte."""
        pass
    
    @abstractmethod
    def get_latest_data(self, data_type: str) -> Dict[str, Any]:
        """Método abstrato para obter os dados mais recentes."""
        pass
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """Valida os dados extraídos."""
        if not data:
            self.logger.error("Dados vazios")
            return False
        
        if 'value' not in data or data['value'] is None:
            self.logger.error("Valor não encontrado nos dados")
            return False
        
        if 'date' not in data:
            self.logger.warning("Data não encontrada nos dados")
        
        return True
    
    def format_data(self, raw_data: Any, data_type: str) -> Dict[str, Any]:
        """Formata os dados extraídos para um formato padrão."""
        return {
            'source': self.name,
            'data_type': data_type,
            'value': raw_data,
            'extracted_at': datetime.now().isoformat(),
            'cache_duration': self.cache_duration
        }
    
    def get_historical_data(self, data_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Obtém dados históricos (implementação padrão)."""
        self.logger.warning(f"Dados históricos não implementados para {self.name}")
        return pd.DataFrame()
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica a saúde da conexão com a fonte de dados."""
        try:
            response = self._make_request(self.base_url, timeout=10)
            return {
                'status': 'healthy',
                'response_time': response.elapsed.total_seconds(),
                'status_code': response.status_code,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def clear_cache(self) -> None:
        """Limpa o cache do extrator."""
        self.cache.clear()
        self.logger.info("Cache limpo")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Retorna informações sobre o cache."""
        return {
            'cache_size': len(self.cache),
            'cache_keys': list(self.cache.keys()),
            'cache_duration': self.cache_duration
        }
    
    def __str__(self) -> str:
        return f"{self.name} Extractor ({self.base_url})"
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(name='{self.name}', base_url='{self.base_url}')>"