#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema de Cálculo Automatizado do WACC

Este módulo implementa um calculador automatizado do WACC (Weighted Average Cost of Capital)
que integra todos os componentes extraídos das diferentes fontes de dados.

Autor: Sistema Automatizado WACC
Data: 2025-09-24
"""

import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
import numpy as np
from pathlib import Path

# Importar os extratores
from data_extractors import WACCDataManager


@dataclass
class WACCComponents:
    """Classe para armazenar os componentes do WACC."""
    risk_free_rate: float = 0.0
    market_risk_premium: float = 0.0
    country_risk_premium: float = 0.0
    beta: float = 1.0
    size_premium: float = 0.0
    cost_of_debt: float = 0.0
    tax_rate: float = 0.0
    debt_to_equity: float = 0.0
    market_value_equity: float = 0.0
    market_value_debt: float = 0.0
    
    # Campos calculados
    cost_of_equity: float = 0.0
    weight_equity: float = 0.0
    weight_debt: float = 0.0
    wacc: float = 0.0
    
    # Metadados
    calculation_date: str = ""
    data_sources: Dict[str, str] = None
    
    def __post_init__(self):
        if self.data_sources is None:
            self.data_sources = {}


class WACCCalculator:
    """Calculador automatizado do WACC."""
    
    def __init__(self, cache_dir: str = "cache"):
        """Inicializa o calculador WACC.
        
        Args:
            cache_dir: Diretório para cache de dados
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Configurar logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Inicializar gerenciador de dados
        self.data_manager = WACCDataManager(cache_dir=str(cache_dir))
        
        # Configurações padrão
        self.default_values = {
            'risk_free_rate': 0.045,  # 4.5% padrão
            'market_risk_premium': 0.055,  # 5.5% padrão
            'country_risk_premium': 0.025,  # 2.5% para Brasil
            'beta': 1.0,
            'size_premium': 0.0,
            'cost_of_debt': 0.08,  # 8% padrão
            'tax_rate': 0.34,  # 34% padrão Brasil
        }
    
    def extract_all_components(self, sector: str = None, country: str = "Brazil") -> Dict[str, Any]:
        """Extrai todos os componentes necessários para o cálculo do WACC.
        
        Args:
            sector: Setor da empresa (opcional)
            country: País para risco país (padrão: Brazil)
            
        Returns:
            Dicionário com todos os componentes extraídos
        """
        self.logger.info(f"Extraindo componentes WACC para setor: {sector}, país: {country}")
        
        # Extrair dados usando o data manager
        components = self.data_manager.extract_all_wacc_components()
        
        # Processar dados específicos por setor se fornecido
        if sector:
            sector_data = self._get_sector_specific_data(sector)
            components.update(sector_data)
        
        # Processar dados específicos por país
        country_data = self._get_country_specific_data(country)
        components.update(country_data)
        
        return components
    
    def _get_sector_specific_data(self, sector: str) -> Dict[str, Any]:
        """Obtém dados específicos do setor.
        
        Args:
            sector: Nome do setor
            
        Returns:
            Dicionário com dados do setor
        """
        try:
            # Tentar extrair beta do setor do Damodaran
            damodaran_data = self.data_manager.damodaran.get_industry_beta(sector)
            
            sector_data = {}
            if damodaran_data and 'data' in damodaran_data:
                # Procurar pelo setor nos dados
                for industry in damodaran_data['data']:
                    if sector.lower() in industry.get('Industry Name', '').lower():
                        sector_data['beta'] = float(industry.get('Unlevered beta', 1.0))
                        sector_data['debt_to_equity'] = float(industry.get('D/E', 0.0))
                        sector_data['tax_rate'] = float(industry.get('Tax rate', 0.34))
                        break
            
            return sector_data
            
        except Exception as e:
            self.logger.warning(f"Erro ao obter dados do setor {sector}: {e}")
            return {}
    
    def _get_country_specific_data(self, country: str) -> Dict[str, Any]:
        """Obtém dados específicos do país.
        
        Args:
            country: Nome do país
            
        Returns:
            Dicionário com dados do país
        """
        try:
            country_data = {}
            
            # Obter risco país
            if country.lower() == "brazil":
                # Dados específicos do Brasil
                bcb_data = self.data_manager.bcb.get_latest_data('selic')
                if bcb_data and 'data' in bcb_data:
                    country_data['cost_of_debt'] = bcb_data['data'].get('value', 0.08)
                
                # Risco país do Damodaran
                risk_data = self.data_manager.damodaran.get_country_risk('Brazil')
                if risk_data and 'data' in risk_data:
                    country_data['country_risk_premium'] = risk_data['data'].get('country_risk_premium', 0.025)
            
            return country_data
            
        except Exception as e:
            self.logger.warning(f"Erro ao obter dados do país {country}: {e}")
            return {}
    
    def calculate_wacc(self, 
                      sector: str = None, 
                      country: str = "Brazil",
                      market_value_equity: float = None,
                      market_value_debt: float = None,
                      custom_components: Dict[str, float] = None) -> WACCComponents:
        """Calcula o WACC completo.
        
        Args:
            sector: Setor da empresa
            country: País da empresa
            market_value_equity: Valor de mercado do patrimônio líquido
            market_value_debt: Valor de mercado da dívida
            custom_components: Componentes customizados pelo usuário
            
        Returns:
            Objeto WACCComponents com todos os valores calculados
        """
        self.logger.info("Iniciando cálculo do WACC")
        
        # Extrair componentes
        extracted_data = self.extract_all_components(sector, country)
        
        # Criar objeto de componentes
        components = WACCComponents()
        components.calculation_date = datetime.now().isoformat()
        
        # Preencher componentes com dados extraídos ou valores padrão
        components.risk_free_rate = self._get_component_value(
            'risk_free_rate', extracted_data, custom_components
        )
        components.market_risk_premium = self._get_component_value(
            'market_risk_premium', extracted_data, custom_components
        )
        components.country_risk_premium = self._get_component_value(
            'country_risk_premium', extracted_data, custom_components
        )
        components.beta = self._get_component_value(
            'beta', extracted_data, custom_components
        )
        components.size_premium = self._get_component_value(
            'size_premium', extracted_data, custom_components
        )
        components.cost_of_debt = self._get_component_value(
            'cost_of_debt', extracted_data, custom_components
        )
        components.tax_rate = self._get_component_value(
            'tax_rate', extracted_data, custom_components
        )
        
        # Valores de mercado
        if market_value_equity:
            components.market_value_equity = market_value_equity
        if market_value_debt:
            components.market_value_debt = market_value_debt
        
        # Calcular D/E se valores de mercado fornecidos
        if market_value_equity and market_value_debt:
            components.debt_to_equity = market_value_debt / market_value_equity
        else:
            components.debt_to_equity = self._get_component_value(
                'debt_to_equity', extracted_data, custom_components
            )
        
        # Calcular componentes derivados
        self._calculate_derived_components(components)
        
        # Registrar fontes de dados
        components.data_sources = self._get_data_sources(extracted_data)
        
        self.logger.info(f"WACC calculado: {components.wacc:.4f} ({components.wacc*100:.2f}%)")
        
        return components
    
    def _get_component_value(self, component: str, extracted_data: Dict[str, Any], 
                           custom_components: Dict[str, float] = None) -> float:
        """Obtém o valor de um componente com prioridade: custom > extracted > default.
        
        Args:
            component: Nome do componente
            extracted_data: Dados extraídos
            custom_components: Componentes customizados
            
        Returns:
            Valor do componente
        """
        # Prioridade 1: Valores customizados
        if custom_components and component in custom_components:
            return custom_components[component]
        
        # Prioridade 2: Dados extraídos
        if component in extracted_data:
            value = extracted_data[component]
            if isinstance(value, dict) and 'data' in value:
                if isinstance(value['data'], dict) and 'value' in value['data']:
                    return float(value['data']['value'])
                elif isinstance(value['data'], (int, float)):
                    return float(value['data'])
            elif isinstance(value, (int, float)):
                return float(value)
        
        # Prioridade 3: Valores padrão
        return self.default_values.get(component, 0.0)
    
    def _calculate_derived_components(self, components: WACCComponents):
        """Calcula os componentes derivados do WACC.
        
        Args:
            components: Objeto WACCComponents para atualizar
        """
        # Custo do patrimônio líquido (CAPM + Size Premium)
        components.cost_of_equity = (
            components.risk_free_rate + 
            components.beta * (components.market_risk_premium + components.country_risk_premium) +
            components.size_premium
        )
        
        # Pesos da dívida e patrimônio líquido
        total_value = 1 + components.debt_to_equity
        components.weight_equity = 1 / total_value
        components.weight_debt = components.debt_to_equity / total_value
        
        # WACC final
        components.wacc = (
            components.weight_equity * components.cost_of_equity +
            components.weight_debt * components.cost_of_debt * (1 - components.tax_rate)
        )
    
    def _get_data_sources(self, extracted_data: Dict[str, Any]) -> Dict[str, str]:
        """Extrai as fontes de dados utilizadas.
        
        Args:
            extracted_data: Dados extraídos
            
        Returns:
            Dicionário com as fontes de dados
        """
        sources = {}
        for component, data in extracted_data.items():
            if isinstance(data, dict) and 'source' in data:
                sources[component] = data['source']
        return sources
    
    def save_calculation(self, components: WACCComponents, filename: str = None) -> str:
        """Salva o cálculo do WACC em arquivo JSON.
        
        Args:
            components: Componentes calculados
            filename: Nome do arquivo (opcional)
            
        Returns:
            Caminho do arquivo salvo
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"wacc_calculation_{timestamp}.json"
        
        filepath = self.cache_dir / filename
        
        # Converter para dicionário
        data = {
            'calculation_date': components.calculation_date,
            'components': {
                'risk_free_rate': components.risk_free_rate,
                'market_risk_premium': components.market_risk_premium,
                'country_risk_premium': components.country_risk_premium,
                'beta': components.beta,
                'size_premium': components.size_premium,
                'cost_of_debt': components.cost_of_debt,
                'tax_rate': components.tax_rate,
                'debt_to_equity': components.debt_to_equity,
                'market_value_equity': components.market_value_equity,
                'market_value_debt': components.market_value_debt
            },
            'calculated_values': {
                'cost_of_equity': components.cost_of_equity,
                'weight_equity': components.weight_equity,
                'weight_debt': components.weight_debt,
                'wacc': components.wacc
            },
            'data_sources': components.data_sources,
            'wacc_percentage': components.wacc * 100
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Cálculo salvo em: {filepath}")
        return str(filepath)
    
    def load_calculation(self, filename: str) -> WACCComponents:
        """Carrega um cálculo salvo.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            Objeto WACCComponents carregado
        """
        filepath = self.cache_dir / filename
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        components = WACCComponents()
        components.calculation_date = data['calculation_date']
        
        # Carregar componentes
        comp_data = data['components']
        components.risk_free_rate = comp_data['risk_free_rate']
        components.market_risk_premium = comp_data['market_risk_premium']
        components.country_risk_premium = comp_data['country_risk_premium']
        components.beta = comp_data['beta']
        components.size_premium = comp_data['size_premium']
        components.cost_of_debt = comp_data['cost_of_debt']
        components.tax_rate = comp_data['tax_rate']
        components.debt_to_equity = comp_data['debt_to_equity']
        components.market_value_equity = comp_data['market_value_equity']
        components.market_value_debt = comp_data['market_value_debt']
        
        # Carregar valores calculados
        calc_data = data['calculated_values']
        components.cost_of_equity = calc_data['cost_of_equity']
        components.weight_equity = calc_data['weight_equity']
        components.weight_debt = calc_data['weight_debt']
        components.wacc = calc_data['wacc']
        
        components.data_sources = data['data_sources']
        
        return components
    
    def get_calculation_summary(self, components: WACCComponents) -> str:
        """Gera um resumo textual do cálculo.
        
        Args:
            components: Componentes calculados
            
        Returns:
            String com resumo do cálculo
        """
        summary = f"""
=== RESUMO DO CÁLCULO WACC ===
Data do Cálculo: {components.calculation_date}

COMPONENTES UTILIZADOS:
• Taxa Livre de Risco: {components.risk_free_rate:.4f} ({components.risk_free_rate*100:.2f}%)
• Prêmio de Risco de Mercado: {components.market_risk_premium:.4f} ({components.market_risk_premium*100:.2f}%)
• Prêmio de Risco País: {components.country_risk_premium:.4f} ({components.country_risk_premium*100:.2f}%)
• Beta: {components.beta:.4f}
• Prêmio de Tamanho: {components.size_premium:.4f} ({components.size_premium*100:.2f}%)
• Custo da Dívida: {components.cost_of_debt:.4f} ({components.cost_of_debt*100:.2f}%)
• Taxa de Imposto: {components.tax_rate:.4f} ({components.tax_rate*100:.2f}%)
• Relação D/E: {components.debt_to_equity:.4f}

VALORES CALCULADOS:
• Custo do Patrimônio Líquido: {components.cost_of_equity:.4f} ({components.cost_of_equity*100:.2f}%)
• Peso do Patrimônio: {components.weight_equity:.4f} ({components.weight_equity*100:.2f}%)
• Peso da Dívida: {components.weight_debt:.4f} ({components.weight_debt*100:.2f}%)

🎯 WACC FINAL: {components.wacc:.4f} ({components.wacc*100:.2f}%)

FONTES DE DADOS:
"""
        
        for component, source in components.data_sources.items():
            summary += f"• {component}: {source}\n"
        
        return summary


def main():
    """Função principal para teste do calculador."""
    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    
    # Criar calculador
    calculator = WACCCalculator()
    
    print("=== TESTE DO CALCULADOR WACC ===")
    
    # Exemplo 1: Cálculo básico
    print("\n1. Cálculo básico (sem setor específico):")
    components1 = calculator.calculate_wacc()
    print(calculator.get_calculation_summary(components1))
    
    # Salvar cálculo
    filename1 = calculator.save_calculation(components1)
    print(f"Cálculo salvo em: {filename1}")
    
    # Exemplo 2: Cálculo com setor específico
    print("\n2. Cálculo para setor de transporte:")
    components2 = calculator.calculate_wacc(
        sector="Transportation",
        market_value_equity=1000000,
        market_value_debt=500000
    )
    print(calculator.get_calculation_summary(components2))
    
    # Exemplo 3: Cálculo com componentes customizados
    print("\n3. Cálculo com componentes customizados:")
    custom_components = {
        'beta': 1.5,
        'tax_rate': 0.30,
        'cost_of_debt': 0.10
    }
    components3 = calculator.calculate_wacc(
        sector="Technology",
        custom_components=custom_components
    )
    print(calculator.get_calculation_summary(components3))


if __name__ == "__main__":
    main()