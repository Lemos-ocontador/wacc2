#!/usr/bin/env python3
"""
Módulo de extração automatizada de dados para cálculo do WACC.

Este módulo fornece extratores especializados para diferentes fontes de dados:
- FRED: Federal Reserve Economic Data
- BCB: Banco Central do Brasil
- Damodaran: Dados do Professor Aswath Damodaran (NYU Stern)
- WebScraper: Extrator genérico para web scraping
- WACCDataManager: Gerenciador central que coordena todos os extratores
"""

__version__ = "1.0.0"
__author__ = "WACC Automation System"

from .base_extractor import BaseExtractor
from .fred_extractor import FREDExtractor
from .bcb_extractor import BCBExtractor
from .damodaran_extractor import DamodaranExtractor
from .web_scraper import WebScraper
from .wacc_data_manager import WACCDataManager

__all__ = [
    'BaseExtractor',
    'FREDExtractor',
    'BCBExtractor',
    'DamodaranExtractor',
    'WebScraper',
    'WACCDataManager'
]