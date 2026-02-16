#!/usr/bin/env python3
"""
Catálogo detalhado das fontes de dados para automação do cálculo do WACC
Baseado na análise da planilha BDWACC.xlsx
"""

import json
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class DataSource:
    """Classe para representar uma fonte de dados."""
    name: str
    url: str
    data_type: str
    wacc_component: str
    update_frequency: str
    extraction_method: str
    description: str
    api_available: bool = False
    requires_auth: bool = False
    notes: str = ""

class WACCDataSourcesCatalog:
    """Catálogo de fontes de dados para cálculo do WACC."""
    
    def __init__(self):
        self.sources = self._initialize_sources()
        self.wacc_formula_components = self._define_wacc_components()
    
    def _initialize_sources(self) -> List[DataSource]:
        """Inicializa o catálogo de fontes de dados."""
        return [
            # === TAXA LIVRE DE RISCO (Risk-Free Rate) ===
            DataSource(
                name="US Treasury 10-Year Bond Yield",
                url="https://fred.stlouisfed.org/series/DGS10",
                data_type="Taxa de Juros",
                wacc_component="Risk-Free Rate (Rf)",
                update_frequency="Diário",
                extraction_method="FRED API",
                description="Taxa do título do tesouro americano de 10 anos - proxy para taxa livre de risco",
                api_available=True,
                requires_auth=True,
                notes="Fonte oficial do Federal Reserve. API gratuita com registro."
            ),
            
            DataSource(
                name="US Treasury 30-Year Bond Yield",
                url="https://br.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data",
                data_type="Taxa de Juros",
                wacc_component="Risk-Free Rate (Rf)",
                update_frequency="Diário",
                extraction_method="Web Scraping",
                description="Taxa do título do tesouro americano de 30 anos - alternativa para Rf",
                api_available=False,
                requires_auth=False,
                notes="Backup para dados do Treasury. Requer web scraping."
            ),
            
            # === PRÊMIO DE RISCO DE MERCADO (Market Risk Premium) ===
            DataSource(
                name="Damodaran Market Risk Premium",
                url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/data.html",
                data_type="Prêmio de Risco",
                wacc_component="Market Risk Premium (MRP)",
                update_frequency="Anual",
                extraction_method="Web Scraping + Excel Download",
                description="Prêmio de risco de mercado calculado por Damodaran (NYU Stern)",
                api_available=False,
                requires_auth=False,
                notes="Fonte acadêmica reconhecida. Dados históricos desde 1928."
            ),
            
            # === RISCO PAÍS (Country Risk Premium) ===
            DataSource(
                name="Damodaran Country Risk Premiums",
                url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/data.html",
                data_type="Risco País",
                wacc_component="Country Risk Premium (CRP)",
                update_frequency="Anual",
                extraction_method="Web Scraping + Excel Download",
                description="Prêmios de risco por país calculados por Damodaran",
                api_available=False,
                requires_auth=False,
                notes="Já implementado no sistema atual. Dados para 200+ países."
            ),
            
            # === BETA SETORIAL ===
            DataSource(
                name="Damodaran Industry Betas",
                url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/data.html",
                data_type="Beta Setorial",
                wacc_component="Beta (β)",
                update_frequency="Anual",
                extraction_method="Web Scraping + Excel Download",
                description="Betas por setor de atividade calculados por Damodaran",
                api_available=False,
                requires_auth=False,
                notes="Dados globais por setor. Inclui beta alavancado e desalavancado."
            ),
            
            # === DADOS DE EMPRESAS GLOBAIS ===
            DataSource(
                name="Damodaran Global Company Data",
                url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/data.html",
                data_type="Dados Empresariais",
                wacc_component="Beta, D/E Ratio, Tax Rate",
                update_frequency="Anual",
                extraction_method="Web Scraping + Excel Download",
                description="Dados financeiros de empresas globais (globalcompfirms{year}.xlsx)",
                api_available=False,
                requires_auth=False,
                notes="Já implementado no sistema atual. Dados de 40,000+ empresas."
            ),
            
            # === PRÊMIO POR TAMANHO (Size Premium) ===
            DataSource(
                name="Damodaran Size Premiums",
                url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/data.html",
                data_type="Prêmio por Tamanho",
                wacc_component="Size Premium (SP)",
                update_frequency="Anual",
                extraction_method="Web Scraping + Excel Download",
                description="Prêmios de risco baseados no tamanho da empresa",
                api_available=False,
                requires_auth=False,
                notes="Dados por faixa de market cap. Usado para ajustar o custo de capital próprio."
            ),
            
            # === INFLAÇÃO AMERICANA (CPI) ===
            DataSource(
                name="US Consumer Price Index (CPI)",
                url="https://fred.stlouisfed.org/series/FPCPITOTLZGUSA/",
                data_type="Inflação",
                wacc_component="Inflation Adjustment",
                update_frequency="Mensal",
                extraction_method="FRED API",
                description="Índice de preços ao consumidor americano - inflação",
                api_available=True,
                requires_auth=True,
                notes="Usado para ajustes de inflação entre países."
            ),
            
            DataSource(
                name="Philadelphia Fed CPI Expectations",
                url="https://www.philadelphiafed.org/surveys-and-data/cpi-spf",
                data_type="Expectativa de Inflação",
                wacc_component="Inflation Adjustment",
                update_frequency="Trimestral",
                extraction_method="Web Scraping",
                description="Expectativas de inflação do Philadelphia Fed",
                api_available=False,
                requires_auth=False,
                notes="Dados prospectivos de inflação."
            ),
            
            # === INFLAÇÃO BRASILEIRA (IPCA) ===
            DataSource(
                name="IPCA - Banco Central do Brasil",
                url="https://www3.bcb.gov.br/sgspub/",
                data_type="Inflação",
                wacc_component="Inflation Adjustment",
                update_frequency="Mensal",
                extraction_method="BCB API",
                description="Índice de Preços ao Consumidor Amplo - inflação brasileira",
                api_available=True,
                requires_auth=False,
                notes="API gratuita do Banco Central. Série 433 (IPCA mensal)."
            ),
            
            # === TAXA SELIC (Custo de Capital de Terceiros) ===
            DataSource(
                name="Taxa Selic - Banco Central do Brasil",
                url="https://www3.bcb.gov.br/sgspub/",
                data_type="Taxa de Juros",
                wacc_component="Cost of Debt (Kd)",
                update_frequency="Diário",
                extraction_method="BCB API",
                description="Taxa básica de juros da economia brasileira",
                api_available=True,
                requires_auth=False,
                notes="API gratuita do Banco Central. Série 432 (Selic diária)."
            ),
            
            # === DADOS MACROECONÔMICOS BRASILEIROS ===
            DataSource(
                name="Focus - Relatório de Mercado (BCB)",
                url="https://www3.bcb.gov.br/sgspub/",
                data_type="Expectativas Macroeconômicas",
                wacc_component="Economic Projections",
                update_frequency="Semanal",
                extraction_method="BCB API",
                description="Expectativas do mercado para indicadores macroeconômicos",
                api_available=True,
                requires_auth=False,
                notes="Inclui expectativas de IPCA, PIB, Selic, etc."
            )
        ]
    
    def _define_wacc_components(self) -> Dict[str, Dict]:
        """Define os componentes da fórmula do WACC."""
        return {
            "formula": "WACC = (E/V × Re) + (D/V × Rd × (1-T))",
            "components": {
                "Re": {
                    "name": "Cost of Equity (Custo do Capital Próprio)",
                    "formula": "Re = Rf + β × (MRP + CRP) + SP",
                    "subcomponents": {
                        "Rf": "Risk-Free Rate (Taxa Livre de Risco)",
                        "β": "Beta (Risco Sistemático)",
                        "MRP": "Market Risk Premium (Prêmio de Risco de Mercado)",
                        "CRP": "Country Risk Premium (Prêmio de Risco País)",
                        "SP": "Size Premium (Prêmio por Tamanho)"
                    }
                },
                "Rd": {
                    "name": "Cost of Debt (Custo do Capital de Terceiros)",
                    "formula": "Rd = Risk-Free Rate + Credit Spread",
                    "notes": "Pode usar múltiplo da Selic (ex: 1.5 × Selic)"
                },
                "E/V": {
                    "name": "Equity Weight (Peso do Capital Próprio)",
                    "formula": "E / (E + D)",
                    "source": "Market Cap / Enterprise Value"
                },
                "D/V": {
                    "name": "Debt Weight (Peso do Capital de Terceiros)",
                    "formula": "D / (E + D)",
                    "source": "Total Debt / Enterprise Value"
                },
                "T": {
                    "name": "Tax Rate (Alíquota de Imposto)",
                    "value": "34% (Brasil - IRPJ + CSLL)",
                    "notes": "Pode variar por país e empresa"
                }
            }
        }
    
    def get_sources_by_component(self, component: str) -> List[DataSource]:
        """Retorna fontes de dados por componente do WACC."""
        return [source for source in self.sources if component.lower() in source.wacc_component.lower()]
    
    def get_sources_by_frequency(self, frequency: str) -> List[DataSource]:
        """Retorna fontes de dados por frequência de atualização."""
        return [source for source in self.sources if frequency.lower() in source.update_frequency.lower()]
    
    def get_api_sources(self) -> List[DataSource]:
        """Retorna apenas fontes que possuem API disponível."""
        return [source for source in self.sources if source.api_available]
    
    def export_catalog(self, filename: str = "data/wacc_data_sources_catalog.json"):
        """Exporta o catálogo para arquivo JSON."""
        catalog_data = {
            "catalog_info": {
                "created_date": datetime.now().isoformat(),
                "total_sources": len(self.sources),
                "api_sources": len(self.get_api_sources()),
                "description": "Catálogo de fontes de dados para automação do cálculo do WACC"
            },
            "wacc_formula": self.wacc_formula_components,
            "data_sources": [
                {
                    "name": source.name,
                    "url": source.url,
                    "data_type": source.data_type,
                    "wacc_component": source.wacc_component,
                    "update_frequency": source.update_frequency,
                    "extraction_method": source.extraction_method,
                    "description": source.description,
                    "api_available": source.api_available,
                    "requires_auth": source.requires_auth,
                    "notes": source.notes
                }
                for source in self.sources
            ]
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(catalog_data, f, indent=2, ensure_ascii=False)
        
        print(f"Catálogo exportado para: {filename}")
    
    def print_summary(self):
        """Imprime um resumo do catálogo."""
        print("=== CATÁLOGO DE FONTES DE DADOS PARA WACC ===")
        print(f"Total de fontes: {len(self.sources)}")
        print(f"Fontes com API: {len(self.get_api_sources())}")
        print("\n=== COMPONENTES DO WACC ===")
        print(f"Fórmula: {self.wacc_formula_components['formula']}")
        
        print("\n=== FONTES POR COMPONENTE ===")
        components = set(source.wacc_component for source in self.sources)
        for component in sorted(components):
            sources = self.get_sources_by_component(component)
            print(f"\n{component}:")
            for source in sources:
                api_status = "[API]" if source.api_available else "[WEB]"
                print(f"  {api_status} {source.name}")
                print(f"      URL: {source.url}")
                print(f"      Frequência: {source.update_frequency}")
        
        print("\n=== PRÓXIMOS PASSOS ===")
        print("1. Implementar extratores para fontes com API")
        print("2. Desenvolver web scrapers para fontes sem API")
        print("3. Criar sistema de cache e atualização automática")
        print("4. Integrar com calculadora de WACC")

if __name__ == "__main__":
    # Criar e exportar catálogo
    catalog = WACCDataSourcesCatalog()
    catalog.print_summary()
    catalog.export_catalog()
    
    print("\n" + "="*60)
    print("Catálogo de fontes de dados criado com sucesso!")
    print("Arquivo gerado: data/wacc_data_sources_catalog.json")