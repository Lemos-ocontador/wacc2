#!/usr/bin/env python3
"""
Sistema de Categorização de Campos para Plataforma Anloc Valuation
Gerencia categorias e campos disponíveis para seleção no frontend
"""

import csv
import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime


def normalize_column_name(column_name: str) -> str:
    text = str(column_name).strip().lower()
    text = ''.join(ch if ch.isalnum() else '_' for ch in text)
    while '__' in text:
        text = text.replace('__', '_')
    return text.strip('_')


def get_column_overrides() -> dict[str, str]:
    return {
        normalize_column_name('EV/Sales'): 'ev_revenue',
        normalize_column_name('EV/EBITDA'): 'ev_ebitda',
        normalize_column_name('EV/EBIT'): 'ev_ebit',
        normalize_column_name('PBV'): 'pb_ratio',
        normalize_column_name('PEG'): 'peg_ratio',
        normalize_column_name('Forward PE'): 'forward_pe',
        normalize_column_name('Current PE'): 'current_pe',
        normalize_column_name('Trailing PE'): 'trailing_pe',
    }


def load_mapping_rows(mapping_path: Path) -> list[dict[str, str]]:
    if not mapping_path.exists():
        return []

    with mapping_path.open(newline='', encoding='utf-8') as handle:
        reader = csv.DictReader(handle)
        cleaned_rows = []
        for row in reader:
            cleaned_row = {}
            for key, value in row.items():
                if key is None:
                    continue
                clean_key = key.strip().lower().lstrip('\ufeff')
                cleaned_row[clean_key] = (value or '').strip()
            cleaned_rows.append(cleaned_row)
        return cleaned_rows


def build_field_label(campo: str, descricao: str) -> str:
    if descricao and not descricao.lower().startswith('campo:'):
        return descricao
    return campo


def infer_field_type(field_name: str, label: str) -> Tuple[str, int]:
    name = field_name.lower()
    text = f"{field_name} {label}".lower()

    ratio_fields = {
        'pe_ratio', 'forward_pe', 'current_pe', 'trailing_pe', 'pb_ratio',
        'peg_ratio', 'ev_ebitda', 'ev_revenue', 'ev_ebit', 'interest_coverage',
        'debt_equity', 'debt_to_equity_market', 'debt_to_capital_book',
        'liquidity_ratio'
    }

    percentage_keywords = ['%', 'percent', 'taxa', 'margem', 'yield', 'growth', 'return']
    currency_keywords = [
        'market cap', 'enterprise value', 'revenue', 'income', 'ebit', 'ebitda',
        'cash', 'debt', 'value', 'capital', 'price', 'dividend'
    ]

    if name in ratio_fields or 'ratio' in name:
        return 'ratio', 2

    if any(keyword in text for keyword in percentage_keywords):
        return 'percentage', 2

    if any(keyword in text for keyword in currency_keywords):
        return 'currency', 2

    return 'text', 2

class FieldCategoriesManager:
    def __init__(self, db_path="data/damodaran_data_new.db", mapping_csv="data/mapeamento_campos_damodaran_20250926_235026.csv"):
        self.db_path = db_path
        self.mapping_csv = mapping_csv
        
        # Definição das categorias e campos
        self.field_categories = {
            "IDENTIFICAÇÃO": {
                "icon": "🏢",
                "description": "Dados básicos de identificação da empresa",
                "fields": {
                    "company_name": {"label": "Nome da Empresa", "type": "text"},
                    "ticker": {"label": "Ticker", "type": "text"},
                    "exchange": {"label": "Bolsa", "type": "text"},
                    "country": {"label": "País", "type": "text"},
                    "sic_code": {"label": "Código SIC", "type": "text"}
                }
            },
            
            "CLASSIFICAÇÃO SETORIAL": {
                "icon": "🏭",
                "description": "Classificação por setor e indústria",
                "fields": {
                    "primary_sector": {"label": "Setor Primário", "type": "text"},
                    "industry_group": {"label": "Grupo Industrial", "type": "text"},
                    "industry": {"label": "Indústria", "type": "text"},
                    "broad_group": {"label": "Grupo Amplo", "type": "text"},
                    "sub_group": {"label": "Subgrupo", "type": "text"}
                }
            },
            
            "DADOS FINANCEIROS BÁSICOS": {
                "icon": "💰",
                "description": "Principais métricas financeiras",
                "fields": {
                    "market_cap": {"label": "Market Cap", "type": "currency", "format": "millions"},
                    "enterprise_value": {"label": "Enterprise Value", "type": "currency", "format": "millions"},
                    "revenue": {"label": "Receita", "type": "currency", "format": "millions"},
                    "net_income": {"label": "Lucro Líquido", "type": "currency", "format": "millions"},
                    "ebitda": {"label": "EBITDA", "type": "currency", "format": "millions"},
                    "free_cash_flow": {"label": "Free Cash Flow", "type": "currency", "format": "millions"},
                    "fcf_equity": {"label": "FCF to Equity", "type": "currency", "format": "millions"}
                }
            },
            
            "MÚLTIPLOS DE VALUATION": {
                "icon": "📊",
                "description": "Múltiplos e indicadores de valuation",
                "fields": {
                    "pe_ratio": {"label": "P/E Ratio", "type": "ratio", "decimals": 2},
                    "forward_pe": {"label": "Forward P/E", "type": "ratio", "decimals": 2},
                    "pb_ratio": {"label": "P/B Ratio", "type": "ratio", "decimals": 2},
                    "ev_ebitda": {"label": "EV/EBITDA", "type": "ratio", "decimals": 2},
                    "ev_revenue": {"label": "EV/Revenue", "type": "ratio", "decimals": 2},
                    "peg_ratio": {"label": "PEG Ratio", "type": "ratio", "decimals": 2}
                }
            },
            
            "RENTABILIDADE": {
                "icon": "📈",
                "description": "Indicadores de rentabilidade e eficiência",
                "fields": {
                    "roe": {"label": "ROE", "type": "percentage", "decimals": 2},
                    "roe_adjusted": {"label": "ROE Ajustado", "type": "percentage", "decimals": 2},
                    "roa": {"label": "ROA", "type": "percentage", "decimals": 2},
                    "roic_adjusted": {"label": "ROIC Ajustado", "type": "percentage", "decimals": 2},
                    "normalized_roic": {"label": "ROIC Normalizado", "type": "percentage", "decimals": 2},
                    "operating_margin": {"label": "Margem Operacional", "type": "percentage", "decimals": 2}
                }
            },
            
            "CRESCIMENTO": {
                "icon": "🚀",
                "description": "Métricas de crescimento histórico e projetado",
                "fields": {
                    "revenue_growth": {"label": "Crescimento Receita (1Y)", "type": "percentage", "decimals": 2},
                    "revenue_growth_5y": {"label": "Crescimento Receita (5Y)", "type": "percentage", "decimals": 2},
                    "expected_revenue_growth": {"label": "Crescimento Esperado", "type": "percentage", "decimals": 2}
                }
            },
            
            "RISCO E BETA": {
                "icon": "⚡",
                "description": "Métricas de risco e volatilidade",
                "fields": {
                    "beta": {"label": "Beta", "type": "ratio", "decimals": 3},
                    "beta_2y_modified": {"label": "Beta 2Y Modificado", "type": "ratio", "decimals": 3},
                    "beta_5y_modified": {"label": "Beta 5Y Modificado", "type": "ratio", "decimals": 3},
                    "bottom_up_levered_beta": {"label": "Beta Bottom-Up Alavancado", "type": "ratio", "decimals": 3},
                    "bottom_up_beta_sector": {"label": "Beta Setorial", "type": "ratio", "decimals": 3}
                }
            },
            
            "ESTRUTURA DE CAPITAL": {
                "icon": "🏦",
                "description": "Endividamento e estrutura de capital",
                "fields": {
                    "debt_equity": {"label": "Debt/Equity", "type": "ratio", "decimals": 2},
                    "debt_to_equity_market": {"label": "D/E Market Value", "type": "ratio", "decimals": 2},
                    "debt_to_capital_book": {"label": "D/C Book Value", "type": "ratio", "decimals": 2},
                    "total_debt_incl_leases": {"label": "Dívida Total (c/ Leases)", "type": "currency", "format": "millions"},
                    "interest_coverage": {"label": "Cobertura de Juros", "type": "ratio", "decimals": 2}
                }
            },
            
            "LIQUIDEZ E DIVIDENDOS": {
                "icon": "💧",
                "description": "Liquidez e política de dividendos",
                "fields": {
                    "liquidity_ratio": {"label": "Índice de Liquidez", "type": "ratio", "decimals": 2},
                    "dividend_yield": {"label": "Dividend Yield", "type": "percentage", "decimals": 2}
                }
            },
            
            "CAPITAL DE GIRO": {
                "icon": "🔄",
                "description": "Gestão de capital de giro",
                "fields": {
                    "working_capital_noncash": {"label": "Capital de Giro (ex-caixa)", "type": "currency", "format": "millions"},
                    "wc_pct_revenue": {"label": "CG % da Receita", "type": "percentage", "decimals": 2},
                    "change_working_capital": {"label": "Variação CG", "type": "currency", "format": "millions"}
                }
            },
            
            "INVESTIMENTOS": {
                "icon": "🏗️",
                "description": "Investimentos e CapEx",
                "fields": {
                    "net_capex": {"label": "CapEx Líquido", "type": "currency", "format": "millions"},
                    "capex_sales_ratio": {"label": "CapEx/Vendas", "type": "percentage", "decimals": 2}
                }
            },
            
            "DADOS DE MERCADO": {
                "icon": "📉",
                "description": "Preços e performance de mercado",
                "fields": {
                    "stock_price_end_year": {"label": "Preço Fim do Ano", "type": "currency", "format": "units"},
                    "price_change_2023": {"label": "Variação Preço 2023", "type": "percentage", "decimals": 2}
                }
            },
            
            "PRÊMIOS DE RISCO": {
                "icon": "🌍",
                "description": "Prêmios de risco por país e tamanho",
                "fields": {
                    "erp_for_country": {"label": "ERP do País", "type": "percentage", "decimals": 2}
                }
            },
            
            "VALOR PATRIMONIAL": {
                "icon": "🏛️",
                "description": "Valor patrimonial e book value",
                "fields": {
                    "book_value_equity": {"label": "Book Value do Equity", "type": "currency", "format": "millions"}
                }
            }
        }

        csv_categories = self._load_categories_from_csv()
        if csv_categories:
            self.field_categories = csv_categories
    
    def get_all_categories(self) -> Dict[str, Any]:
        """Retorna todas as categorias com seus campos"""
        return self.field_categories
    
    def get_category_fields(self, category_name: str) -> Dict[str, Any]:
        """Retorna campos de uma categoria específica"""
        return self.field_categories.get(category_name, {}).get("fields", {})
    
    def get_field_info(self, field_name: str) -> Dict[str, Any]:
        """Retorna informações de um campo específico"""
        for category_name, category_data in self.field_categories.items():
            if field_name in category_data.get("fields", {}):
                field_info = category_data["fields"][field_name].copy()
                field_info["category"] = category_name
                field_info["category_icon"] = category_data["icon"]
                return field_info
        return {}

    def _load_categories_from_csv(self) -> Dict[str, Any]:
        mapping_path = Path(self.mapping_csv)
        rows = load_mapping_rows(mapping_path)
        if not rows:
            return {}

        overrides = get_column_overrides()
        icon_map = {
            'DADOS DE CONTROLE E METADADOS': '🧾',
            'DADOS DE MERCADO': '📉',
            'DADOS FINANCEIROS': '💰',
            'DADOS INSTITUCIONAIS': '🏢',
            'LIQUIDEZ E CAPITAL DE GIRO': '💧',
            'CRESCIMENTO': '🚀',
            'RENTABILIDADE': '📈',
            'ESTRUTURA DE CAPITAL': '🏦',
            'MÉTRICAS DE VALUATION': '📊',
            'OUTROS/NÃO CLASSIFICADO': '📦'
        }

        categories: Dict[str, Any] = {}
        seen_fields: set[str] = set()

        for row in rows:
            campo = row.get('campo', '').strip()
            categoria = row.get('categoria', '').strip() or 'OUTROS/NÃO CLASSIFICADO'
            descricao = row.get('descricao', '').strip()

            if not campo:
                continue

            normalized = normalize_column_name(campo)
            field_name = overrides.get(normalized, normalized)

            if field_name in seen_fields:
                continue
            seen_fields.add(field_name)

            label = build_field_label(campo, descricao)
            field_type, decimals = infer_field_type(field_name, label)

            if categoria not in categories:
                categories[categoria] = {
                    'icon': icon_map.get(categoria, '📊'),
                    'description': '',
                    'fields': {}
                }

            categories[categoria]['fields'][field_name] = {
                'label': label,
                'type': field_type,
                'decimals': decimals
            }

        return categories
    
    def get_available_fields_from_db(self) -> List[str]:
        """Obtém lista de campos disponíveis no banco de dados"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Obter estrutura da tabela
            cursor.execute("PRAGMA table_info(damodaran_global)")
            columns = cursor.fetchall()
            
            field_names = [col[1] for col in columns]
            conn.close()
            
            return field_names
            
        except Exception as e:
            print(f"Erro ao obter campos do banco: {e}")
            return []
    
    def validate_field_availability(self) -> Dict[str, Any]:
        """Valida quais campos definidos estão disponíveis no banco"""
        db_fields = set(self.get_available_fields_from_db())
        
        validation_result = {
            "available_categories": {},
            "missing_fields": [],
            "extra_fields": [],
            "summary": {}
        }
        
        # Campos definidos nas categorias
        defined_fields = set()
        for category_name, category_data in self.field_categories.items():
            category_fields = category_data.get("fields", {})
            defined_fields.update(category_fields.keys())
            
            # Verificar disponibilidade por categoria
            available_fields = {}
            missing_fields = []
            
            for field_name, field_info in category_fields.items():
                if field_name in db_fields:
                    available_fields[field_name] = field_info
                else:
                    missing_fields.append(field_name)
            
            validation_result["available_categories"][category_name] = {
                "icon": category_data["icon"],
                "description": category_data["description"],
                "fields": available_fields,
                "missing_fields": missing_fields,
                "availability_pct": len(available_fields) / len(category_fields) * 100 if category_fields else 0
            }
        
        # Campos faltantes e extras
        validation_result["missing_fields"] = list(defined_fields - db_fields)
        validation_result["extra_fields"] = list(db_fields - defined_fields)
        
        # Resumo
        validation_result["summary"] = {
            "total_defined_fields": len(defined_fields),
            "total_db_fields": len(db_fields),
            "available_fields": len(defined_fields & db_fields),
            "missing_fields": len(defined_fields - db_fields),
            "extra_fields": len(db_fields - defined_fields),
            "coverage_pct": len(defined_fields & db_fields) / len(defined_fields) * 100 if defined_fields else 0
        }
        
        return validation_result
    
    def get_field_statistics(self, field_names: List[str] = None) -> Dict[str, Any]:
        """Obtém estatísticas de completude dos campos"""
        if field_names is None:
            field_names = []
            for category_data in self.field_categories.values():
                field_names.extend(category_data.get("fields", {}).keys())
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total de registros
            cursor.execute("SELECT COUNT(*) FROM damodaran_global")
            total_records = cursor.fetchone()[0]
            
            statistics = {}
            
            for field_name in field_names:
                try:
                    # Contar registros não nulos
                    cursor.execute(f"SELECT COUNT(*) FROM damodaran_global WHERE {field_name} IS NOT NULL")
                    non_null_count = cursor.fetchone()[0]
                    
                    # Estatísticas básicas para campos numéricos
                    cursor.execute(f"SELECT MIN({field_name}), MAX({field_name}), AVG({field_name}) FROM damodaran_global WHERE {field_name} IS NOT NULL")
                    stats = cursor.fetchone()
                    
                    statistics[field_name] = {
                        "total_records": total_records,
                        "non_null_records": non_null_count,
                        "completeness_pct": (non_null_count / total_records * 100) if total_records > 0 else 0,
                        "min_value": stats[0] if stats[0] is not None else None,
                        "max_value": stats[1] if stats[1] is not None else None,
                        "avg_value": round(stats[2], 4) if stats[2] is not None else None
                    }
                    
                except Exception as e:
                    statistics[field_name] = {
                        "error": str(e),
                        "total_records": total_records,
                        "non_null_records": 0,
                        "completeness_pct": 0
                    }
            
            conn.close()
            return statistics
            
        except Exception as e:
            print(f"Erro ao obter estatísticas: {e}")
            return {}
    
    def export_categories_json(self, include_statistics=True) -> str:
        """Exporta categorias para JSON para uso no frontend"""
        validation = self.validate_field_availability()
        
        export_data = {
            "categories": validation["available_categories"],
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "summary": validation["summary"]
            }
        }
        
        if include_statistics:
            # Obter estatísticas apenas dos campos disponíveis
            available_fields = []
            for category_data in validation["available_categories"].values():
                available_fields.extend(category_data["fields"].keys())
            
            statistics = self.get_field_statistics(available_fields)
            export_data["field_statistics"] = statistics
        
        return json.dumps(export_data, indent=2, ensure_ascii=False)

def main():
    """Função principal para teste"""
    manager = FieldCategoriesManager()
    
    print("🔍 VALIDAÇÃO DE CAMPOS E CATEGORIAS")
    print("=" * 50)
    
    # Validar disponibilidade
    validation = manager.validate_field_availability()
    
    print(f"\n📊 RESUMO:")
    summary = validation["summary"]
    print(f"   Campos definidos: {summary['total_defined_fields']}")
    print(f"   Campos no banco: {summary['total_db_fields']}")
    print(f"   Campos disponíveis: {summary['available_fields']}")
    print(f"   Cobertura: {summary['coverage_pct']:.1f}%")
    
    print(f"\n📋 CATEGORIAS DISPONÍVEIS:")
    for category_name, category_data in validation["available_categories"].items():
        available_count = len(category_data["fields"])
        total_count = available_count + len(category_data["missing_fields"])
        availability = category_data["availability_pct"]
        
        status = "✅" if availability == 100 else "⚠️" if availability > 50 else "❌"
        print(f"   {status} {category_data['icon']} {category_name}: {available_count}/{total_count} campos ({availability:.1f}%)")
    
    # Exportar para JSON
    json_data = manager.export_categories_json()
    
    # Salvar arquivo
    filename = f"field_categories_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(json_data)
    
    print(f"\n💾 Arquivo exportado: {filename}")

if __name__ == "__main__":
    main()