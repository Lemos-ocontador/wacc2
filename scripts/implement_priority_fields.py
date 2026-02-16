#!/usr/bin/env python3
"""
Script para implementar campos prioritários do Damodaran na base de dados
Implementa campos críticos, alta prioridade e média prioridade de forma estruturada
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import json
import os

class DamodaranFieldsImplementer:
    def __init__(self, db_path="data/damodaran_data_new.db", excel_path="globalcompfirms2025.xlsx"):
        self.db_path = db_path
        self.excel_path = excel_path
        self.conn = None
        self.df_excel = None
        
    def connect_db(self):
        """Conecta ao banco de dados"""
        self.conn = sqlite3.connect(self.db_path)
        print(f"✅ Conectado ao banco: {self.db_path}")
        
    def load_excel_data(self):
        """Carrega dados do Excel"""
        try:
            self.df_excel = pd.read_excel(self.excel_path)
            print(f"✅ Excel carregado: {len(self.df_excel)} registros, {len(self.df_excel.columns)} colunas")
            return True
        except Exception as e:
            print(f"❌ Erro ao carregar Excel: {e}")
            return False
    
    def get_current_table_structure(self):
        """Obtém estrutura atual da tabela"""
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(damodaran_global)")
        columns = cursor.fetchall()
        current_fields = [col[1] for col in columns]
        print(f"📋 Campos atuais na tabela: {len(current_fields)}")
        return current_fields
    
    def define_priority_fields(self):
        """Define os campos por prioridade baseado na análise"""
        
        # 🔴 PRIORIDADE CRÍTICA
        critical_fields = {
            # Múltiplos essenciais
            'ev_ebitda': {
                'excel_name': 'EV/EBITDA',
                'sql_type': 'REAL',
                'description': 'Múltiplo Enterprise Value / EBITDA'
            },
            'ev_revenue': {
                'excel_name': 'EV/Revenues',
                'sql_type': 'REAL', 
                'description': 'Múltiplo Enterprise Value / Revenue'
            },
            # Métricas de crescimento
            'revenue_growth_5y': {
                'excel_name': 'Revenue Growth (last 5 yeers)',
                'sql_type': 'REAL',
                'description': 'Crescimento da receita nos últimos 5 anos (%)'
            },
            'expected_revenue_growth': {
                'excel_name': 'Expected revenue Growth (next 5 years)',
                'sql_type': 'REAL',
                'description': 'Crescimento esperado da receita próximos 5 anos (%)'
            },
            # Free Cash Flow
            'free_cash_flow': {
                'excel_name': 'Free Cash Flow to Firm',
                'sql_type': 'REAL',
                'description': 'Fluxo de Caixa Livre da Empresa'
            },
            'fcf_equity': {
                'excel_name': 'Free Cash Flow to Equity',
                'sql_type': 'REAL',
                'description': 'Fluxo de Caixa Livre do Acionista'
            },
            # Múltiplos prospectivos
            'forward_pe': {
                'excel_name': 'Forward PE',
                'sql_type': 'REAL',
                'description': 'P/E Ratio prospectivo'
            },
            'peg_ratio': {
                'excel_name': 'PEG',
                'sql_type': 'REAL',
                'description': 'PEG Ratio (P/E dividido por crescimento)'
            }
        }
        
        # 🟡 PRIORIDADE ALTA
        high_priority_fields = {
            # Rentabilidade detalhada
            'roic_adjusted': {
                'excel_name': 'ROIC (adj for R&D)',
                'sql_type': 'REAL',
                'description': 'ROIC ajustado para P&D (%)'
            },
            'roe_adjusted': {
                'excel_name': 'ROE (adj for R&D)',
                'sql_type': 'REAL',
                'description': 'ROE ajustado para P&D (%)'
            },
            'normalized_roic': {
                'excel_name': 'Normalized ROIC',
                'sql_type': 'REAL',
                'description': 'ROIC normalizado (%)'
            },
            # Book Value e PB
            'book_value_equity': {
                'excel_name': 'Current Book Value of Equity',
                'sql_type': 'REAL',
                'description': 'Valor patrimonial atual'
            },
            'pb_ratio': {
                'excel_name': 'PBV',
                'sql_type': 'REAL',
                'description': 'Múltiplo Preço/Valor Patrimonial'
            },
            # Debt ratios detalhados
            'debt_to_equity_market': {
                'excel_name': 'Market Debt to Equity ratio',
                'sql_type': 'REAL',
                'description': 'Dívida/Patrimônio a valor de mercado'
            },
            'debt_to_capital_book': {
                'excel_name': 'Book Debt to capital ratio',
                'sql_type': 'REAL',
                'description': 'Dívida/Capital a valor contábil'
            },
            'total_debt_incl_leases': {
                'excel_name': 'Total Debt incl leases (in US $)',
                'sql_type': 'REAL',
                'description': 'Dívida total incluindo leases (US$)'
            },
            # Liquidity ratios
            'liquidity_ratio': {
                'excel_name': 'Liquidity Ratio (Annual trading volume/Shrs outs)',
                'sql_type': 'REAL',
                'description': 'Ratio de liquidez (Volume/Ações em circulação)'
            },
            'interest_coverage': {
                'excel_name': 'Interest coverage ratio',
                'sql_type': 'REAL',
                'description': 'Cobertura de juros'
            }
        }
        
        # 🟢 PRIORIDADE MÉDIA
        medium_priority_fields = {
            # Dados históricos de preços
            'price_change_2023': {
                'excel_name': '% Price Change: 2023',
                'sql_type': 'REAL',
                'description': 'Variação do preço em 2023 (%)'
            },
            'stock_price_end_year': {
                'excel_name': 'Stock price (End of most recent year)in US$',
                'sql_type': 'REAL',
                'description': 'Preço da ação no final do ano (US$)'
            },
            # Beta ajustado
            'beta_2y_modified': {
                'excel_name': 'Modified 2-year beta',
                'sql_type': 'REAL',
                'description': 'Beta modificado 2 anos'
            },
            'beta_5y_modified': {
                'excel_name': 'Modified 5-year beta',
                'sql_type': 'REAL',
                'description': 'Beta modificado 5 anos'
            },
            'bottom_up_levered_beta': {
                'excel_name': 'Bottom up levered beta',
                'sql_type': 'REAL',
                'description': 'Beta alavancado bottom-up'
            },
            # Working Capital
            'working_capital_noncash': {
                'excel_name': 'Non-cash Working Capital',
                'sql_type': 'REAL',
                'description': 'Capital de giro não-caixa'
            },
            'wc_pct_revenue': {
                'excel_name': 'Non-cash Working Capital as % of Revenues',
                'sql_type': 'REAL',
                'description': 'Capital de giro como % da receita'
            },
            'change_working_capital': {
                'excel_name': 'Change in non-cash Working capital',
                'sql_type': 'REAL',
                'description': 'Variação do capital de giro'
            },
            # CapEx
            'net_capex': {
                'excel_name': 'Net Cap Ex',
                'sql_type': 'REAL',
                'description': 'CapEx líquido'
            },
            'capex_sales_ratio': {
                'excel_name': 'Sales/Capital',
                'sql_type': 'REAL',
                'description': 'Vendas/Capital (eficiência de capital)'
            }
        }
        
        return critical_fields, high_priority_fields, medium_priority_fields
    
    def add_columns_to_table(self, fields_dict, priority_name):
        """Adiciona colunas à tabela existente"""
        cursor = self.conn.cursor()
        added_count = 0
        
        print(f"\n🔄 Adicionando campos de {priority_name}...")
        
        for field_name, field_info in fields_dict.items():
            try:
                sql = f"ALTER TABLE damodaran_global ADD COLUMN {field_name} {field_info['sql_type']}"
                cursor.execute(sql)
                print(f"   ✅ {field_name} - {field_info['description']}")
                added_count += 1
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    print(f"   ⚠️  {field_name} - Já existe")
                else:
                    print(f"   ❌ {field_name} - Erro: {e}")
        
        self.conn.commit()
        print(f"✅ {added_count} campos adicionados para {priority_name}")
        return added_count
    
    def map_excel_to_db_fields(self, fields_dict):
        """Mapeia campos do Excel para campos do DB"""
        mapping = {}
        excel_columns = list(self.df_excel.columns)
        
        for db_field, field_info in fields_dict.items():
            excel_name = field_info['excel_name']
            
            # Busca exata
            if excel_name in excel_columns:
                mapping[db_field] = excel_name
            else:
                # Busca aproximada (case insensitive)
                for col in excel_columns:
                    if excel_name.lower() in col.lower() or col.lower() in excel_name.lower():
                        mapping[db_field] = col
                        break
        
        return mapping
    
    def update_data_from_excel(self, fields_dict, priority_name):
        """Atualiza dados da tabela com valores do Excel"""
        mapping = self.map_excel_to_db_fields(fields_dict)
        
        if not mapping:
            print(f"⚠️  Nenhum campo mapeado para {priority_name}")
            return 0
        
        print(f"\n🔄 Atualizando dados de {priority_name}...")
        print(f"   Campos mapeados: {len(mapping)}")
        
        cursor = self.conn.cursor()
        updated_count = 0
        
        # Para cada campo mapeado
        for db_field, excel_field in mapping.items():
            try:
                # Prepara dados do Excel
                excel_data = self.df_excel[['Company Name', excel_field]].dropna()
                
                if len(excel_data) == 0:
                    print(f"   ⚠️  {db_field}: Sem dados no Excel")
                    continue
                
                # Atualiza registros no DB
                updates = 0
                for _, row in excel_data.iterrows():
                    company_name = row['Company Name']
                    value = row[excel_field]
                    
                    # Converte valor se necessário
                    if pd.isna(value):
                        continue
                    
                    # Atualiza no DB
                    sql = f"""
                    UPDATE damodaran_global 
                    SET {db_field} = ? 
                    WHERE company_name = ?
                    """
                    cursor.execute(sql, (value, company_name))
                    if cursor.rowcount > 0:
                        updates += 1
                
                print(f"   ✅ {db_field}: {updates} registros atualizados")
                updated_count += updates
                
            except Exception as e:
                print(f"   ❌ {db_field}: Erro - {e}")
        
        self.conn.commit()
        print(f"✅ Total de {updated_count} atualizações para {priority_name}")
        return updated_count
    
    def create_backup(self):
        """Cria backup da base antes das alterações"""
        backup_name = f"damodaran_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        
        try:
            import shutil
            shutil.copy2(self.db_path, backup_name)
            print(f"✅ Backup criado: {backup_name}")
            return backup_name
        except Exception as e:
            print(f"❌ Erro ao criar backup: {e}")
            return None
    
    def generate_implementation_report(self, results):
        """Gera relatório da implementação"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"implementation_report_{timestamp}.json"
        
        report = {
            "timestamp": timestamp,
            "database": self.db_path,
            "excel_source": self.excel_path,
            "results": results,
            "summary": {
                "total_fields_added": sum(r.get('fields_added', 0) for r in results.values()),
                "total_records_updated": sum(r.get('records_updated', 0) for r in results.values())
            }
        }
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"📊 Relatório gerado: {report_file}")
        return report_file
    
    def implement_all_priorities(self):
        """Implementa todos os campos por prioridade"""
        print("🚀 IMPLEMENTAÇÃO DE CAMPOS PRIORITÁRIOS DAMODARAN")
        print("=" * 60)
        
        # Conecta e carrega dados
        self.connect_db()
        if not self.load_excel_data():
            return False
        
        # Cria backup
        backup_file = self.create_backup()
        
        # Define campos por prioridade
        critical, high_priority, medium_priority = self.define_priority_fields()
        
        results = {}
        
        # Implementa por prioridade
        priorities = [
            ("🔴 PRIORIDADE CRÍTICA", critical),
            ("🟡 PRIORIDADE ALTA", high_priority),
            ("🟢 PRIORIDADE MÉDIA", medium_priority)
        ]
        
        for priority_name, fields_dict in priorities:
            print(f"\n{priority_name}")
            print("-" * 40)
            
            # Adiciona colunas
            fields_added = self.add_columns_to_table(fields_dict, priority_name)
            
            # Atualiza dados
            records_updated = self.update_data_from_excel(fields_dict, priority_name)
            
            results[priority_name] = {
                "fields_added": fields_added,
                "records_updated": records_updated,
                "fields_planned": len(fields_dict)
            }
        
        # Gera relatório
        report_file = self.generate_implementation_report(results)
        
        # Fecha conexão
        self.conn.close()
        
        # Resumo final
        print(f"\n🎯 IMPLEMENTAÇÃO CONCLUÍDA!")
        print(f"   Backup: {backup_file}")
        print(f"   Relatório: {report_file}")
        
        total_added = sum(r['fields_added'] for r in results.values())
        total_updated = sum(r['records_updated'] for r in results.values())
        
        print(f"   Campos adicionados: {total_added}")
        print(f"   Registros atualizados: {total_updated}")
        
        return True

def main():
    """Função principal"""
    implementer = DamodaranFieldsImplementer()
    success = implementer.implement_all_priorities()
    
    if success:
        print("\n✅ Implementação realizada com sucesso!")
        print("\n💡 Próximos passos:")
        print("   • Verificar dados implementados")
        print("   • Atualizar interfaces da aplicação")
        print("   • Criar dashboards para novos campos")
        print("   • Testar cálculos e múltiplos")
    else:
        print("\n❌ Falha na implementação")

if __name__ == "__main__":
    main()