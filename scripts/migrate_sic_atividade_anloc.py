"""
Script para criar tabela de lookup SIC e popular os campos
sic_desc, sic_round e atividade_anloc na tabela damodaran_global.

Usa o arquivo data/link_cnae_sic_damodaran_anloc.xlsx como fonte de mapeamento.

Estratégia de lookup:
  1. Tenta match direto: sic_code == SIC (coluna do Excel)
  2. Fallback: arredonda sic_code para centenas (floor(sic_code/100)*100) e busca por sic_round
"""

import sqlite3
import pandas as pd
import sys
import os
from datetime import datetime

DB_PATH = "data/damodaran_data_new.db"
EXCEL_PATH = "data/link_cnae_sic_damodaran_anloc.xlsx"


def load_sic_mappings(excel_path):
    """Carrega os mapeamentos SIC do Excel e retorna dois dicts de lookup."""
    df = pd.read_excel(excel_path, engine='openpyxl')
    
    # Lookup por SIC exato (coluna SIC do Excel -> int)
    df_sic = df.dropna(subset=['SIC']).copy()
    df_sic['SIC_int'] = df_sic['SIC'].astype(int)
    # Deduplica por SIC exato (pega o primeiro)
    df_sic_unique = df_sic.drop_duplicates(subset=['SIC_int'])
    
    sic_direct = {}
    for _, row in df_sic_unique.iterrows():
        sic_direct[row['SIC_int']] = {
            'sic_desc': str(row['SIC_DESC']) if pd.notna(row['SIC_DESC']) else None,
            'sic_round': int(row['sic_round']),
            'atividade_anloc': str(row['[atividade_anloc]']) if pd.notna(row['[atividade_anloc]']) else None,
        }
    
    # Lookup por sic_round (fallback)
    df_round = df.drop_duplicates(subset=['sic_round'])
    sic_round_map = {}
    for _, row in df_round.iterrows():
        sic_round_map[int(row['sic_round'])] = {
            'sic_desc': str(row['SIC_DESC']) if pd.notna(row['SIC_DESC']) else None,
            'sic_round': int(row['sic_round']),
            'atividade_anloc': str(row['[atividade_anloc]']) if pd.notna(row['[atividade_anloc]']) else None,
        }
    
    print(f"Mapeamentos carregados: {len(sic_direct)} SIC diretos, {len(sic_round_map)} sic_round")
    return sic_direct, sic_round_map


def lookup_sic(sic_code_str, sic_direct, sic_round_map):
    """Busca sic_desc, sic_round e atividade_anloc para um sic_code."""
    if not sic_code_str:
        return None, None, None
    
    try:
        sic_int = int(float(sic_code_str))
    except (ValueError, TypeError):
        return None, None, None
    
    # 1. Tenta match direto
    if sic_int in sic_direct:
        info = sic_direct[sic_int]
        return info['sic_desc'], info['sic_round'], info['atividade_anloc']
    
    # 2. Fallback: arredonda para centenas
    sic_r = (sic_int // 100) * 100
    if sic_r in sic_round_map:
        info = sic_round_map[sic_r]
        return info['sic_desc'], sic_r, info['atividade_anloc']
    
    return None, None, None


def create_sic_lookup_table(conn, sic_direct, sic_round_map):
    """Cria tabela sic_lookup no banco para consultas futuras."""
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS sic_lookup")
    cur.execute("""
        CREATE TABLE sic_lookup (
            sic_code INTEGER PRIMARY KEY,
            sic_desc TEXT,
            sic_round INTEGER,
            atividade_anloc TEXT
        )
    """)
    
    # Inserir todos os SIC diretos
    for sic, info in sic_direct.items():
        cur.execute(
            "INSERT OR REPLACE INTO sic_lookup (sic_code, sic_desc, sic_round, atividade_anloc) VALUES (?, ?, ?, ?)",
            (sic, info['sic_desc'], info['sic_round'], info['atividade_anloc'])
        )
    
    # Criar tabela auxiliar para lookup por sic_round
    cur.execute("DROP TABLE IF EXISTS sic_round_lookup")
    cur.execute("""
        CREATE TABLE sic_round_lookup (
            sic_round INTEGER PRIMARY KEY,
            sic_desc TEXT,
            atividade_anloc TEXT
        )
    """)
    
    for sic_r, info in sic_round_map.items():
        cur.execute(
            "INSERT OR REPLACE INTO sic_round_lookup (sic_round, sic_desc, atividade_anloc) VALUES (?, ?, ?)",
            (sic_r, info['sic_desc'], info['atividade_anloc'])
        )
    
    conn.commit()
    print(f"Tabela sic_lookup criada: {len(sic_direct)} registros")
    print(f"Tabela sic_round_lookup criada: {len(sic_round_map)} registros")


def add_columns_if_needed(conn):
    """Adiciona as colunas sic_desc, sic_round e atividade_anloc se não existem."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(damodaran_global)")
    existing_cols = {row[1] for row in cur.fetchall()}
    
    new_cols = {
        'sic_desc': 'TEXT',
        'sic_round': 'INTEGER',
        'atividade_anloc': 'TEXT',
    }
    
    for col, col_type in new_cols.items():
        if col not in existing_cols:
            cur.execute(f'ALTER TABLE damodaran_global ADD COLUMN {col} {col_type}')
            print(f"  Coluna '{col}' ({col_type}) adicionada à tabela damodaran_global")
        else:
            print(f"  Coluna '{col}' já existe")
    
    conn.commit()


def populate_sic_fields(conn, sic_direct, sic_round_map):
    """Popula sic_desc, sic_round e atividade_anloc para todos os registros."""
    cur = conn.cursor()
    
    # Buscar todos os registros com sic_code
    cur.execute("SELECT id, sic_code FROM damodaran_global WHERE sic_code IS NOT NULL AND sic_code != ''")
    rows = cur.fetchall()
    
    total = len(rows)
    updated = 0
    no_match = 0
    
    print(f"\nPopulando {total} registros...")
    
    batch = []
    for i, (row_id, sic_code) in enumerate(rows):
        sic_desc, sic_round_val, atividade = lookup_sic(sic_code, sic_direct, sic_round_map)
        
        if sic_desc or atividade:
            batch.append((sic_desc, sic_round_val, atividade, row_id))
            updated += 1
        else:
            no_match += 1
        
        # Commit em lotes de 5000
        if len(batch) >= 5000:
            cur.executemany(
                "UPDATE damodaran_global SET sic_desc = ?, sic_round = ?, atividade_anloc = ? WHERE id = ?",
                batch
            )
            conn.commit()
            batch = []
            print(f"  Progresso: {i+1}/{total} ({(i+1)/total*100:.1f}%)")
    
    # Commit do último lote
    if batch:
        cur.executemany(
            "UPDATE damodaran_global SET sic_desc = ?, sic_round = ?, atividade_anloc = ? WHERE id = ?",
            batch
        )
        conn.commit()
    
    print(f"\nResultado:")
    print(f"  Total processado: {total}")
    print(f"  Atualizados: {updated} ({updated/total*100:.1f}%)")
    print(f"  Sem match: {no_match} ({no_match/total*100:.1f}%)")


def main():
    if not os.path.exists(EXCEL_PATH):
        print(f"ERRO: Arquivo Excel não encontrado: {EXCEL_PATH}")
        sys.exit(1)
    
    if not os.path.exists(DB_PATH):
        print(f"ERRO: Banco de dados não encontrado: {DB_PATH}")
        sys.exit(1)
    
    print(f"{'='*60}")
    print(f"Migração SIC -> atividade_anloc")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Excel: {EXCEL_PATH}")
    print(f"DB: {DB_PATH}")
    print(f"{'='*60}\n")
    
    # 1. Carregar mapeamentos do Excel
    print("1. Carregando mapeamentos do Excel...")
    sic_direct, sic_round_map = load_sic_mappings(EXCEL_PATH)
    
    # 2. Conectar ao banco
    print("\n2. Conectando ao banco de dados...")
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # 3. Criar tabelas de lookup
        print("\n3. Criando tabelas de lookup...")
        create_sic_lookup_table(conn, sic_direct, sic_round_map)
        
        # 4. Adicionar colunas
        print("\n4. Verificando/adicionando colunas...")
        add_columns_if_needed(conn)
        
        # 5. Popular campos
        print("\n5. Populando campos sic_desc, sic_round e atividade_anloc...")
        populate_sic_fields(conn, sic_direct, sic_round_map)
        
        # 6. Verificação final
        print("\n6. Verificação final...")
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM damodaran_global WHERE atividade_anloc IS NOT NULL")
        count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM damodaran_global")
        total = cur.fetchone()[0]
        print(f"  Registros com atividade_anloc: {count}/{total} ({count/total*100:.1f}%)")
        
        # Amostra
        cur.execute("""
            SELECT sic_code, sic_desc, sic_round, atividade_anloc, company_name 
            FROM damodaran_global 
            WHERE atividade_anloc IS NOT NULL 
            LIMIT 10
        """)
        print("\n  Amostra:")
        for row in cur.fetchall():
            print(f"    SIC={row[0]}, DESC={row[1][:30] if row[1] else 'N/A'}, "
                  f"ROUND={row[2]}, ATIV={row[3][:35] if row[3] else 'N/A'}, "
                  f"EMPRESA={row[4][:25] if row[4] else 'N/A'}")
        
        print(f"\n{'='*60}")
        print("Migração concluída com sucesso!")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
