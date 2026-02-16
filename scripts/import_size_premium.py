import sqlite3
import json
import os

def import_size_premium_data():
    """
    Importa dados de size premium do arquivo BDSize.json para o banco de dados.
    """
    print("=== IMPORTAÇÃO DE DADOS DE SIZE PREMIUM ===")
    
    # Caminhos dos arquivos
    json_file = "wacc4/static/BDSize.json"
    db_file = "data/damodaran_data_new.db"
    
    # Verificar se o arquivo JSON existe
    if not os.path.exists(json_file):
        print(f"❌ Arquivo não encontrado: {json_file}")
        return False
    
    try:
        # Carregar dados do JSON
        with open(json_file, 'r', encoding='utf-8') as f:
            size_data = json.load(f)
        
        print(f"✅ Dados carregados: {len(size_data)} registros")
        
        # Conectar ao banco de dados
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Criar tabela size_premium (drop se existir)
        cursor.execute("DROP TABLE IF EXISTS size_premium")
        
        cursor.execute("""
        CREATE TABLE size_premium (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            size_decile INTEGER NOT NULL,
            market_cap_min REAL NOT NULL,
            market_cap_max REAL NOT NULL,
            premium_decimal REAL NOT NULL,
            premium_percentage TEXT NOT NULL,
            reference_year INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        print("✅ Tabela size_premium criada")
        
        # Processar e inserir dados
        for record in size_data:
            # Limpar e converter valores monetários
            market_cap_min = float(record[' De '].replace(' ', '').replace('.', '').replace(',', '.'))
            market_cap_max = float(record[' até '].replace(' ', '').replace('.', '').replace(',', '.'))
            
            # Converter prêmio para decimal
            premium_str = record['Premio'].replace('%', '').replace(',', '.')
            premium_decimal = float(premium_str) / 100
            
            # Inserir dados
            cursor.execute("""
            INSERT INTO size_premium 
            (size_decile, market_cap_min, market_cap_max, premium_decimal, premium_percentage, reference_year)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                record['Tamanho'],
                market_cap_min,
                market_cap_max,
                premium_decimal,
                record['Premio'],
                record['[ANO_REFER]']
            ))
        
        # Commit e fechar
        conn.commit()
        conn.close()
        
        print(f"✅ {len(size_data)} registros inseridos com sucesso")
        
        # Verificar dados inseridos
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM size_premium")
        count = cursor.fetchone()[0]
        
        cursor.execute("""
        SELECT size_decile, market_cap_min, market_cap_max, premium_percentage 
        FROM size_premium 
        ORDER BY size_decile 
        LIMIT 5
        """)
        
        sample_data = cursor.fetchall()
        
        print(f"\n=== VERIFICAÇÃO ===")
        print(f"Total de registros na tabela: {count}")
        print("\nPrimeiros 5 registros:")
        for row in sample_data:
            print(f"  Decil {row[0]}: R$ {row[1]:,.0f} - R$ {row[2]:,.0f} | Prêmio: {row[3]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro durante importação: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = import_size_premium_data()
    if success:
        print("\n🎉 Importação concluída com sucesso!")
    else:
        print("\n💥 Falha na importação")