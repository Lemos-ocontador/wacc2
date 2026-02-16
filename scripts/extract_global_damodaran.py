import sqlite3
import requests
import pandas as pd
import os
import io
from datetime import datetime
import re
import time

# URL base para os arquivos do Damodaran
base_url = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/"

# Nome do banco de dados
db_name = "data/damodaran_data_new.db"

# Configurações de otimização
CHUNK_SIZE = 1000  # Processar 1000 linhas por vez
BATCH_SIZE = 100   # Inserir 100 registros por transação

def get_available_years():
    """
    Busca os anos disponíveis dos arquivos globalcompfirms{ano}.xlsx no site do Damodaran
    """
    print("Buscando anos disponíveis no site do Damodaran...")
    
    try:
        # Fazer requisição para a página principal
        response = requests.get(base_url, timeout=30)
        if response.status_code != 200:
            print(f"Erro ao acessar o site: {response.status_code}")
            return []
        
        # Buscar por arquivos globalcompfirms{ano}.xlsx
        content = response.text
        pattern = r'globalcompfirms(\d{4})\.xlsx'
        matches = re.findall(pattern, content)
        
        # Converter para inteiros e ordenar
        years = sorted([int(year) for year in set(matches)])
        
        print(f"Anos encontrados: {years}")
        return years
    
    except Exception as e:
        print(f"Erro ao buscar anos disponíveis: {e}")
        return []

def download_file_with_progress(year):
    """
    Baixa um arquivo globalcompfirms{ano}.xlsx com indicador de progresso
    """
    filename = f"globalcompfirms{year}.xlsx"
    url = base_url + filename
    
    print(f"Baixando {filename}...")
    
    try:
        # Baixar o arquivo com timeout
        response = requests.get(url, timeout=60, stream=True)
        if response.status_code != 200:
            print(f"Erro ao baixar {filename}: {response.status_code}")
            return None
        
        # Obter tamanho do arquivo se disponível
        total_size = int(response.headers.get('content-length', 0))
        
        # Baixar em chunks
        content = b''
        downloaded = 0
        
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                content += chunk
                downloaded += len(chunk)
                if total_size > 0:
                    progress = (downloaded / total_size) * 100
                    print(f"\rProgresso: {progress:.1f}%", end='', flush=True)
        
        print(f"\n{filename} baixado com sucesso ({len(content)} bytes)")
        return content
    
    except Exception as e:
        print(f"Erro ao baixar {filename}: {e}")
        return None

def process_excel_in_chunks(excel_content, year):
    """
    Processa o arquivo Excel em chunks para otimizar memória
    """
    print(f"Processando arquivo do ano {year} em chunks...")
    
    try:
        # Ler apenas o cabeçalho primeiro para entender a estrutura
        df_header = pd.read_excel(io.BytesIO(excel_content), nrows=0)
        total_columns = len(df_header.columns)
        print(f"Arquivo tem {total_columns} colunas")
        
        # Ler o arquivo completo para saber quantas linhas tem
        df_full = pd.read_excel(io.BytesIO(excel_content))
        total_rows = len(df_full)
        print(f"Arquivo tem {total_rows} linhas")
        
        # Selecionar apenas colunas essenciais para reduzir uso de memória
        essential_columns = select_essential_columns(df_full.columns)
        print(f"Selecionadas {len(essential_columns)} colunas essenciais")
        
        # Filtrar apenas as colunas essenciais
        df_filtered = df_full[essential_columns].copy()
        df_filtered['year'] = year
        
        # Limpar memória
        del df_full
        
        return df_filtered
    
    except Exception as e:
        print(f"Erro ao processar Excel: {e}")
        return None

def select_essential_columns(all_columns):
    """
    Seleciona apenas as colunas essenciais para reduzir uso de memória
    Inclui campos específicos de subdivisões geográficas e setoriais
    """
    # Converter colunas para string e minúsculas para comparação
    columns_lower = [str(col).lower().strip() for col in all_columns]
    
    # Campos obrigatórios de subdivisões (nomes exatos)
    mandatory_subdivision_fields = [
        'Company Name',
        'Exchange:Ticker', 
        'Country',
        'Industry Group',
        'Primary Sector',
        'SIC Code',
        'Broad Group',
        'Sub Group',
        'ERP for Country',
        'Total Default Spread for cost of debt (Company + Country)',
        'Bottom up Beta for sector',
        'Price Change for Sub-group'
    ]
    
    # Definir padrões de colunas essenciais
    essential_patterns = [
        # Identificação da empresa
        r'.*company.*name.*',
        r'.*name.*',
        r'.*ticker.*',
        r'.*symbol.*',
        r'.*exchange.*',
        r'.*industry.*',
        r'.*sector.*',
        r'.*country.*',
        
        # Subdivisões geográficas e setoriais
        r'.*broad.*group.*',
        r'.*sub.*group.*',
        r'.*primary.*sector.*',
        r'.*sic.*code.*',
        r'.*erp.*country.*',
        r'.*default.*spread.*',
        
        # Métricas financeiras básicas
        r'.*market.*cap.*',
        r'.*mkt.*cap.*',
        r'.*enterprise.*value.*',
        r'.*revenue.*',
        r'.*sales.*',
        r'.*net.*income.*',
        r'.*ebitda.*',
        r'.*pe.*ratio.*',
        r'.*pe$',
        r'.*beta.*',
        r'.*debt.*equity.*',
        r'.*roe.*',
        r'.*roa.*',
        r'.*dividend.*yield.*',
        
        # Crescimento
        r'.*growth.*',
        r'.*margin.*'
    ]
    
    selected_columns = []
    
    # Primeiro, adicionar campos obrigatórios de subdivisões se existirem
    for mandatory_field in mandatory_subdivision_fields:
        if mandatory_field in all_columns:
            selected_columns.append(mandatory_field)
    
    # Depois, adicionar outros campos que correspondem aos padrões
    for i, col_lower in enumerate(columns_lower):
        original_col = all_columns[i]
        
        # Pular se já foi adicionado como campo obrigatório
        if original_col in selected_columns:
            continue
            
        # Verificar se a coluna corresponde a algum padrão essencial
        for pattern in essential_patterns:
            if re.search(pattern, col_lower):
                selected_columns.append(original_col)
                break
    
    # Se não encontrou colunas essenciais, pegar as primeiras 20
    if len(selected_columns) < 5:
        selected_columns = list(all_columns[:20])
    
    # Aumentar limite para incluir campos de subdivisões (de 30 para 50)
    return selected_columns[:50]

def clean_and_standardize_data(df):
    """
    Limpa e padroniza os dados do DataFrame de forma otimizada
    """
    if df is None or df.empty:
        return None
    
    print(f"Limpando dados ({len(df)} linhas)...")
    
    # Remover linhas completamente vazias
    df = df.dropna(how='all')
    
    # Padronizar nomes das colunas
    new_columns = []
    for col in df.columns:
        # Converter para string, minúsculas, remover espaços e caracteres especiais
        clean_col = str(col).strip().lower()
        clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', clean_col)
        clean_col = re.sub(r'_+', '_', clean_col)  # Remover underscores múltiplos
        clean_col = clean_col.strip('_')  # Remover underscores no início/fim
        new_columns.append(clean_col)
    
    df.columns = new_columns
    
    print(f"Dados limpos: {len(df)} linhas, {len(df.columns)} colunas")
    return df

def create_optimized_table(conn):
    """
    Cria uma tabela otimizada para os dados globais
    """
    cursor = conn.cursor()
    
    # Verificar se a tabela já existe
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='damodaran_global';")
    if cursor.fetchone():
        print("Tabela damodaran_global já existe. Será recriada.")
        cursor.execute("DROP TABLE damodaran_global;")
    
    # Criar tabela com estrutura mais simples e otimizada
    # Incluindo campos específicos para subdivisões geográficas e setoriais
    cursor.execute('''
    CREATE TABLE damodaran_global (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year INTEGER NOT NULL,
        company_name TEXT,
        ticker TEXT,
        exchange TEXT,
        industry TEXT,
        country TEXT,
        
        -- Campos de subdivisões geográficas
        broad_group TEXT,
        erp_for_country REAL,
        
        -- Campos de subdivisões setoriais  
        industry_group TEXT,
        primary_sector TEXT,
        sub_group TEXT,
        sic_code TEXT,
        bottom_up_beta_sector REAL,
        
        -- Métricas financeiras básicas
        market_cap REAL,
        enterprise_value REAL,
        revenue REAL,
        net_income REAL,
        ebitda REAL,
        pe_ratio REAL,
        beta REAL,
        debt_equity REAL,
        roe REAL,
        roa REAL,
        dividend_yield REAL,
        revenue_growth REAL,
        operating_margin REAL,
        
        -- Dados brutos completos em JSON
        raw_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Criar índices para melhor performance
    cursor.execute("CREATE INDEX idx_year ON damodaran_global(year);")
    cursor.execute("CREATE INDEX idx_company ON damodaran_global(company_name);")
    cursor.execute("CREATE INDEX idx_country ON damodaran_global(country);")
    
    conn.commit()
    print("Tabela damodaran_global criada com índices otimizados.")

def map_columns_optimized(df):
    """
    Mapeia colunas de forma otimizada, incluindo campos de subdivisões
    """
    # Mapeamento simplificado para colunas mais comuns
    column_mapping = {
        # Identificação
        'company_name': ['Company Name', 'company_name', 'name', 'company'],
        'ticker': ['Exchange:Ticker', 'ticker', 'symbol'],
        'exchange': ['exchange'],
        'industry': ['industry', 'sector'],
        'country': ['Country', 'country'],
        
        # Subdivisões geográficas
        'broad_group': ['Broad Group', 'broad_group'],
        'erp_for_country': ['ERP for Country', 'erp_for_country'],
        
        # Subdivisões setoriais
        'industry_group': ['Industry Group', 'industry_group'],
        'primary_sector': ['Primary Sector', 'primary_sector'],
        'sub_group': ['Sub Group', 'sub_group'],
        'sic_code': ['SIC Code', 'sic_code'],
        'bottom_up_beta_sector': ['Bottom up Beta for sector', 'bottom_up_beta_sector'],
        
        # Métricas financeiras
        'market_cap': ['Market Cap (in US $)', 'market_cap', 'mkt_cap', 'market_capitalization'],
        'enterprise_value': ['Enterprise Value (in US $)', 'enterprise_value', 'ev'],
        'revenue': ['Revenues', 'revenue', 'sales', 'total_revenue'],
        'net_income': ['Net Income', 'net_income', 'net_profit'],
        'ebitda': ['EBITDA', 'ebitda'],
        'pe_ratio': ['pe_ratio', 'pe', 'trailing_pe'],
        'beta': ['beta'],
        'debt_equity': ['debt_equity', 'de_ratio'],
        'roe': ['roe', 'return_on_equity'],
        'roa': ['roa', 'return_on_assets'],
        'dividend_yield': ['dividend_yield', 'div_yield'],
        'revenue_growth': ['revenue_growth', 'sales_growth'],
        'operating_margin': ['Operating Margin', 'operating_margin', 'op_margin']
    }
    
    mapped_data = {'year': df['year'] if 'year' in df.columns else None}
    
    # Mapear colunas disponíveis
    for target_col, possible_names in column_mapping.items():
        found = False
        for possible_name in possible_names:
            # Buscar por nome exato ou similar
            for df_col in df.columns:
                if possible_name in str(df_col).lower():
                    mapped_data[target_col] = df[df_col]
                    found = True
                    break
            if found:
                break
    
    # Criar DataFrame mapeado
    result_df = pd.DataFrame(mapped_data)
    
    # Adicionar dados brutos como JSON (apenas uma amostra para economizar espaço)
    if len(df) > 0:
        sample_data = df.head(1).to_dict('records')[0] if len(df) > 0 else {}
        import json
        result_df['raw_data'] = json.dumps(sample_data)
    
    return result_df

def insert_data_in_batches(conn, df, year):
    """
    Insere dados em lotes para melhor performance
    """
    if df is None or df.empty:
        print(f"Nenhum dado para inserir do ano {year}")
        return 0
    
    cursor = conn.cursor()
    
    # Obter colunas da tabela
    cursor.execute("PRAGMA table_info(damodaran_global);")
    table_columns = [col[1] for col in cursor.fetchall() if col[1] not in ['id', 'created_at']]
    
    inserted_count = 0
    total_rows = len(df)
    
    print(f"Inserindo {total_rows} registros em lotes de {BATCH_SIZE}...")
    
    # Processar em lotes
    for i in range(0, total_rows, BATCH_SIZE):
        batch_df = df.iloc[i:i+BATCH_SIZE]
        
        try:
            # Preparar dados do lote
            batch_data = []
            
            for _, row in batch_df.iterrows():
                row_data = []
                for col in table_columns:
                    if col in batch_df.columns:
                        value = row[col]
                        # Converter valores não numéricos para None se necessário
                        if pd.isna(value):
                            row_data.append(None)
                        else:
                            row_data.append(value)
                    else:
                        row_data.append(None)
                batch_data.append(row_data)
            
            # Inserir lote
            placeholders = ', '.join(['?' for _ in table_columns])
            query = f"INSERT INTO damodaran_global ({', '.join(table_columns)}) VALUES ({placeholders})"
            
            cursor.executemany(query, batch_data)
            conn.commit()
            
            inserted_count += len(batch_data)
            progress = (i + BATCH_SIZE) / total_rows * 100
            print(f"\rProgresso inserção: {progress:.1f}% ({inserted_count}/{total_rows})", end='', flush=True)
            
        except Exception as e:
            print(f"\nErro ao inserir lote {i//BATCH_SIZE + 1}: {e}")
            continue
    
    print(f"\nInseridos {inserted_count} registros do ano {year}")
    return inserted_count

def select_years_to_process(available_years):
    """
    Permite ao usuário selecionar quais anos processar
    """
    if not available_years:
        print("Nenhum ano disponível encontrado.")
        return []
    
    print(f"\nAnos disponíveis: {available_years}")
    print("Opções:")
    print("1. Processar todos os anos")
    print("2. Selecionar anos específicos")
    print("3. Processar apenas o ano mais recente")
    
    choice = input("Escolha uma opção (1-3): ").strip()
    
    if choice == '1':
        return available_years
    elif choice == '2':
        selected_years = []
        years_input = input(f"Digite os anos separados por vírgula (ex: {available_years[0]},{available_years[-1]}): ")
        try:
            for year_str in years_input.split(','):
                year = int(year_str.strip())
                if year in available_years:
                    selected_years.append(year)
                else:
                    print(f"Ano {year} não está disponível.")
            return sorted(selected_years)
        except ValueError:
            print("Formato inválido. Processando apenas o ano mais recente.")
            return [available_years[-1]]
    elif choice == '3':
        return [available_years[-1]]
    else:
        print("Opção inválida. Processando apenas o ano mais recente.")
        return [available_years[-1]]

def main():
    """
    Função principal otimizada
    """
    print("=== Extrator Otimizado de Dados Globais do Damodaran ===")
    print(f"Banco de dados: {db_name}")
    print(f"Configurações: Chunk size = {CHUNK_SIZE}, Batch size = {BATCH_SIZE}")
    
    # Buscar anos disponíveis
    available_years = get_available_years()
    
    if not available_years:
        print("Nenhum arquivo encontrado. Tentando anos padrão...")
        available_years = [2020, 2021, 2022, 2023, 2024, 2025]
    
    # Selecionar anos para processar
    years_to_process = select_years_to_process(available_years)
    
    if not years_to_process:
        print("Nenhum ano selecionado. Encerrando.")
        return
    
    print(f"\nProcessando anos: {years_to_process}")
    
    # Conectar ao banco de dados
    conn = sqlite3.connect(db_name)
    
    # Otimizações do SQLite
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA cache_size = 10000;")
    cursor.execute("PRAGMA temp_store = MEMORY;")
    
    try:
        # Criar tabela otimizada
        create_optimized_table(conn)
        
        total_inserted = 0
        start_time = time.time()
        
        # Processar cada ano
        for year in years_to_process:
            print(f"\n--- Processando ano {year} ---")
            year_start_time = time.time()
            
            # Baixar arquivo
            excel_content = download_file_with_progress(year)
            
            if excel_content is not None:
                # Processar em chunks
                processed_data = process_excel_in_chunks(excel_content, year)
                
                if processed_data is not None:
                    # Limpar dados
                    cleaned_data = clean_and_standardize_data(processed_data)
                    
                    if cleaned_data is not None:
                        # Mapear colunas
                        mapped_data = map_columns_optimized(cleaned_data)
                        
                        # Inserir no banco em lotes
                        inserted = insert_data_in_batches(conn, mapped_data, year)
                        total_inserted += inserted
                        
                        year_time = time.time() - year_start_time
                        print(f"Ano {year} processado em {year_time:.1f}s")
                    else:
                        print(f"Erro ao limpar dados do ano {year}")
                else:
                    print(f"Erro ao processar dados do ano {year}")
            else:
                print(f"Erro ao baixar dados do ano {year}")
        
        total_time = time.time() - start_time
        
        print(f"\n=== Processo Concluído em {total_time:.1f}s ===")
        print(f"Total de registros inseridos: {total_inserted}")
        
        # Mostrar estatísticas finais
        cursor.execute("SELECT year, COUNT(*) FROM damodaran_global GROUP BY year ORDER BY year;")
        stats = cursor.fetchall()
        
        print("\nEstatísticas por ano:")
        for year, count in stats:
            print(f"  {year}: {count} empresas")
        
        # Otimizar banco após inserção
        print("\nOtimizando banco de dados...")
        cursor.execute("ANALYZE;")
        cursor.execute("VACUUM;")
        
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
        print("\nConexão com banco de dados fechada.")

if __name__ == "__main__":
    main()