import sqlite3
import requests
import pandas as pd
import os
import io
import re

# URL da página HTML com os dados de prêmio de risco país
url_html = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html"

# Nome do novo banco de dados
db_name = "data/damodaran_data_new.db"

# Função para extrair os dados da página HTML
def extract_country_risk_data():
    print("Baixando dados de prêmio de risco país da página do Damodaran...")
    
    try:
        # Fazer o download da página HTML
        response = requests.get(url_html)
        
        if response.status_code != 200:
            print(f"Erro ao acessar a URL: {response.status_code}")
            return None
        
        # Extrair o conteúdo HTML
        html_content = response.text
        
        # Usar StringIO para evitar o aviso de depreciação
        from io import StringIO
        
        # Extrair todas as tabelas da página HTML
        tables = pd.read_html(StringIO(html_content))
        print(f"Encontradas {len(tables)} tabelas na página HTML")
        
        # A tabela que queremos é geralmente a maior
        country_risk_table = None
        max_rows = 0
        
        for i, table in enumerate(tables):
            if len(table) > max_rows:
                max_rows = len(table)
                country_risk_table = table
                print(f"Selecionando tabela {i+1} com {max_rows} linhas")
        
        if country_risk_table is None or len(country_risk_table) < 10:  # Deve ter pelo menos alguns países
            print("Não foi possível encontrar uma tabela adequada com os dados de prêmio de risco país")
            return None
        
        # Mostrar as primeiras linhas da tabela encontrada
        print("Primeiras linhas da tabela encontrada:")
        print(country_risk_table.head())
        
        # Verificar as colunas disponíveis
        print("Colunas disponíveis:")
        print(country_risk_table.columns.tolist())
        
        # Limpar os nomes das colunas
        # Primeiro, verificar se temos uma estrutura de colunas multi-nível
        if isinstance(country_risk_table.columns, pd.MultiIndex):
            # Pegar o último nível para cada coluna
            new_cols = [col[-1] if isinstance(col, tuple) else col for col in country_risk_table.columns]
            country_risk_table.columns = new_cols
        
        # Renomear as colunas para garantir que temos 'Country' e 'Country Risk Premium'
        # Assumindo que a primeira coluna é o país e a coluna com 'Premium' é o prêmio de risco
        new_columns = {}
        country_col = None
        premium_col = None
        
        for col in country_risk_table.columns:
            col_str = str(col).strip()
            if col_str.lower() == 'country' or country_col is None and col_str == country_risk_table.columns[0]:
                country_col = col
                new_columns[col] = 'Country'
            elif 'premium' in col_str.lower() or 'risk' in col_str.lower() and 'premium' in col_str.lower():
                premium_col = col
                new_columns[col] = 'Country Risk Premium'
        
        # Se não encontrou a coluna de prêmio, usar a terceira coluna (comum em tabelas do Damodaran)
        if premium_col is None and len(country_risk_table.columns) >= 3:
            premium_col = country_risk_table.columns[2]
            new_columns[premium_col] = 'Country Risk Premium'
        
        # Renomear as colunas identificadas
        if new_columns:
            country_risk_table = country_risk_table.rename(columns=new_columns)
        
        # Se ainda não temos as colunas necessárias, tentar uma abordagem mais direta
        if 'Country' not in country_risk_table.columns or 'Country Risk Premium' not in country_risk_table.columns:
            print("Renomeando colunas para formato padrão...")
            # Assumir que a primeira coluna é o país e a terceira é o prêmio de risco
            if len(country_risk_table.columns) >= 3:
                country_risk_table = country_risk_table.iloc[:, [0, 2]]
                country_risk_table.columns = ['Country', 'Country Risk Premium']
            # Se tiver apenas duas colunas, usar a primeira e a segunda
            elif len(country_risk_table.columns) == 2:
                country_risk_table.columns = ['Country', 'Country Risk Premium']
        
        # Verificar se temos as colunas necessárias
        if 'Country' not in country_risk_table.columns or 'Country Risk Premium' not in country_risk_table.columns:
            print("Não foi possível identificar as colunas necessárias na tabela")
            return None
        
        # Filtrar apenas as colunas que nos interessam
        filtered_data = country_risk_table[['Country', 'Country Risk Premium']]
        
        # Limpar os dados
        # 1. Remover linhas com valores NaN
        filtered_data = filtered_data.dropna()
        
        # 2. Remover linhas que não são países (como cabeçalhos, totais, etc)
        filtered_data = filtered_data[~filtered_data['Country'].astype(str).str.contains('Country|Total|Average|Median|Region|Unnamed|^$', case=False)]
        
        # 3. Limpar a coluna de país (remover espaços extras, etc)
        filtered_data['Country'] = filtered_data['Country'].astype(str).str.strip()
        
        # 4. Converter o prêmio de risco para formato numérico
        try:
            # Primeiro, limpar a coluna de prêmio de risco
            filtered_data['Country Risk Premium'] = filtered_data['Country Risk Premium'].astype(str).str.strip()
            
            # Remover caracteres não numéricos, exceto ponto e vírgula
            filtered_data['Country Risk Premium'] = filtered_data['Country Risk Premium'].apply(
                lambda x: re.sub(r'[^0-9.,]', '', str(x))
            )
            
            # Substituir vírgula por ponto (padrão internacional)
            filtered_data['Country Risk Premium'] = filtered_data['Country Risk Premium'].str.replace(',', '.')
            
            # Converter para numérico
            filtered_data['Country Risk Premium'] = pd.to_numeric(filtered_data['Country Risk Premium'], errors='coerce')
            
            # Se os valores parecem ser percentuais (maiores que 1), dividir por 100
            if filtered_data['Country Risk Premium'].median() > 1:
                filtered_data['Country Risk Premium'] = filtered_data['Country Risk Premium'] / 100
        except Exception as e:
            print(f"Erro ao converter prêmio de risco para número: {e}")
            # Mostrar alguns exemplos de valores para diagnóstico
            print("Exemplos de valores na coluna de prêmio de risco:")
            print(filtered_data['Country Risk Premium'].head())
        
        # Remover linhas com valores NaN após conversão
        filtered_data = filtered_data.dropna()
        
        # Remover duplicatas
        filtered_data = filtered_data.drop_duplicates(subset=['Country'])
        
        # Mostrar amostra final dos dados
        print("Amostra final dos dados:")
        print(filtered_data.head())
        
        print(f"Dados extraídos com sucesso: {len(filtered_data)} países")
        return filtered_data
    
    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        import traceback
        traceback.print_exc()
        return None

# Função para criar o banco de dados e salvar os dados
def create_database_and_save_data(data):
    # Verificar se o banco de dados já existe
    if os.path.exists(db_name):
        print(f"O banco de dados {db_name} já existe. Será substituído.")
        os.remove(db_name)
    
    # Criar conexão com o banco de dados
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Criar tabela country_risk
    cursor.execute('''
    CREATE TABLE country_risk (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country TEXT NOT NULL,
        risk_premium REAL NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Inserir dados na tabela
    for _, row in data.iterrows():
        cursor.execute(
            "INSERT INTO country_risk (country, risk_premium) VALUES (?, ?)",
            (row['Country'], row['Country Risk Premium'])
        )
    
    # Commit e fechar conexão
    conn.commit()
    conn.close()
    
    print(f"Banco de dados {db_name} criado com sucesso!")
    print(f"Tabela country_risk criada com {len(data)} registros.")

# Função principal
def main():
    print("Iniciando extração de dados e criação do banco de dados...")
    
    # Extrair dados
    data = extract_country_risk_data()
    
    if data is not None:
        # Criar banco de dados e salvar dados
        create_database_and_save_data(data)
        print("Processo concluído com sucesso!")
    else:
        print("Não foi possível extrair os dados. Processo abortado.")

# Executar o script
if __name__ == "__main__":
    main()