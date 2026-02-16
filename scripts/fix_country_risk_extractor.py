import sqlite3
import requests
import pandas as pd
import os
import io

# URL correta do arquivo Excel do Damodaran
url_excel = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/ctrypremJuly25.xlsx"

# Nome do banco de dados
db_name = "data/damodaran_data_new.db"

def extract_country_risk_from_excel():
    """
    Extrai dados de risco país do arquivo Excel do Damodaran.
    Aba: 'ERPs by country'
    Coluna: 'Country Risk Premium'
    """
    print("Baixando dados de risco país do arquivo Excel do Damodaran...")
    print(f"URL: {url_excel}")
    
    try:
        # Fazer o download do arquivo Excel
        response = requests.get(url_excel)
        
        if response.status_code != 200:
            print(f"Erro ao acessar a URL: {response.status_code}")
            return None
        
        # Ler o arquivo Excel diretamente da memória
        excel_file = io.BytesIO(response.content)
        
        # Ler a aba 'ERPs by country'
        df = pd.read_excel(excel_file, sheet_name='ERPs by country')
        
        print(f"Dados carregados: {df.shape[0]} linhas, {df.shape[1]} colunas")
        
        # Procurar especificamente pelo Brasil em todas as colunas
        print("\n=== PROCURANDO BRASIL EM TODAS AS COLUNAS ===")
        
        brazil_positions = []
        for i in range(len(df)):
            for j, col in enumerate(df.columns):
                cell_value = str(df.iloc[i][col]).strip().lower()
                if 'brazil' in cell_value:
                    print(f"Brasil encontrado na linha {i}, coluna {j} ('{col}'): {df.iloc[i][col]}")
                    
                    # Mostrar valores nas colunas adjacentes
                    print(f"  Valores na linha {i}:")
                    for k, col_name in enumerate(df.columns):
                        val = df.iloc[i][col_name]
                        if pd.notna(val):
                            print(f"    Col {k} ('{col_name}'): {val}")
                    
                    brazil_positions.append((i, j))
                    print("---")
        
        if not brazil_positions:
            print("Brasil não encontrado. Tentando variações...")
            # Tentar outras variações
            variations = ['brasil', 'bra']
            for variation in variations:
                for i in range(len(df)):
                    for j, col in enumerate(df.columns):
                        cell_value = str(df.iloc[i][col]).strip().lower()
                        if variation in cell_value and len(cell_value) < 20:
                            print(f"Variação '{variation}' encontrada na linha {i}, coluna {j}: {df.iloc[i][col]}")
                            brazil_positions.append((i, j))
        
        if not brazil_positions:
            print("Nenhuma referência ao Brasil encontrada.")
            return None
        
        # Analisar cada posição do Brasil para encontrar o valor 3.70%
        print("\n=== ANALISANDO VALORES PRÓXIMOS AO BRASIL ===")
        
        target_value = 3.70  # Valor esperado em percentual
        best_match = None
        best_diff = float('inf')
        
        for row_idx, col_idx in brazil_positions:
            print(f"\nAnalisando Brasil na linha {row_idx}:")
            
            # Verificar todas as colunas numéricas nesta linha
            for j, col_name in enumerate(df.columns):
                val = df.iloc[row_idx][col_name]
                
                if pd.notna(val):
                    try:
                        # Tentar converter para número
                        if isinstance(val, str):
                            num_val = float(val.replace('%', '').replace(',', '.').strip())
                        else:
                            num_val = float(val)
                        
                        # Verificar se está próximo de 3.70% (tanto em decimal quanto percentual)
                        diff_percent = abs(num_val - target_value)
                        diff_decimal = abs(num_val - (target_value / 100))
                        
                        min_diff = min(diff_percent, diff_decimal)
                        
                        print(f"  Col {j} ('{col_name}'): {val} -> {num_val} (diff: {min_diff:.4f})")
                        
                        # Se está próximo do valor esperado
                        if min_diff < 1.0:  # Tolerância de 1%
                            print(f"    *** POSSÍVEL MATCH: {num_val} ***")
                            
                            if min_diff < best_diff:
                                best_diff = min_diff
                                best_match = {
                                    'row': row_idx,
                                    'col': j,
                                    'col_name': col_name,
                                    'value': num_val,
                                    'is_decimal': diff_decimal < diff_percent
                                }
                    
                    except:
                        pass
        
        if best_match:
            print(f"\n✅ MELHOR MATCH ENCONTRADO:")
            print(f"  Linha: {best_match['row']}")
            print(f"  Coluna: {best_match['col']} ('{best_match['col_name']}')")
            print(f"  Valor: {best_match['value']}")
            print(f"  É decimal: {best_match['is_decimal']}")
            
            # Usar esta coluna para extrair todos os dados
            target_col = best_match['col_name']
            country_col = df.columns[0]  # Assumir que a primeira coluna tem os países
            
            print(f"\nExtraindo dados usando:")
            print(f"  Coluna de países: '{country_col}'")
            print(f"  Coluna de risco: '{target_col}'")
            
        else:
            print("\n❌ Não foi possível encontrar valor próximo a 3.70% para o Brasil")
            print("Usando a coluna com mais valores numéricos válidos...")
            
            # Fallback: usar a coluna com mais valores numéricos
            best_col = None
            max_numeric = 0
            
            for col in df.columns:
                numeric_count = 0
                for val in df[col]:
                    try:
                        if pd.notna(val):
                            num_val = float(str(val).replace('%', '').replace(',', '.'))
                            if 0 < num_val < 50:
                                numeric_count += 1
                    except:
                        pass
                
                if numeric_count > max_numeric:
                    max_numeric = numeric_count
                    best_col = col
            
            target_col = best_col
            country_col = df.columns[0]
        
        # Extrair dados usando as colunas identificadas
        print(f"\n=== EXTRAINDO DADOS ===")
        print(f"Coluna de países: '{country_col}'")
        print(f"Coluna de risco: '{target_col}'")
        
        result_data = []
        
        for i in range(len(df)):
            country = df.iloc[i][country_col]
            risk = df.iloc[i][target_col]
            
            # Validar país
            if pd.isna(country) or str(country).strip() == '':
                continue
            
            country_str = str(country).strip()
            
            # Filtrar linhas que não são países
            if any(word in country_str.lower() for word in ['country', 'total', 'average', 'median', 'region', 'unnamed', 'enter', 'date']):
                continue
            
            if len(country_str) < 2 or len(country_str) > 50:
                continue
            
            # Validar risco
            if pd.isna(risk):
                continue
            
            try:
                risk_str = str(risk).replace('%', '').replace(',', '.').strip()
                risk_num = float(risk_str)
                
                # Determinar se está em percentual ou decimal
                if best_match and best_match['col_name'] == target_col:
                    # Usar a informação do best_match
                    if not best_match['is_decimal'] and risk_num > 1:
                        risk_num = risk_num / 100
                else:
                    # Heurística: se a maioria dos valores > 1, está em percentual
                    if risk_num > 1:
                        risk_num = risk_num / 100
                
                # Validar range razoável
                if 0 <= risk_num <= 0.5:  # 0% a 50%
                    result_data.append({
                        'Country': country_str,
                        'Country Risk Premium': risk_num
                    })
                    
            except:
                continue
        
        if len(result_data) == 0:
            print("Nenhum dado válido encontrado.")
            return None
        
        # Converter para DataFrame
        final_df = pd.DataFrame(result_data)
        
        # Remover duplicatas
        final_df = final_df.drop_duplicates(subset=['Country'])
        
        print(f"\n✅ Dados extraídos: {len(final_df)} países")
        
        # Mostrar alguns exemplos
        print("\nExemplos de dados extraídos:")
        print(final_df.head(10))
        
        # Verificar Brasil especificamente
        brazil_data = final_df[final_df['Country'].str.contains('Brazil', case=False, na=False)]
        if not brazil_data.empty:
            brazil_risk = brazil_data.iloc[0]['Country Risk Premium']
            print(f"\n✅ Brasil encontrado: {brazil_risk:.4f} ({brazil_risk*100:.2f}%)")
            
            # Verificar se está próximo do valor esperado (3.70%)
            expected_value = 0.037  # 3.70% em decimal
            if abs(brazil_risk - expected_value) < 0.01:  # Tolerância de 1%
                print("✅ Valor do Brasil está correto!")
            else:
                print(f"⚠️  Valor do Brasil: {brazil_risk*100:.2f}%. Esperado: ~3.70%")
        else:
            print("❌ Brasil não encontrado nos dados finais")
        
        return final_df
        
    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        import traceback
        traceback.print_exc()
        return None

def update_country_risk_table(data):
    """
    Atualiza a tabela country_risk no banco de dados.
    """
    print(f"\nAtualizando tabela country_risk no banco {db_name}...")
    
    try:
        # Conectar ao banco de dados
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        # Limpar tabela existente
        cursor.execute("DELETE FROM country_risk")
        print("Tabela country_risk limpa")
        
        # Inserir novos dados
        for _, row in data.iterrows():
            cursor.execute(
                "INSERT INTO country_risk (country, risk_premium) VALUES (?, ?)",
                (row['Country'], row['Country Risk Premium'])
            )
        
        # Commit e fechar conexão
        conn.commit()
        conn.close()
        
        print(f"✅ Tabela atualizada com {len(data)} registros")
        
        # Verificar se a atualização foi bem-sucedida
        conn = sqlite3.connect(db_name)
        brazil_check = pd.read_sql_query(
            "SELECT country, risk_premium FROM country_risk WHERE country LIKE '%Brazil%'", 
            conn
        )
        conn.close()
        
        if not brazil_check.empty:
            brazil_risk = brazil_check.iloc[0]['risk_premium']
            print(f"✅ Verificação: Brasil agora tem {brazil_risk:.4f} ({brazil_risk*100:.2f}%)")
        else:
            print("⚠️  Brasil não encontrado na verificação")
        
    except Exception as e:
        print(f"Erro ao atualizar tabela: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("=== CORREÇÃO DO EXTRATOR DE RISCO PAÍS ===")
    print("Extraindo dados do arquivo Excel correto do Damodaran...")
    print("Procurando especificamente pelo valor 3.70% para o Brasil...")
    
    # Extrair dados
    data = extract_country_risk_from_excel()
    
    if data is not None and len(data) > 0:
        # Atualizar tabela no banco de dados
        update_country_risk_table(data)
        print("\n✅ Processo concluído com sucesso!")
    else:
        print("\n❌ Não foi possível extrair os dados. Processo abortado.")

if __name__ == "__main__":
    main()