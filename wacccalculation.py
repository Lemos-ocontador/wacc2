import pandas as pd
import json
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

def get_activity_values(json_file_name, atividade):
    try:
        dfatividade = pd.read_json(json_file_name, orient='records', encoding='UTF-8')
        filtered_dfatividade = dfatividade[dfatividade['atividade'] == atividade]
        if not filtered_dfatividade.empty:
            beta_value = filtered_dfatividade['Beta'].values[0]
            de_value = filtered_dfatividade['DE'].values[0]
            ev_sales_value = filtered_dfatividade['EV/Sales'].values[0]
            return beta_value, de_value, ev_sales_value
        else:
            print(f"Atividade {atividade} não encontrada no JSON.")
            return None, None, None
    except ValueError as e:
        print("Erro ao carregar o arquivo JSON:", e)
        return None, None, None

def get_values_from_json(json_file_name, campos):
    df = pd.read_json(json_file_name, orient='records', encoding='UTF-8')
    valores = {}
    for campo in campos:
        valor = df[df['Campo'] == campo]['Valor'].values
        if valor.size > 0:
            valores[campo] = valor[0]
        else:
            valores[campo] = None
    return valores

def get_ipca_values(json_file_name, indices):
    df = pd.read_json(json_file_name, orient='records', encoding='UTF-8')
    valores = {}
    for indice in indices:
        valor = df.loc[df['indice'] == indice, 'Valor'].values[0]
        valores[indice] = valor
    return valores

def get_premio(dfsize, ValordaEmpresa):
    dfsize[' De '] = dfsize[' De '].astype(str).str.strip().str.replace('.', '').str.replace(',', '.').astype(float).astype(int)
    dfsize[' até '] = dfsize[' até '].astype(str).str.strip().str.replace('.', '').str.replace(',', '.').astype(float).astype(int)
    linha = dfsize[(dfsize[' De '] <= ValordaEmpresa) & (dfsize[' até '] >= ValordaEmpresa)]
    if not linha.empty:
        return linha['Premio'].iloc[0]
    else:
        return "Valor fora do range"

def porcentagem_para_decimal(valor_porcentagem):
    if isinstance(valor_porcentagem, str):
        valor_decimal = valor_porcentagem.strip('%').replace(',', '.')
        return float(valor_decimal) / 100
    elif isinstance(valor_porcentagem, (int, float)):
        return valor_porcentagem / 100
    else:
        return None

def string_para_float(valor):
    if isinstance(valor, str):
        return float(valor.strip('%').strip().replace(',', '.'))
    elif isinstance(valor, (int, float)):
        return float(valor)
    else:
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/calcular-wacc', methods=['POST'])
def calcular_wacc():
    data = request.json
    atividade = data.get('atividade')
    lucroReal = data.get('lucroReal')
    Vendasano5 = data.get('vendasAno5')
    
    if lucroReal == 'Nao':
        IR = 0
    else:
        IR = 0.25  # Exemplo de valor de IR para "Sim". Você pode ajustar conforme necessário.

    json_file_name = 'Bd_Atividade.json'
    Beta, DE, EV_Sales = get_activity_values(json_file_name, atividade)

    json_file_wacc = 'BDWacc.json'
    campos = ['RF', 'RM', 'CR', 'IA', 'IB', 'CT', 'IR']  # Campos necessários
    valores = get_values_from_json(json_file_wacc, campos)

    RF = valores['RF']
    RM = valores['RM']
    CR = valores['CR']
    IA = valores['IA']
    IB = valores['IB']
    CT = valores['CT']
    IR = valores['IR'] if lucroReal == 'Sim' else 0

    json_file_macro = 'BDMacro2.json'
    indices_ipca = ['IPCAano1', 'IPCAano2', 'IPCAano3', 'IPCAano4', 'IPCAano5']
    valores_ipca = get_ipca_values(json_file_macro, indices_ipca)

    valores_numericos = [float(valor.strip().replace(',', '.')) for valor in valores_ipca.values()]
    IPCAlongo = sum(valores_numericos) / len(valores_numericos)

    json_file_size = 'BDSize.json'
    dfsize = pd.read_json(json_file_size, orient='records', encoding='UTF-8')

    EV_Sales_float = float(EV_Sales.strip().replace('.', '').replace(',', '.'))
    ValordaEmpresa = Vendasano5 * EV_Sales_float
    premio = get_premio(dfsize, ValordaEmpresa)

    RF_decimal = porcentagem_para_decimal(RF)
    RM_decimal = porcentagem_para_decimal(RM)
    CR_decimal = porcentagem_para_decimal(CR)
    IA_decimal = porcentagem_para_decimal(IA)
    IB_decimal = porcentagem_para_decimal(IB)
    CT_decimal = porcentagem_para_decimal(CT)
    IR_decimal = porcentagem_para_decimal(IR)
    premio_decimal = porcentagem_para_decimal(premio)

    RF = string_para_float(RF)
    RM = string_para_float(RM)
    CR = string_para_float(CR)
    IA = string_para_float(IA)
    IB = string_para_float(IB)
    CT = string_para_float(CT)
    IR = string_para_float(IR)
    IPCAlongo = string_para_float(IPCAlongo)
    premio = string_para_float(premio)
    Beta = string_para_float(Beta)
    DE = string_para_float(DE)

    Betarealav = Beta * (1+(DE*(1-(IR/100))))
    KEnomUS = (RF + (Betarealav * RM) + CR + premio) / 100
    KEreal = (1 + KEnomUS) / (1 + (IA / 100)) - 1
    KenomBR = (1 + KEreal) * (1 + (IB / 100)) - 1

    KDnom = ((CT / 100) * (1 - (IR / 100)))
    KDreal = (1 + KDnom) / (1 + IPCAlongo) - 1

    partkd = DE / (1 + DE)
    partke = 1 - partkd

    WaccReal = ((KDreal * partkd) + (KEreal * partke))
    WaccNominal = (KDnom * partkd) + (KenomBR * partke)

    return jsonify(wacc=WaccNominal)

if __name__ == '__main__':
    app.run(debug=True)

