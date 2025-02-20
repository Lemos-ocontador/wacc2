from flask import Flask, request, render_template, jsonify, send_from_directory
import pandas as pd
import logging
import os
from flask_cors import CORS  # Para permitir requisições cross-origin se necessário

app = Flask(__name__)
CORS(app)  # Habilita CORS para todas as rotas

# Configuração do logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create handlers
file_handler = logging.FileHandler('app.log')
stream_handler = logging.StreamHandler()

# Create formatters
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# --------------------------------------------
# Funções auxiliares
# --------------------------------------------
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
            logger.error(f"Atividade não encontrada: {atividade}")
            return None, None, None
    except Exception as e:
        logger.error(f"Erro ao ler arquivo de atividades: {str(e)}")
        return None, None, None

def get_values_from_json(json_file_name, campos):
    try:
        df = pd.read_json(json_file_name, orient='records', encoding='UTF-8')
        valores = {}
        for campo in campos:
            valor = df[df['Campo'] == campo]['Valor'].values
            if valor.size > 0:
                valores[campo] = valor[0]
            else:
                valores[campo] = None
        return valores
    except Exception as e:
        logger.error(f"Erro ao ler valores do arquivo {json_file_name}: {str(e)}")
        return {}

def get_ipca_values(json_file_name, indices):
    try:
        df = pd.read_json(json_file_name, orient='records', encoding='UTF-8')
        valores = {}
        for indice in indices:
            valor = df.loc[df['indice'] == indice, 'Valor'].values[0]
            valores[indice] = valor
        return valores
    except Exception as e:
        logger.error(f"Erro ao ler valores IPCA: {str(e)}")
        return {}

def get_premio(dfsize, ValordaEmpresa):
    try:
        dfsize[' De '] = dfsize[' De '].astype(str).str.strip().str.replace('.', '').str.replace(',', '.').astype(float).astype(int)
        dfsize[' até '] = dfsize[' até '].astype(str).str.strip().str.replace('.', '').str.replace(',', '.').astype(float).astype(int)
        linha = dfsize[(dfsize[' De '] <= ValordaEmpresa) & (dfsize[' até '] >= ValordaEmpresa)]
        if not linha.empty:
            return linha['Premio'].iloc[0]
        else:
            logger.warning(f"Valor fora do range: {ValordaEmpresa}")
            return "0"  # Retornar zero como fallback
    except Exception as e:
        logger.error(f"Erro ao calcular prêmio: {str(e)}")
        return "0"

def porcentagem_para_decimal(valor_porcentagem):
    if isinstance(valor_porcentagem, str):
        try:
            valor_decimal = valor_porcentagem.strip('%').replace(',', '.')
            return float(valor_decimal) / 100
        except:
            return 0
    elif isinstance(valor_porcentagem, (int, float)):
        return valor_porcentagem / 100
    else:
        return 0

def string_para_float(valor):
    if isinstance(valor, str):
        try:
            # Handle percentage sign and format "5.000.000,00"
            valor = valor.strip().replace('%', '').replace('.', '').replace(',', '.')
            return float(valor)
        except:
            return 0
    elif isinstance(valor, (int, float)):
        return float(valor)
    else:
        return 0

# --------------------------------------------
# Rotas do Flask - Simplificado
# --------------------------------------------

# Rota específica para a página da calculadora WACC
@app.route('/wacc-calculator')
def wacc_calculator():
    """Carrega a página da calculadora WACC"""
    return render_template('wacc-calculator.html')

# API endpoint para o cálculo WACC
@app.route('/api/calcular-wacc', methods=['POST'])
def calcular_wacc():
    try:
        # Receber dados do formulário
        atividade = request.form.get('atividade')
        faturamento_str = request.form.get('faturamento', '0')
        lucroReal = request.form.get('lucroReal', 'Nao')

        # Validar dados
        if not atividade:
            logger.error("Atividade inválida")
            return jsonify({"error": "Selecione uma atividade válida"}), 400

        # Converter valores
        try:
            faturamento = string_para_float(faturamento_str)
        except:
            logger.error("Formato de faturamento inválido")
            return jsonify({"error": "Formato de faturamento inválido"}), 400

        # ---------------------------------------------------------
        # Cálculo do WACC
        # ---------------------------------------------------------
        json_file_name = 'static/Bd_Atividade.json'
        Beta, DE, EV_Sales = get_activity_values(json_file_name, atividade)
        
        # Verificar se os valores foram encontrados
        if Beta is None or DE is None or EV_Sales is None:
            return jsonify({"error": f"Não foi possível encontrar dados para a atividade: {atividade}"}), 404
        
        json_file_wacc = 'static/BDWacc.json'
        campos = ['RF', 'RM', 'CR', 'IA', 'IB', 'CT', 'IR']
        valores = get_values_from_json(json_file_wacc, campos)
        
        # Verificar se todos os valores foram encontrados
        for campo in campos:
            if campo not in valores or valores[campo] is None:
                return jsonify({"error": f"Valor {campo} não encontrado na base de dados"}), 404
        
        RF = valores['RF']
        RM = valores['RM']
        CR = valores['CR']
        IA = valores['IA']
        IB = valores['IB']
        CT = valores['CT']
        IR = valores['IR'] if lucroReal == 'Sim' else 0
        
        json_file_macro = 'static/BDMacro2.json'
        indices_ipca = ['IPCAano1', 'IPCAano2', 'IPCAano3', 'IPCAano4', 'IPCAano5']
        valores_ipca = get_ipca_values(json_file_macro, indices_ipca)
        
        # Verificar se todos os valores IPCA foram encontrados
        if len(valores_ipca) != len(indices_ipca):
            return jsonify({"error": "Valores IPCA incompletos"}), 404
            
        valores_numericos = [float(valor.strip().replace(',', '.')) for valor in valores_ipca.values()]
        IPCAlongo = sum(valores_numericos) / len(valores_numericos)
        
        json_file_size = 'static/BDSize.json'
        try:
            dfsize = pd.read_json(json_file_size, orient='records', encoding='UTF-8')
        except Exception as e:
            logger.error(f"Erro ao ler arquivo BDSize: {str(e)}")
            return jsonify({"error": "Erro ao ler base de dados de tamanho"}), 500
        
        try:
            EV_Sales_float = float(EV_Sales.strip().replace('.', '').replace(',', '.'))
            ValordaEmpresa = faturamento * EV_Sales_float
            premio = get_premio(dfsize, ValordaEmpresa)
        except Exception as e:
            logger.error(f"Erro ao calcular valor da empresa: {str(e)}")
            return jsonify({"error": "Erro ao calcular valor da empresa"}), 500
        
        # Converter valores para decimal
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
        
        # Cálculos WACC
        Betarealav = Beta * (1+(DE*(1-(IR/100))))
        KEnomUS = (RF + (Betarealav * RM) + CR + premio) / 100
        KEreal = (1 + KEnomUS) / (1 + (IA / 100)) - 1
        KenomBR = (1 + KEreal) * (1 + (IB / 100)) - 1
        
        KDnom = ((CT / 100) * (1 - (IR / 100)))
        KDreal = (1 + KDnom) / (1 + IPCAlongo) - 1
        
        # Verificação para evitar divisão por zero ou valores incorretos
        if DE == 0:
            partkd = 0
            partke = 1
        else:
            partkd = DE / (1 + DE)
            partke = 1 - partkd
        
        WaccReal = ((KDreal * partkd) + (KEreal * partke))
        WaccNominal = (KDnom * partkd) + (KenomBR * partke)
        
        # Criar resposta com todos os valores calculados
        logger.info(f"Cálculo de WACC realizado com sucesso para atividade: {atividade}")
        return jsonify({
            'wacc_nominal': WaccNominal,
            'wacc_real': WaccReal,
            'detalhes': {
                'Beta': round(Beta, 4),
                'DE': round(DE, 4),
                'RF': round(RF, 2),
                'RM': round(RM, 2), 
                'CR': round(CR, 2),
                'IA': round(IA, 2),
                'IB': round(IB, 2),
                'CT': round(CT, 2),
                'IR': round(IR, 2),
                'Premio': round(premio, 2),
                'Custo_Equity': round(KEnomUS, 4),
                'Custo_Debt': round(KDnom, 4),
                'partke': round(partke, 4),
                'partkd': round(partkd, 4),
                'EV_Sales': EV_Sales_float,
                'Betarealav': round(Betarealav, 4),
                'KEreal': round(KEreal, 4),
                'KenomBR': round(KenomBR, 4),
                'KDreal': round(KDreal, 4),
                'IPCAlongo': round(IPCAlongo, 2),
                'ValordaEmpresa': round(ValordaEmpresa, 2)
            }
        })

    except Exception as e:
        logger.error(f"Erro interno: {str(e)}")
        return jsonify({"error": f"Erro interno: {str(e)}"}), 500

# Rota para servir os arquivos JSON de atividades para o frontend
@app.route('/api/atividades')
def get_atividades():
    try:
        df_atividade = pd.read_json('static/Bd_Atividade.json', orient='records', encoding='UTF-8')
        atividades = df_atividade['atividade'].unique().tolist()
        return jsonify(atividades)
    except Exception as e:
        logger.error(f"Erro ao obter atividades: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Rota para servir arquivos estáticos (opcional - apenas se precisar servir HTML estático)
@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path:path>')
def serve_static(path):
    if path.endswith('.html'):
        return send_from_directory('templates', path)
    return send_from_directory('static', path)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)