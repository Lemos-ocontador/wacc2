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

# Definir caminhos base para arquivos estáticos
STATIC_BASE = os.environ.get('STATIC_BASE_PATH', 'static')

# --------------------------------------------
# Funções auxiliares
# --------------------------------------------
def get_activity_values(json_file_name, atividade):
    try:
        # Ajuste para considerar caminho base
        full_path = os.path.join(STATIC_BASE, os.path.basename(json_file_name))
        dfatividade = pd.read_json(full_path, orient='records', encoding='UTF-8')
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
        # Ajuste para considerar caminho base
        full_path = os.path.join(STATIC_BASE, os.path.basename(json_file_name))
        df = pd.read_json(full_path, orient='records', encoding='UTF-8')
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
        # Ajuste para considerar caminho base
        full_path = os.path.join(STATIC_BASE, os.path.basename(json_file_name))
        df = pd.read_json(full_path, orient='records', encoding='UTF-8')
        valores = {}
        for indice in indices:
            valor = df.loc[df['indice'] == indice, 'Valor'].values[0]
            valores[indice] = valor
        return valores
    except Exception as e:
        logger.error(f"Erro ao ler valores IPCA: {str(e)}")
        return {}

# Resto das funções permanece igual...

# --------------------------------------------
# Rotas do Flask - Com ajustes para GitHub Pages
# --------------------------------------------

# Rota raiz que serve o index.html
@app.route('/')
def index():
    return render_template('index.html')

# Rota específica para a página da calculadora WACC
@app.route('/wacc-calculator')
def wacc_calculator():
    """Carrega a página da calculadora WACC"""
    return render_template('calculator-page.html')

# API endpoint para servir JSON de atividades (para o frontend puro)
@app.route('/static/<filename>')
def serve_static_json(filename):
    """Serve arquivos JSON estáticos"""
    return send_from_directory('static', filename)

# API endpoint para o cálculo WACC
@app.route('/api/calcular-wacc', methods=['POST'])
def calcular_wacc():
    # Código de cálculo WACC permanece igual...

# Rota para servir os arquivos JSON de atividades para o frontend
@app.route('/api/atividades')
def get_atividades():
    try:
        df_atividade = pd.read_json(os.path.join(STATIC_BASE, 'Bd_Atividade.json'), orient='records', encoding='UTF-8')
        atividades = df_atividade['atividade'].unique().tolist()
        return jsonify(atividades)
    except Exception as e:
        logger.error(f"Erro ao obter atividades: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Rota para servir arquivos estáticos (ajustada para GitHub Pages)
@app.route('/<path:path>')
def serve_static(path):
    if path.startswith('static/'):
        # Serve arquivos estáticos como JSON, CSS, JS
        return send_from_directory('.', path)
    elif path.endswith('.html'):
        # Serve páginas HTML
        return send_from_directory('templates', path)
    else:
        # Outros arquivos
        return send_from_directory('.', path)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
