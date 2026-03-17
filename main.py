"""
Entrypoint para Google App Engine.
O GAE procura por um objeto WSGI chamado 'app' em main.py.
"""
from app import app

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
