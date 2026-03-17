# Deploy Google Cloud — WACC Hub

> Última atualização: Março/2026

## 1) Infraestrutura

| Item | Valor |
|------|-------|
| **Projeto GCloud** | `dataanloc` (ID: 309801295606) |
| **Conta de faturamento** | `01666D-CD7AEA-188BA6` |
| **Serviço** | App Engine Standard |
| **Runtime** | Python 3.12 |
| **Região** | `southamerica-east1` (São Paulo) |
| **Instância** | F4_1G (1 vCPU, 1 GB RAM) |
| **URL produção** | `https://dataanloc.rj.r.appspot.com` |
| **Service Account** | `dataanloc@appspot.gserviceaccount.com` |

---

## 2) Arquivos de Configuração

| Arquivo | Função |
|---------|--------|
| `app.yaml` | Config do App Engine (runtime, scaling, handlers) |
| `main.py` | Entrypoint WSGI (`from app import app`) |
| `requirements-gae.txt` | Dependências para GAE (sem selenium/curl_cffi, com gunicorn) |
| `.gcloudignore` | Arquivos excluídos do deploy |
| `deploy_gcloud.bat` | Script automatizado de deploy |

---

## 3) Decisões de Arquitetura

### SQLite Embarcado (Read-Only)

O banco SQLite (~232 MB) é deployado junto com a aplicação. No GAE Standard, o filesystem da app é **read-only**, então:

- **Modo immutable**: Todas as conexões SQLite usam `?immutable=1` na URI, eliminando criação de journal/WAL.
- **Sem escrita no banco em produção**: Inserções/updates no banco só ocorrem localmente. O deploy carrega um snapshot estático.
- **Função centralizada**: `get_db()` em `app.py` e `_connect_db()` nos módulos detectam `IS_GAE` e aplicam modo immutable automaticamente.

```python
# Padrão usado em todos os módulos
def get_db(db_path=None):
    path = db_path or DB_PATH
    if IS_GAE:
        abs_path = os.path.abspath(path)
        uri = 'file:' + abs_path + '?immutable=1'
        return sqlite3.connect(uri, uri=True)
    return sqlite3.connect(path)
```

### Cache em `/tmp`

O GAE Standard tem um filesystem efêmero gravável em `/tmp`. Cache de cálculos WACC e extrações vai para `/tmp/cache` no GAE (vs `cache/` localmente).

### Instância F4_1G

Necessária porque o banco SQLite (232 MB) + pandas/numpy excedem o limite de 384 MB da instância F1 padrão.

### Gunicorn com `--preload`

```
gunicorn -b :$PORT -w 2 --threads 4 --timeout 120 --preload app:app
```

- `--preload`: Carrega a app antes de forkar workers, evitando duplicação de memória e race conditions no acesso ao banco.
- `-w 2 --threads 4`: 2 workers × 4 threads = 8 requests concorrentes.
- `--timeout 120`: Queries pesadas (consolidado, treemap) podem demorar.

---

## 4) Deploy

### Via Script (Recomendado)

```cmd
deploy_gcloud.bat
```

O script automatiza: backup do requirements.txt → troca por requirements-gae.txt → deploy → restaura.

### Manual

```powershell
cd "c:\Users\eduar\OneDrive - Anloc\Programação\bd_damodaran"

# 1. Trocar requirements
Copy-Item requirements.txt requirements-local-backup.txt
Copy-Item requirements-gae.txt requirements.txt

# 2. Deploy
gcloud app deploy app.yaml --project=dataanloc --quiet

# 3. Restaurar requirements
Copy-Item requirements-local-backup.txt requirements.txt
Remove-Item requirements-local-backup.txt
```

### Pós-Deploy: Verificação

```powershell
# Aguardar cold start (~40s) e testar
Start-Sleep -Seconds 40

# Homepage
(Invoke-WebRequest -Uri "https://dataanloc.rj.r.appspot.com/" -UseBasicParsing).StatusCode

# APIs
(Invoke-WebRequest -Uri "https://dataanloc.rj.r.appspot.com/api/historico/summary" -UseBasicParsing).StatusCode
(Invoke-WebRequest -Uri "https://dataanloc.rj.r.appspot.com/api/historico_filter_options" -UseBasicParsing).StatusCode
(Invoke-WebRequest -Uri "https://dataanloc.rj.r.appspot.com/api/historico/search?limit=3" -UseBasicParsing).StatusCode
```

### Logs

```powershell
# Últimos 30 logs
gcloud logging read 'resource.type="gae_app"' --project=dataanloc --limit=30 --format="value(textPayload)" --freshness=5m

# Tail em tempo real
gcloud app logs tail -s default --project=dataanloc
```

---

## 5) .gcloudignore — O que sobe e o que não sobe

### Incluído no deploy

- `app.py`, `main.py`, módulos Python (*.py raiz)
- `data/damodaran_data_new.db` (banco SQLite — **essencial**)
- `data/*.json` (catálogos e mapeamentos)
- `data_extractors/` (pacote de extratores)
- `templates/` (HTML/Jinja2)
- `static/` (CSS, JS, JSON)
- `requirements.txt` (trocado por requirements-gae.txt antes do deploy)

### Excluído do deploy

- `.venv/`, `__pycache__/`, `.git/`, `.vscode/`
- `cache/` (recriado em `/tmp` no GAE)
- `scripts/`, `docs/`, `backups/`, `videos/`
- `data/damodaran_data/` (Excels brutos)
- `data/*.csv`, `data/*.xlsx`, `data/*.xls`
- `*.md`, `*.bat`, `*.log`
- `_temp_*.py`

---

## 6) Adaptações no Código para GAE

### Detecção de ambiente

```python
IS_GAE = os.environ.get('GAE_ENV', '').startswith('standard')
```

### Módulos com `_connect_db` (modo immutable)

- `wacc_data_connector.py`
- `field_categories_manager.py`
- `data_source_manager.py`

### DataSourceManager — modo read-only

No GAE, `_ensure_log_table()` falha (não pode CREATE TABLE com immutable). A classe captura a exceção e ativa `self.read_only = True`, desabilitando `_log_start()`, `_log_complete()` e `get_update_history()`.

### Cache dir — fallback `/tmp`

- `wacc_calculator.py`: try/except em `self.cache_dir.mkdir()` → fallback `/tmp/cache`
- `data_extractors/wacc_data_manager.py`: try/except em `os.makedirs()` → fallback `/tmp/cache`

---

## 7) Scaling e Custos

### Configuração atual (`app.yaml`)

```yaml
automatic_scaling:
  min_instances: 0        # Scale to zero quando ocioso
  max_instances: 3        # Limite de instâncias
  min_idle_instances: 0   # Nenhuma instância ociosa mínima
  max_idle_instances: 1
  target_cpu_utilization: 0.65
```

### Estimativa de custo

- **Instância F4_1G**: ~$0.30/hora de instância ativa
- **Scale to zero**: Sem custo quando não há requisições
- **Cold start**: ~30-40s na primeira requisição após inatividade
- **Storage**: ~300 MB (app + banco) dentro do free tier de Cloud Build

### Dicas para reduzir custo

- Manter `min_instances: 0` (scale to zero)
- Limitar `max_instances` ao mínimo aceitável
- Monitorar via Console GCloud → App Engine → Dashboard

---

## 8) Troubleshooting

| Problema | Causa | Solução |
|----------|-------|---------|
| `502 Bad Gateway` | App crashou no startup | Verificar logs: `gcloud app logs tail` |
| `sqlite3.OperationalError: unable to open database file` | SQLite tentando criar journal em filesystem read-only | Usar `?immutable=1` na URI de conexão |
| `sqlite3.DatabaseError: database disk image is malformed` | Race condition ao copiar DB para `/tmp` | Não usar cópia — usar `?immutable=1` direto no path original |
| `Exceeded hard memory limit of 384 MiB` | Banco + libs > limite F1 | Usar `instance_class: F4_1G` (1 GB) |
| `OSError: Read-only file system: 'cache'` | Tentando criar diretório cache no GAE | Usar `/tmp/cache` no GAE |
| Deploy falha com `PERMISSION_DENIED` | Cloud Build sem permissão no Storage | `gcloud projects add-iam-policy-binding` com `roles/storage.admin` |
| APIs retornam dados mas páginas em branco | JS não carregou | Verificar static handlers no `app.yaml` |

---

## 9) Comandos Úteis

```powershell
# Status do projeto
gcloud app describe --project=dataanloc

# Listar versões deployadas
gcloud app versions list --project=dataanloc

# Limpar versões antigas (manter apenas a ativa)
gcloud app versions delete <VERSION> --project=dataanloc --quiet

# Abrir no browser
gcloud app browse --project=dataanloc

# Ver billing
gcloud billing accounts describe 01666D-CD7AEA-188BA6
```
