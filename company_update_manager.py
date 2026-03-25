"""
company_update_manager.py — Backend para atualização de dados de empresas.

Gerencia jobs de atualização com:
- Tabela update_jobs para histórico
- SSE para progress em tempo real
- Filtros por setor/indústria/país/exchange
- Execução de scripts existentes como subprocessos
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Generator

DB_PATH = Path("data/damodaran_data_new.db")
PROGRESS_FILE = Path("cache/_company_update_progress.json")


# ─── Setup DB ──────────────────────────────────────────────────────────────────

def ensure_update_tables(db_path: Path | None = None):
    """Cria tabelas de controle se não existem."""
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS update_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            filters TEXT,
            total_items INTEGER DEFAULT 0,
            processed_items INTEGER DEFAULT 0,
            success_items INTEGER DEFAULT 0,
            error_items INTEGER DEFAULT 0,
            error_log TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            duration_seconds REAL
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_basic_data_id INTEGER NOT NULL,
            yahoo_code TEXT NOT NULL,
            price_date DATE NOT NULL,
            close_price REAL,
            volume INTEGER,
            market_cap REAL,
            enterprise_value REAL,
            currency TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(yahoo_code, price_date)
        );

        CREATE INDEX IF NOT EXISTS idx_price_history_yahoo ON price_history(yahoo_code);
        CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(price_date);
        CREATE INDEX IF NOT EXISTS idx_update_jobs_type ON update_jobs(job_type);
    """)
    conn.commit()
    conn.close()


# ─── Statistics ────────────────────────────────────────────────────────────────

def get_database_stats(db_path: Path | None = None) -> dict:
    """Retorna estatísticas completas da base."""
    conn = sqlite3.connect(str(db_path or DB_PATH))
    
    stats = {}
    
    # company_basic_data
    stats["total_companies"] = conn.execute(
        "SELECT COUNT(*) FROM company_basic_data"
    ).fetchone()[0]
    stats["with_yahoo_code"] = conn.execute(
        "SELECT COUNT(*) FROM company_basic_data WHERE yahoo_code IS NOT NULL AND yahoo_code != ''"
    ).fetchone()[0]
    stats["with_about"] = conn.execute(
        "SELECT COUNT(*) FROM company_basic_data WHERE about IS NOT NULL"
    ).fetchone()[0]
    stats["with_market_cap"] = conn.execute(
        "SELECT COUNT(*) FROM company_basic_data WHERE market_cap IS NOT NULL"
    ).fetchone()[0]
    
    # company_financials_historical
    stats["total_historical"] = conn.execute(
        "SELECT COUNT(*) FROM company_financials_historical"
    ).fetchone()[0]
    stats["companies_with_annual"] = conn.execute(
        "SELECT COUNT(DISTINCT company_basic_data_id) FROM company_financials_historical WHERE period_type='annual'"
    ).fetchone()[0]
    stats["companies_with_quarterly"] = conn.execute(
        "SELECT COUNT(DISTINCT company_basic_data_id) FROM company_financials_historical WHERE period_type='quarterly'"
    ).fetchone()[0]
    stats["with_ttm"] = conn.execute(
        "SELECT COUNT(*) FROM company_financials_historical WHERE total_revenue_ttm IS NOT NULL"
    ).fetchone()[0]
    
    # Datas
    stats["last_basic_update"] = conn.execute(
        "SELECT MAX(updated_at) FROM company_basic_data"
    ).fetchone()[0]
    stats["last_historical_fetch"] = conn.execute(
        "SELECT MAX(fetched_at) FROM company_financials_historical"
    ).fetchone()[0]
    
    # price_history
    try:
        stats["price_history_count"] = conn.execute(
            "SELECT COUNT(*) FROM price_history"
        ).fetchone()[0]
        stats["last_price_date"] = conn.execute(
            "SELECT MAX(price_date) FROM price_history"
        ).fetchone()[0]
    except Exception:
        stats["price_history_count"] = 0
        stats["last_price_date"] = None
    
    # Jobs recentes
    try:
        stats["recent_jobs"] = []
        rows = conn.execute("""
            SELECT id, job_type, status, total_items, processed_items, success_items, 
                   error_items, started_at, completed_at, duration_seconds, filters
            FROM update_jobs ORDER BY id DESC LIMIT 20
        """).fetchall()
        for r in rows:
            stats["recent_jobs"].append({
                "id": r[0], "job_type": r[1], "status": r[2],
                "total": r[3], "processed": r[4], "success": r[5],
                "errors": r[6], "started_at": r[7], "completed_at": r[8],
                "duration": r[9], "filters": r[10],
            })
    except Exception:
        stats["recent_jobs"] = []
    
    conn.close()
    return stats


def get_filter_options(db_path: Path | None = None) -> dict:
    """Retorna opções de filtro disponíveis."""
    conn = sqlite3.connect(str(db_path or DB_PATH))
    
    # Setores Yahoo
    sectors = [r[0] for r in conn.execute(
        "SELECT DISTINCT yahoo_sector FROM company_basic_data WHERE yahoo_sector IS NOT NULL ORDER BY yahoo_sector"
    ).fetchall()]
    
    # Indústrias Yahoo
    industries = [r[0] for r in conn.execute(
        "SELECT DISTINCT yahoo_industry FROM company_basic_data WHERE yahoo_industry IS NOT NULL ORDER BY yahoo_industry"
    ).fetchall()]
    
    # Países
    countries = [r[0] for r in conn.execute(
        "SELECT DISTINCT country FROM company_basic_data WHERE country IS NOT NULL ORDER BY country"
    ).fetchall()]
    
    # Exchanges (da damodaran_global)
    try:
        exchanges = [r[0] for r in conn.execute(
            "SELECT DISTINCT exchange FROM damodaran_global WHERE exchange IS NOT NULL ORDER BY exchange"
        ).fetchall()]
    except Exception:
        exchanges = []
    
    # Contagem por setor
    sector_counts = {}
    for r in conn.execute("""
        SELECT yahoo_sector, COUNT(*) FROM company_basic_data 
        WHERE yahoo_sector IS NOT NULL GROUP BY yahoo_sector ORDER BY COUNT(*) DESC
    """).fetchall():
        sector_counts[r[0]] = r[1]
    
    # Mapeamento setor → indústrias (ambos Yahoo)
    sector_industries = {}
    for r in conn.execute("""
        SELECT DISTINCT yahoo_sector, yahoo_industry FROM company_basic_data
        WHERE yahoo_sector IS NOT NULL AND yahoo_industry IS NOT NULL
        ORDER BY yahoo_sector, yahoo_industry
    """).fetchall():
        sector_industries.setdefault(r[0], []).append(r[1])
    
    conn.close()
    return {
        "sectors": sectors,
        "industries": industries,
        "countries": countries,
        "exchanges": exchanges,
        "sector_counts": sector_counts,
        "sector_industries": sector_industries,
    }


def count_affected(job_type: str, filters: dict, db_path: Path | None = None) -> int:
    """Conta quantas empresas seriam afetadas por um job."""
    conn = sqlite3.connect(str(db_path or DB_PATH))
    
    where_clauses = ["cbd.yahoo_code IS NOT NULL AND cbd.yahoo_code != ''"]
    params = []
    
    if filters.get("sector"):
        where_clauses.append("cbd.yahoo_sector = ?")
        params.append(filters["sector"])
    if filters.get("industry"):
        where_clauses.append("cbd.yahoo_industry = ?")
        params.append(filters["industry"])
    if filters.get("country"):
        where_clauses.append("cbd.country = ?")
        params.append(filters["country"])
    
    # Filtro adicional por tipo de job
    if job_type == "basic_data":
        where_clauses.append("(cbd.about IS NULL OR cbd.yahoo_sector IS NULL OR cbd.market_cap IS NULL)")
    elif job_type == "historical_annual":
        where_clauses.append("""cbd.id NOT IN (
            SELECT DISTINCT company_basic_data_id FROM company_financials_historical WHERE period_type='annual'
        )""")
    elif job_type == "historical_quarterly":
        where_clauses.append("""cbd.id NOT IN (
            SELECT DISTINCT company_basic_data_id FROM company_financials_historical WHERE period_type='quarterly'
        )""")
    
    where = " AND ".join(where_clauses)
    count = conn.execute(f"SELECT COUNT(*) FROM company_basic_data cbd WHERE {where}", params).fetchone()[0]
    conn.close()
    return count


def get_filtered_stats(filters: dict, db_path: Path | None = None) -> dict:
    """Retorna contagens filtradas para cada tipo de job e sumário."""
    conn = sqlite3.connect(str(db_path or DB_PATH))

    # Montar WHERE com filtros
    base_clauses = []
    params = []
    if filters.get("sector"):
        base_clauses.append("cbd.yahoo_sector = ?")
        params.append(filters["sector"])
    if filters.get("industry"):
        base_clauses.append("cbd.yahoo_industry = ?")
        params.append(filters["industry"])
    if filters.get("country"):
        base_clauses.append("cbd.country = ?")
        params.append(filters["country"])

    base_where = " AND ".join(base_clauses) if base_clauses else "1=1"

    # Total filtrado
    total = conn.execute(
        f"SELECT COUNT(*) FROM company_basic_data cbd WHERE {base_where}", params
    ).fetchone()[0]

    # Com yahoo code
    with_yahoo = conn.execute(
        f"SELECT COUNT(*) FROM company_basic_data cbd WHERE {base_where} AND cbd.yahoo_code IS NOT NULL AND cbd.yahoo_code != ''",
        params
    ).fetchone()[0]

    # Com about (proxy para dados cadastrais completos)
    with_about = conn.execute(
        f"SELECT COUNT(*) FROM company_basic_data cbd WHERE {base_where} AND cbd.about IS NOT NULL",
        params
    ).fetchone()[0]

    # Com market cap
    with_mcap = conn.execute(
        f"SELECT COUNT(*) FROM company_basic_data cbd WHERE {base_where} AND cbd.market_cap IS NOT NULL",
        params
    ).fetchone()[0]

    # Com dados anuais
    with_annual = conn.execute(
        f"""SELECT COUNT(DISTINCT cbd.id) FROM company_basic_data cbd
            INNER JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id AND cfh.period_type='annual'
            WHERE {base_where}""",
        params
    ).fetchone()[0]

    # Com dados trimestrais
    with_quarterly = conn.execute(
        f"""SELECT COUNT(DISTINCT cbd.id) FROM company_basic_data cbd
            INNER JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id AND cfh.period_type='quarterly'
            WHERE {base_where}""",
        params
    ).fetchone()[0]

    # Com TTM
    with_ttm = conn.execute(
        f"""SELECT COUNT(DISTINCT cbd.id) FROM company_basic_data cbd
            INNER JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id AND cfh.total_revenue_ttm IS NOT NULL
            WHERE {base_where}""",
        params
    ).fetchone()[0]

    # Total registros históricos filtrados
    total_hist = conn.execute(
        f"""SELECT COUNT(*) FROM company_financials_historical cfh
            INNER JOIN company_basic_data cbd ON cbd.id = cfh.company_basic_data_id
            WHERE {base_where}""",
        params
    ).fetchone()[0]

    conn.close()

    return {
        "total": total,
        "with_yahoo": with_yahoo,
        "with_about": with_about,
        "with_mcap": with_mcap,
        "with_annual": with_annual,
        "with_quarterly": with_quarterly,
        "with_ttm": with_ttm,
        "total_historical": total_hist,
        # Gaps por tipo de job
        "gaps": {
            "basic_data": max(0, total - with_about),
            "prices": max(0, with_yahoo - with_mcap),
            "historical_annual": max(0, with_yahoo - with_annual),
            "historical_quarterly": max(0, with_yahoo - with_quarterly),
        },
        # Completos por tipo
        "completed": {
            "basic_data": with_about,
            "prices": with_mcap,
            "historical_annual": with_annual,
            "historical_quarterly": with_quarterly,
            "calculate_ttm": with_ttm,
        },
    }


# ─── Job Execution ─────────────────────────────────────────────────────────────

# Active job tracking
_active_job: dict | None = None
_job_lock = threading.Lock()


def _write_progress(data: dict):
    """Escreve progresso para arquivo JSON."""
    PROGRESS_FILE.parent.mkdir(exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def get_progress() -> dict | None:
    """Lê progresso atual."""
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None


def start_job(job_type: str, filters: dict, db_path: Path | None = None) -> dict:
    """Inicia um job de atualização em background."""
    global _active_job
    
    with _job_lock:
        if _active_job and _active_job.get("status") == "running":
            return {"success": False, "error": "Já existe um job em execução", "job": _active_job}
    
    db = str(db_path or DB_PATH)
    conn = sqlite3.connect(db)
    ensure_update_tables(Path(db))
    
    # Criar registro do job
    filters_json = json.dumps(filters, ensure_ascii=False)
    cur = conn.execute(
        "INSERT INTO update_jobs (job_type, status, filters, started_at) VALUES (?, 'running', ?, ?)",
        (job_type, filters_json, datetime.now().isoformat())
    )
    job_id = cur.lastrowid
    conn.commit()
    conn.close()
    
    # Montar comando
    cmd = _build_command(job_type, filters)
    if not cmd:
        return {"success": False, "error": f"Tipo de job desconhecido: {job_type}"}
    
    # Iniciar em background
    _active_job = {
        "id": job_id,
        "job_type": job_type,
        "status": "running",
        "filters": filters,
        "started_at": datetime.now().isoformat(),
    }
    
    _write_progress({
        "job_id": job_id,
        "job_type": job_type,
        "status": "running",
        "phase": "Iniciando...",
        "pct": 0,
        "current": 0,
        "total": 0,
    })
    
    thread = threading.Thread(
        target=_run_job_thread,
        args=(job_id, job_type, cmd, db),
        daemon=True,
    )
    thread.start()
    
    return {"success": True, "job_id": job_id, "command": " ".join(cmd)}


def _build_command(job_type: str, filters: dict) -> list[str] | None:
    """Constrói o comando para executar em subprocess."""
    python = sys.executable
    
    workers = str(filters.get("workers", 3))
    rps = str(filters.get("max_rps", 2))
    force = filters.get("force", False)
    
    # Filtros de sector/industry/country — só para scripts que suportam
    sector = filters.get("sector", "")
    industry = filters.get("industry", "")
    country = filters.get("country", "")
    
    def _hist_args():
        """Args para fetch_historical_financials.py (suporta --sector/--industry/--country)."""
        args = ["--workers", workers, "--max-rps", rps]
        if sector:
            args.extend(["--sector", sector])
        if industry:
            args.extend(["--industry", industry])
        if country:
            args.extend(["--country", country])
        if force:
            args.append("--force")
        return args
    
    if job_type == "discover_tickers":
        return [python, "scripts/discover_new_tickers.py", "--source", "all",
                "--save", f"cache/discovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"]
    
    elif job_type == "basic_data":
        cmd = [python, "scripts/update_company_data_from_yahoo_fast.py",
               "--workers", workers, "--max-rps", rps]
        if sector:
            cmd.extend(["--sector", sector])
        if industry:
            cmd.extend(["--industry", industry])
        if country:
            cmd.extend(["--country", country])
        if force:
            cmd.append("--force")
        return cmd
    
    elif job_type == "prices":
        cmd = [python, "scripts/update_company_data_from_yahoo_fast.py",
               "--workers", workers, "--max-rps", rps, "--force"]
        if sector:
            cmd.extend(["--sector", sector])
        if industry:
            cmd.extend(["--industry", industry])
        if country:
            cmd.extend(["--country", country])
        return cmd
    
    elif job_type == "historical_annual":
        return [python, "scripts/fetch_historical_financials.py"] + _hist_args()
    
    elif job_type == "historical_quarterly":
        return [python, "scripts/fetch_historical_financials.py", "--quarterly"] + _hist_args()
    
    elif job_type == "calculate_ttm":
        cmd = [python, "scripts/calculate_ttm.py"]
        if sector:
            cmd.extend(["--sector", sector])
        return cmd
    
    elif job_type == "recalculate_ratios":
        return [python, "scripts/recalculate_ratios.py"]
    
    elif job_type == "recalculate_fx":
        cmd = [python, "scripts/recalculate_fx_rates.py"]
        if sector:
            cmd.extend(["--sector", sector])
        return cmd
    
    elif job_type == "full_pipeline":
        # Pipeline inicia com annual fetch, demais steps encadeados em _run_pipeline_continuation
        return [python, "scripts/fetch_historical_financials.py"] + _hist_args()
    
    return None


def _run_job_thread(job_id: int, job_type: str, cmd: list[str], db_path: str):
    """Executa o job em thread separada, monitorando saída."""
    global _active_job
    start_time = time.time()
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            cwd=str(Path(__file__).parent),
        )
        
        output_lines = []
        processed = 0
        success = 0
        errors = 0
        total = 0
        
        for line in process.stdout:
            line = line.strip()
            if not line:
                continue
            output_lines.append(line)
            
            # Parse progress from script output
            if "Processando" in line or "Processing" in line:
                processed += 1
            if "OK" in line or "✓" in line or "sucesso" in line.lower():
                success += 1
            if "ERRO" in line or "Error" in line or "✗" in line:
                errors += 1
            if "Total:" in line or "empresas" in line.lower():
                import re
                match = re.search(r'(\d[\d,.]+)\s*(empresas|companies|total)', line.lower())
                if match:
                    try:
                        total = int(match.group(1).replace(",", "").replace(".", ""))
                    except ValueError:
                        pass
            
            # Update progress
            pct = min(99, int(100 * processed / max(total, 1))) if total > 0 else 0
            _write_progress({
                "job_id": job_id,
                "job_type": job_type,
                "status": "running",
                "phase": line[:120],
                "pct": pct,
                "current": processed,
                "total": total,
                "success": success,
                "errors": errors,
            })
        
        process.wait()
        duration = time.time() - start_time
        final_status = "completed" if process.returncode == 0 else "error"
        
        # Se é pipeline completo, encadear próximo passo
        if job_type == "full_pipeline" and process.returncode == 0:
            _run_pipeline_continuation(job_id, db_path, output_lines)
        
        # Atualizar DB
        conn = sqlite3.connect(db_path)
        conn.execute("""
            UPDATE update_jobs SET 
                status = ?, total_items = ?, processed_items = ?, 
                success_items = ?, error_items = ?,
                completed_at = ?, duration_seconds = ?,
                error_log = ?
            WHERE id = ?
        """, (final_status, total, processed, success, errors,
              datetime.now().isoformat(), duration,
              "\n".join(output_lines[-50:]) if output_lines else None,
              job_id))
        conn.commit()
        conn.close()
        
        _write_progress({
            "job_id": job_id,
            "job_type": job_type,
            "status": final_status,
            "phase": "Concluído!" if final_status == "completed" else "Erro",
            "pct": 100 if final_status == "completed" else pct,
            "current": processed,
            "total": total,
            "success": success,
            "errors": errors,
            "duration": round(duration, 1),
        })
        
    except Exception as e:
        duration = time.time() - start_time
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE update_jobs SET status='error', error_log=?, completed_at=?, duration_seconds=? WHERE id=?",
            (str(e), datetime.now().isoformat(), duration, job_id)
        )
        conn.commit()
        conn.close()
        
        _write_progress({
            "job_id": job_id,
            "job_type": job_type,
            "status": "error",
            "phase": f"Erro: {e}",
            "pct": 0,
        })
    finally:
        with _job_lock:
            _active_job = None


def _run_pipeline_continuation(job_id: int, db_path: str, prev_output: list):
    """Encadeia steps do pipeline: quarterly → TTM → ratios → FX."""
    python = sys.executable
    steps = [
        ("quarterly", [python, "scripts/fetch_historical_financials.py", "--quarterly", "--workers", "3", "--max-rps", "2"]),
        ("TTM", [python, "scripts/calculate_ttm.py"]),
        ("Ratios", [python, "scripts/recalculate_ratios.py"]),
        ("FX", [python, "scripts/recalculate_fx_rates.py"]),
    ]
    
    for step_name, cmd in steps:
        _write_progress({
            "job_id": job_id,
            "job_type": "full_pipeline",
            "status": "running",
            "phase": f"Pipeline: {step_name}...",
            "pct": 50,
        })
        
        result = subprocess.run(
            cmd,
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=str(Path(__file__).parent),
        )
        prev_output.append(f"--- {step_name} ---")
        prev_output.append(result.stdout[-500:] if result.stdout else "")
        if result.returncode != 0:
            prev_output.append(f"ERRO: {result.stderr[-300:]}")
            break


def cancel_job() -> dict:
    """Cancela o job ativo."""
    global _active_job
    with _job_lock:
        if _active_job:
            _active_job["status"] = "cancelled"
            _write_progress({
                "job_id": _active_job.get("id"),
                "status": "cancelled",
                "phase": "Cancelado pelo usuário",
                "pct": 0,
            })
            _active_job = None
            return {"success": True}
    return {"success": False, "error": "Nenhum job ativo"}
