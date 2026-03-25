"""
Versão MULTITHREADED do update_company_data_from_yahoo.py

Usa ThreadPoolExecutor para fazer chamadas Yahoo em paralelo (I/O bound),
enquanto um único thread principal escreve no banco SQLite (sem conflito de locks).

Detecta rate-limit do Yahoo e pausa automaticamente para esperar o desbloqueio.

Uso:
    python scripts/update_company_data_from_yahoo_fast.py --workers 4 --limit 50000
    python scripts/update_company_data_from_yahoo_fast.py --workers 4 --max-rps 2
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

# Fix SSL para caminhos com espaços (OneDrive)
_cacert = Path(r"C:\cacerts\cacert.pem")
if _cacert.exists() and "CURL_CA_BUNDLE" not in os.environ:
    os.environ["CURL_CA_BUNDLE"] = str(_cacert)

import yfinance as yf
from yfinance.exceptions import YFRateLimitError
from yfinance.data import YfData

# ---------------------------------------------------------------------------
# Rate limiter thread-safe
# ---------------------------------------------------------------------------

class RateLimiter:
    """Limita número máximo de chamadas por segundo entre múltiplas threads."""

    def __init__(self, max_per_second: float):
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()


_rate_limiter: RateLimiter | None = None

# Flag global para sinalizar rate-limit a todas as threads
_rate_limited = threading.Event()
_RATE_LIMIT_PAUSE = 300  # 5 minutos
_reset_lock = threading.Lock()


def _reset_yf_session() -> None:
    """Reseta a sessão do yfinance para obter novos cookies/crumbs."""
    with _reset_lock:
        if YfData in YfData._instances:
            del YfData._instances[YfData]


# ---------------------------------------------------------------------------
# Funções de busca Yahoo (thread-safe, sem acesso ao banco)
# ---------------------------------------------------------------------------

def _wait_if_rate_limited() -> None:
    """Se rate-limit foi detectado, espera todas as threads pausarem."""
    if _rate_limited.is_set():
        _rate_limited.wait()  # bloqueia até o event ser cleared

# ---------------------------------------------------------------------------
# Funções de busca Yahoo (thread-safe, sem acesso ao banco)
# ---------------------------------------------------------------------------

def fetch_data_from_yahoo(yahoo_code: str) -> dict | None:
    """Busca dados de uma empresa no Yahoo Finance."""
    _wait_if_rate_limited()
    if _rate_limiter:
        _rate_limiter.acquire()
    try:
        ticker = yf.Ticker(yahoo_code)
        info = ticker.get_info()
    except YFRateLimitError:
        return "RATE_LIMITED"  # type: ignore[return-value]
    except Exception:
        return None

    qt = info.get("quoteType", "")
    if qt in ("NONE", ""):
        return None

    result: dict = {}
    about = info.get("longBusinessSummary") or info.get("description")
    if about:
        result["about"] = str(about).strip()

    ev = info.get("enterpriseValue")
    if ev is not None:
        try:
            result["enterprise_value"] = float(ev)
        except (ValueError, TypeError):
            pass

    mc = info.get("marketCap")
    if mc is not None:
        try:
            result["market_cap"] = float(mc)
        except (ValueError, TypeError):
            pass

    currency = info.get("currency") or info.get("financialCurrency")
    if currency:
        result["currency"] = str(currency).strip()

    for field in ("sector", "sectorKey", "industry", "industryKey"):
        val = info.get(field)
        if val:
            result[field] = str(val).strip()

    for src in ("city", "country", "state", "website"):
        val = info.get(src)
        if val:
            result[src] = str(val).strip()

    return result if result else None


def fetch_one(row: tuple[int, str | None, str | None, str | None]) -> tuple[int, dict | None | str]:
    """Worker: busca dados de uma empresa. Retorna (id, data_dict_ou_None)."""
    row_id, yahoo_code, ticker, company_name = row
    if not yahoo_code:
        return row_id, None
    data = fetch_data_from_yahoo(yahoo_code)
    return row_id, data


# ---------------------------------------------------------------------------
# Acesso ao banco (single-thread no main)
# ---------------------------------------------------------------------------

def fetch_candidates(
    conn: sqlite3.Connection,
    limit: int | None,
    exchanges: list[str] | None,
    force: bool,
    extra_filters: dict | None = None,
) -> list[tuple[int, str | None, str | None, str | None]]:
    where = "WHERE yahoo_code IS NOT NULL AND TRIM(yahoo_code) != '' AND COALESCE(yahoo_no_data, 0) = 0"
    params: list[str] = []

    if not force:
        where += " AND (about IS NULL OR TRIM(about) = '' OR enterprise_value IS NULL OR market_cap IS NULL OR yahoo_sector IS NULL)"

    if exchanges:
        conditions = []
        for ex in [e.strip() for e in exchanges if e.strip()]:
            conditions.append("ticker LIKE ?")
            params.append(f"{ex}:%")
        if conditions:
            where += f" AND ({' OR '.join(conditions)})"

    if extra_filters:
        if extra_filters.get("sector"):
            where += " AND yahoo_sector = ?"
            params.append(extra_filters["sector"])
        if extra_filters.get("industry"):
            where += " AND industry = ?"
            params.append(extra_filters["industry"])
        if extra_filters.get("country"):
            where += " AND country = ?"
            params.append(extra_filters["country"])

    sql = f"""
        SELECT id, yahoo_code, ticker, company_name
        FROM company_basic_data
        {where}
        ORDER BY id
    """
    if limit and limit > 0:
        sql += f" LIMIT {int(limit)}"

    return conn.execute(sql, params).fetchall()


def apply_update(cursor: sqlite3.Cursor, row_id: int, data: dict, dta_referencia: str) -> None:
    """Aplica um UPDATE no banco para uma empresa."""
    sets = ["updated_at = CURRENT_TIMESTAMP"]
    values: list = []

    if "about" in data:
        sets.append("about = COALESCE(about, ?)")
        values.append(data["about"])

    if "enterprise_value" in data:
        sets.append("enterprise_value = ?")
        values.append(data["enterprise_value"])
    if "market_cap" in data:
        sets.append("market_cap = ?")
        values.append(data["market_cap"])
    if "currency" in data:
        sets.append("currency = ?")
        values.append(data["currency"])

    for field, col in (
        ("sector", "yahoo_sector"),
        ("sectorKey", "yahoo_sector_key"),
        ("industry", "yahoo_industry"),
        ("industryKey", "yahoo_industry_key"),
        ("city", "yahoo_city"),
        ("country", "yahoo_country"),
        ("state", "yahoo_state"),
        ("website", "yahoo_website"),
    ):
        if field in data:
            sets.append(f"{col} = ?")
            values.append(data[field])

    if "enterprise_value" in data or "market_cap" in data:
        sets.append("dta_referencia = ?")
        values.append(dta_referencia)

    values.append(row_id)
    cursor.execute(
        f"UPDATE company_basic_data SET {', '.join(sets)} WHERE id = ?",
        values,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Update Yahoo data (multithreaded)")
    parser.add_argument("--db-path", default="data/damodaran_data_new.db")
    parser.add_argument("--limit", type=int, default=50000)
    parser.add_argument("--workers", type=int, default=8,
                        help="Número de threads paralelas para chamadas Yahoo (padrão: 8)")
    parser.add_argument("--max-rps", type=float, default=5.0,
                        help="Máximo de requisições por segundo (padrão: 5)")
    parser.add_argument("--exchanges", type=str, default="")
    parser.add_argument("--sector", type=str, default="", help="Filtrar por Yahoo sector")
    parser.add_argument("--industry", type=str, default="", help="Filtrar por Yahoo industry")
    parser.add_argument("--country", type=str, default="", help="Filtrar por country")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dta-referencia", type=str, default="")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    dta_ref = args.dta_referencia or date.today().isoformat()

    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    global _rate_limiter
    _rate_limiter = RateLimiter(args.max_rps)

    exchanges = [x.strip() for x in args.exchanges.split(",") if x.strip()] if args.exchanges else None
    extra_filters = {}
    if args.sector:
        extra_filters["sector"] = args.sector
    if args.industry:
        extra_filters["industry"] = args.industry
    if args.country:
        extra_filters["country"] = args.country
    rows = fetch_candidates(conn, args.limit, exchanges, args.force, extra_filters)

    if not rows:
        print("Nenhum registro elegível.")
        return

    total = len(rows)
    print(f"Registros a processar: {total}")
    print(f"Workers: {args.workers}")
    print(f"Data de referência: {dta_ref}")
    if exchanges:
        print(f"Exchanges: {', '.join(exchanges)}")
    print(flush=True)

    cursor = conn.cursor()
    updated = 0
    failed = 0
    rate_pauses = 0
    t0 = time.time()

    # Processar em batches para dar feedback e commitar
    batch_size = args.workers * 5
    idx = 0
    while idx < total:
        batch = rows[idx : idx + batch_size]
        retry_rows: list[tuple[int, str | None, str | None, str | None]] = []
        hit_rate_limit = False

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(fetch_one, row): row for row in batch}

            for future in as_completed(futures):
                row_id, data = future.result()
                if data == "RATE_LIMITED":
                    hit_rate_limit = True
                    retry_rows.append(futures[future])
                elif data and isinstance(data, dict):
                    apply_update(cursor, row_id, data, dta_ref)
                    updated += 1
                else:
                    failed += 1

        conn.commit()

        if hit_rate_limit:
            rate_pauses += 1
            pause = _RATE_LIMIT_PAUSE * min(rate_pauses, 3)  # max 15min
            print(
                f"\n  ⚠ RATE LIMITED! Pausa de {pause//60}min... "
                f"(pausa #{rate_pauses}) — progresso salvo.",
                flush=True,
            )
            time.sleep(pause)
            _reset_yf_session()
            print("  ↻ Sessão resetada, retomando...", flush=True)
            # Re-inserir rows que falharam por rate-limit no início da fila
            remaining_rows = retry_rows + rows[idx + batch_size:]
            rows = rows[:idx] + remaining_rows  # manter processados + retry + restante  
            total = len(rows)
            # Não avançar idx - retry do batch
            continue
        else:
            # Reset rate_pauses counter após batch sem bloqueio
            if rate_pauses > 0:
                rate_pauses = 0

        idx += len(batch)
        elapsed = time.time() - t0
        rate = idx / elapsed if elapsed > 0 else 0
        remaining = (total - idx) / rate if rate > 0 else 0
        print(
            f"  [{idx:,}/{total:,}] "
            f"ok={updated:,} fail={failed:,} | "
            f"{rate:.1f}/s | "
            f"ETA {remaining/60:.0f}min",
            flush=True,
        )

    conn.commit()
    elapsed = time.time() - t0

    print()
    print(f"=== CONCLUÍDO em {elapsed/60:.1f} minutos ===")
    print(f"Atualizados: {updated:,}")
    print(f"Sem dados: {failed:,}")
    print(f"Taxa média: {total/elapsed:.1f}/s")

    # Resumo
    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN about IS NOT NULL AND TRIM(about) != '' THEN 1 ELSE 0 END) as com_about,
            SUM(CASE WHEN enterprise_value IS NOT NULL THEN 1 ELSE 0 END) as com_ev,
            SUM(CASE WHEN market_cap IS NOT NULL THEN 1 ELSE 0 END) as com_mc,
            SUM(CASE WHEN yahoo_sector IS NOT NULL THEN 1 ELSE 0 END) as com_sector
        FROM company_basic_data
    """).fetchone()
    print(f"\nBanco total: {stats[0]:,} | about={stats[1]:,} | EV={stats[2]:,} | MCap={stats[3]:,} | sector={stats[4]:,}")

    conn.close()


if __name__ == "__main__":
    main()
