"""
Atualiza about, enterprise_value, market_cap e currency de empresas usando Yahoo Finance.

Requer que o yahoo_code já esteja corrigido (use fix_yahoo_codes.py primeiro).
Também tenta buscar via yf.Search como fallback caso o yahoo_code não retorne dados.

Uso:
    python scripts/update_company_data_from_yahoo.py --limit 500
    python scripts/update_company_data_from_yahoo.py --exchanges BSE --limit 1000
    python scripts/update_company_data_from_yahoo.py --force --limit 200
    python scripts/update_company_data_from_yahoo.py --only-financial  (só atualiza EV/MCap)
"""
from __future__ import annotations

import argparse
import os
import random
import sqlite3
import time
from datetime import date, datetime, timezone
from pathlib import Path

# Fix SSL para caminhos com espaços (OneDrive)
_cacert = Path(r"C:\cacerts\cacert.pem")
if _cacert.exists() and "CURL_CA_BUNDLE" not in os.environ:
    os.environ["CURL_CA_BUNDLE"] = str(_cacert)

import yfinance as yf


def _clean_company_name(name: str | None) -> str | None:
    if not name:
        return None
    clean = name.strip()
    paren = clean.rfind("(")
    if paren > 0:
        clean = clean[:paren].strip()
    return clean if clean else None


def _extract_exchange(ticker: str | None) -> str | None:
    if not ticker or ":" not in ticker:
        return None
    return ticker.split(":", 1)[0].strip()


def fetch_data_from_yahoo(yahoo_code: str) -> dict | None:
    """Busca dados de uma empresa no Yahoo Finance."""
    try:
        ticker = yf.Ticker(yahoo_code)
        info = ticker.get_info()
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

    # Classificação
    for field in ("sector", "sectorKey", "industry", "industryKey"):
        val = info.get(field)
        if val:
            result[field] = str(val).strip()

    # Geolocalização
    for src, dst in (("city", "city"), ("country", "country"), ("state", "state"), ("website", "website")):
        val = info.get(src)
        if val:
            result[dst] = str(val).strip()

    return result if result else None


def search_and_fetch(company_name: str | None, max_results: int = 3) -> tuple[str | None, dict | None]:
    """Fallback: busca via yf.Search e depois busca dados."""
    clean = _clean_company_name(company_name)
    if not clean:
        return None, None

    try:
        search = yf.Search(clean, max_results=max_results)
        quotes = getattr(search, "quotes", []) or []
    except Exception:
        return None, None

    for item in quotes:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol", "")
        if not symbol:
            continue
        data = fetch_data_from_yahoo(symbol)
        if data:
            return symbol, data

    return None, None


def fetch_candidates(
    conn: sqlite3.Connection,
    limit: int | None,
    exchanges: list[str] | None,
    force: bool,
    only_financial: bool,
) -> list[tuple[int, str | None, str | None, str | None]]:
    cursor = conn.cursor()

    where = "WHERE yahoo_code IS NOT NULL AND TRIM(yahoo_code) != ''"
    params: list[str] = []

    if only_financial:
        where += " AND (enterprise_value IS NULL OR market_cap IS NULL)"
    elif not force:
        where += " AND (about IS NULL OR TRIM(about) = '' OR enterprise_value IS NULL OR market_cap IS NULL)"

    if exchanges:
        conditions = []
        for ex in [e.strip() for e in exchanges if e.strip()]:
            conditions.append("ticker LIKE ?")
            params.append(f"{ex}:%")
        if conditions:
            where += f" AND ({' OR '.join(conditions)})"

    sql = f"""
        SELECT id, yahoo_code, ticker, company_name
        FROM company_basic_data
        {where}
        ORDER BY id
    """
    if limit and limit > 0:
        sql += f" LIMIT {int(limit)}"

    return cursor.execute(sql, params).fetchall()


def _commit_with_retry(conn: sqlite3.Connection, max_retries: int = 10) -> None:
    """Commit com retry para lidar com database locked em acesso concorrente."""
    for attempt in range(max_retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                wait = 0.5 + random.random() * 1.5
                time.sleep(wait)
            else:
                raise


def update_records(
    conn: sqlite3.Connection,
    rows: list[tuple[int, str | None, str | None, str | None]],
    sleep_seconds: float,
    use_search_fallback: bool,
    dta_referencia: str,
) -> tuple[int, int]:
    cursor = conn.cursor()
    updated = 0
    failed = 0
    total = len(rows)

    for i, (row_id, yahoo_code, ticker, company_name) in enumerate(rows, 1):
        if i % 100 == 0 or i == 1:
            print(f"  Progresso: {i}/{total} (atualizados={updated}, falha={failed})")

        data = fetch_data_from_yahoo(yahoo_code) if yahoo_code else None

        new_yahoo_code = None
        if not data and use_search_fallback:
            new_yahoo_code, data = search_and_fetch(company_name)

        if data:
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

            # Classificação
            if "sector" in data:
                sets.append("yahoo_sector = ?")
                values.append(data["sector"])
            if "sectorKey" in data:
                sets.append("yahoo_sector_key = ?")
                values.append(data["sectorKey"])
            if "industry" in data:
                sets.append("yahoo_industry = ?")
                values.append(data["industry"])
            if "industryKey" in data:
                sets.append("yahoo_industry_key = ?")
                values.append(data["industryKey"])

            # Geolocalização
            if "city" in data:
                sets.append("yahoo_city = ?")
                values.append(data["city"])
            if "country" in data:
                sets.append("yahoo_country = ?")
                values.append(data["country"])
            if "state" in data:
                sets.append("yahoo_state = ?")
                values.append(data["state"])
            if "website" in data:
                sets.append("yahoo_website = ?")
                values.append(data["website"])

            if "enterprise_value" in data or "market_cap" in data:
                sets.append("dta_referencia = ?")
                values.append(dta_referencia)

            if new_yahoo_code:
                sets.append("yahoo_code = ?")
                values.append(new_yahoo_code)

            values.append(row_id)
            cursor.execute(
                f"UPDATE company_basic_data SET {', '.join(sets)} WHERE id = ?",
                values,
            )
            updated += 1
        else:
            failed += 1

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        # Commit intermediário a cada 50 registros para não perder progresso
        if i % 50 == 0:
            _commit_with_retry(conn)

    return updated, failed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Atualiza about, enterprise_value, market_cap e currency via Yahoo Finance."
    )
    parser.add_argument("--db-path", default="data/damodaran_data_new.db")
    parser.add_argument("--limit", type=int, default=500,
                        help="Máximo de registros por execução (padrão: 500)")
    parser.add_argument("--sleep", type=float, default=0.3,
                        help="Pausa entre requisições em segundos (padrão: 0.3)")
    parser.add_argument("--exchanges", type=str, default="",
                        help="Exchanges separadas por vírgula (vazio=todas)")
    parser.add_argument("--force", action="store_true",
                        help="Re-atualiza registros que já possuem dados")
    parser.add_argument("--only-financial", action="store_true",
                        help="Só atualiza enterprise_value/market_cap (ignora about)")
    parser.add_argument("--no-search-fallback", action="store_true",
                        help="Não usar yf.Search como fallback")
    parser.add_argument("--dta-referencia", type=str, default="",
                        help="Data de referência (YYYY-MM-DD). Padrão: data de hoje.")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    dta_ref = args.dta_referencia if args.dta_referencia else date.today().isoformat()

    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        exchanges = [x.strip() for x in args.exchanges.split(",") if x.strip()] if args.exchanges else None

        rows = fetch_candidates(conn, args.limit, exchanges, args.force, args.only_financial)
        if not rows:
            print("Nenhum registro elegível para atualização.")
            return

        print(f"Registros a processar: {len(rows)}")
        if exchanges:
            print(f"Exchanges: {', '.join(exchanges)}")
        print(f"Data de referência: {dta_ref}")
        print()

        updated, failed = update_records(
            conn, rows, args.sleep,
            use_search_fallback=not args.no_search_fallback,
            dta_referencia=dta_ref,
        )
        _commit_with_retry(conn)

        print()
        print(f"Atualizados: {updated}")
        print(f"Sem dados no Yahoo: {failed}")

        # Resumo geral
        stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN about IS NOT NULL AND TRIM(about) != '' THEN 1 ELSE 0 END) as com_about,
                SUM(CASE WHEN enterprise_value IS NOT NULL THEN 1 ELSE 0 END) as com_ev,
                SUM(CASE WHEN market_cap IS NOT NULL THEN 1 ELSE 0 END) as com_mcap
            FROM company_basic_data
        """).fetchone()
        print()
        print(f"--- Resumo do banco ---")
        print(f"Total: {stats[0]}")
        print(f"Com about: {stats[1]}")
        print(f"Com enterprise_value: {stats[2]}")
        print(f"Com market_cap: {stats[3]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
