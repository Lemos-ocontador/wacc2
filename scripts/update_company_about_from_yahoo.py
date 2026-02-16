import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path

import yfinance as yf

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.yahoo_code_normalizer import generate_yahoo_code_candidates, normalize_yahoo_code


def get_about_from_yahoo(yahoo_code: str) -> str | None:
    ticker = yf.Ticker(yahoo_code)

    try:
        info = ticker.get_info()
    except Exception:
        info = {}

    about = info.get("longBusinessSummary") or info.get("description")
    if about:
        about = str(about).strip()
        return about if about else None

    return None


def get_about_from_candidates(yahoo_code: str | None, ticker: str | None) -> tuple[str | None, str | None]:
    candidates = generate_yahoo_code_candidates(yahoo_code, ticker)
    for candidate in candidates:
        about = get_about_from_yahoo(candidate)
        if about:
            return candidate, about
    return None, None


def search_yahoo_symbols_by_name(company_name: str | None, limit: int = 5) -> list[str]:
    if not company_name:
        return []
    try:
        search = yf.Search(company_name, max_results=limit)
        quotes = getattr(search, "quotes", []) or []
        symbols: list[str] = []
        for item in quotes:
            symbol = item.get("symbol") if isinstance(item, dict) else None
            if symbol and symbol not in symbols:
                symbols.append(symbol)
        return symbols
    except Exception:
        return []


def fetch_candidates(
    conn: sqlite3.Connection,
    limit: int | None,
    force: bool,
    exchanges: list[str] | None,
    include_missing_yahoo: bool,
    only_missing_yahoo: bool,
) -> list[tuple[int, str | None, str | None, str | None]]:
    cursor = conn.cursor()

    where_clause = "WHERE 1=1"
    if only_missing_yahoo:
        where_clause += " AND (yahoo_code IS NULL OR TRIM(yahoo_code) = '')"
    elif not include_missing_yahoo:
        where_clause += " AND yahoo_code IS NOT NULL AND TRIM(yahoo_code) != ''"
    else:
        where_clause += " AND (ticker IS NOT NULL OR company_name IS NOT NULL)"
    if not force:
        where_clause += " AND (about IS NULL OR TRIM(about) = '')"

    params: list[str] = []
    if exchanges:
        normalized = [ex.strip().upper() for ex in exchanges if ex and ex.strip()]
        if normalized:
            placeholders = ",".join(["?" for _ in normalized])
            where_clause += (
                " AND UPPER(CASE "
                "WHEN INSTR(ticker, ':') > 0 THEN SUBSTR(ticker, 1, INSTR(ticker, ':') - 1) "
                "ELSE '' END) IN (" + placeholders + ")"
            )
            params.extend(normalized)

    sql = f"""
        SELECT id, yahoo_code, ticker, company_name
        FROM company_basic_data
        {where_clause}
        ORDER BY id
    """

    if limit and limit > 0:
        sql += f" LIMIT {int(limit)}"

    cursor.execute(sql, params)
    return cursor.fetchall()


def update_about(
    conn: sqlite3.Connection,
    rows: list[tuple[int, str | None, str | None, str | None]],
    sleep_seconds: float,
    use_name_search: bool,
) -> tuple[int, int]:
    cursor = conn.cursor()
    updated = 0
    failed = 0

    for row_id, yahoo_code, ticker, company_name in rows:
        try:
            normalized = normalize_yahoo_code(yahoo_code, ticker)
            used_code, about = get_about_from_candidates(normalized or yahoo_code, ticker)

            if (not about) and use_name_search:
                for candidate in search_yahoo_symbols_by_name(company_name, limit=5):
                    about_alt = get_about_from_yahoo(candidate)
                    if about_alt:
                        used_code, about = candidate, about_alt
                        break

            if about:
                cursor.execute(
                    """
                    UPDATE company_basic_data
                    SET about = ?,
                        yahoo_code = COALESCE(?, yahoo_code),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (about, used_code, row_id),
                )
                if cursor.rowcount > 0:
                    updated += 1
            else:
                failed += 1
        except Exception:
            failed += 1

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return updated, failed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Atualiza campo about em company_basic_data usando Yahoo Finance (yfinance)."
    )
    parser.add_argument(
        "--db-path",
        default="data/damodaran_data_new.db",
        help="Caminho do banco SQLite (padrão: data/damodaran_data_new.db)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Máximo de registros a processar por execução (padrão: 100)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Atualiza também registros que já possuem about.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.15,
        help="Pausa entre requisições em segundos (padrão: 0.15)",
    )
    parser.add_argument(
        "--exchanges",
        type=str,
        default="",
        help="Lista de bolsas separadas por vírgula (ex.: BSE,NSEI,SHSE,SZSE).",
    )
    parser.add_argument(
        "--no-name-search",
        action="store_true",
        help="Desabilita fallback por busca de nome da empresa no Yahoo.",
    )
    parser.add_argument(
        "--include-missing-yahoo",
        action="store_true",
        help="Inclui registros sem yahoo_code para tentar busca por ticker/nome.",
    )
    parser.add_argument(
        "--only-missing-yahoo",
        action="store_true",
        help="Processa apenas registros sem yahoo_code (ignora os que ja possuem).",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        exchanges = [x.strip() for x in args.exchanges.split(",")] if args.exchanges else []
        rows = fetch_candidates(
            conn,
            args.limit,
            args.force,
            exchanges,
            args.include_missing_yahoo,
            args.only_missing_yahoo,
        )
        if not rows:
            print("INFO: Nenhum registro elegível para atualização.")
            return

        print(f"Registros elegíveis: {len(rows)}")
        if exchanges:
            print(f"Exchanges alvo: {', '.join(exchanges)}")
        updated, failed = update_about(conn, rows, args.sleep, use_name_search=not args.no_name_search)
        conn.commit()

        print(f"Atualizados com sucesso: {updated}")
        print(f"Sem descrição/erro: {failed}")

        total_with_about = conn.execute(
            "SELECT COUNT(*) FROM company_basic_data WHERE about IS NOT NULL AND TRIM(about) != ''"
        ).fetchone()[0]
        print(f"Total com about preenchido: {total_with_about}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
