import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.yahoo_code_normalizer import normalize_yahoo_code


def derive_yahoo_code(raw_ticker: str | None) -> str | None:
    return normalize_yahoo_code(None, raw_ticker)


def create_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_basic_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            damodaran_company_id INTEGER,
            company_name TEXT NOT NULL,
            ticker TEXT,
            industry TEXT,
            country TEXT,
            cod_anloc TEXT UNIQUE,
            yahoo_code TEXT,
            about TEXT,
            etf_sector TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (damodaran_company_id) REFERENCES damodaran_global(id)
        )
        """
    )

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_company_basic_data_ticker ON company_basic_data(ticker)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_company_basic_data_yahoo_code ON company_basic_data(yahoo_code)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_company_basic_data_cod_anloc ON company_basic_data(cod_anloc)"
    )


def seed_from_damodaran(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, company_name, ticker, industry, country
        FROM damodaran_global
        WHERE company_name IS NOT NULL AND TRIM(company_name) != ''
        ORDER BY market_cap DESC
        """
    )
    rows = cursor.fetchall()

    inserted = 0
    for damodaran_id, company_name, ticker, industry, country in rows:
        ticker_norm = ticker.strip() if isinstance(ticker, str) else None
        yahoo_code = derive_yahoo_code(ticker_norm)
        etf_sector = json.dumps([], ensure_ascii=False)

        cursor.execute(
            """
            INSERT OR IGNORE INTO company_basic_data (
                damodaran_company_id,
                company_name,
                ticker,
                industry,
                country,
                yahoo_code,
                etf_sector
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                damodaran_id,
                company_name,
                ticker_norm,
                industry,
                country,
                yahoo_code,
                etf_sector,
            ),
        )

        if cursor.rowcount > 0:
            inserted += 1

    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cria e inicializa a tabela company_basic_data no SQLite."
    )
    parser.add_argument(
        "--db-path",
        default="data/damodaran_data_new.db",
        help="Caminho do banco SQLite (padrão: data/damodaran_data_new.db)",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Cria somente a estrutura da tabela, sem carga inicial.",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        create_table(conn)
        inserted = 0
        if not args.no_seed:
            inserted = seed_from_damodaran(conn)
        conn.commit()

        total = conn.execute("SELECT COUNT(*) FROM company_basic_data").fetchone()[0]
        print(f"✅ Tabela company_basic_data pronta. Registros inseridos nesta execução: {inserted}")
        print(f"📊 Total atual em company_basic_data: {total}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
