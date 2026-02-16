from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path


def create_new_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS company_basic_data_new")
    conn.execute(
        """
        CREATE TABLE company_basic_data_new (
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


def populate_new_table(conn: sqlite3.Connection) -> int:
    conn.execute(
        """
        INSERT INTO company_basic_data_new (
            damodaran_company_id,
            company_name,
            ticker,
            industry,
            country,
            cod_anloc,
            yahoo_code,
            about,
            etf_sector
        )
        SELECT
            dg.id,
            dg.company_name,
            dg.ticker,
            dg.industry,
            dg.country,
            cbd.cod_anloc,
            cbd.yahoo_code,
            cbd.about,
            COALESCE(cbd.etf_sector, '[]')
        FROM damodaran_global dg
        LEFT JOIN company_basic_data cbd
            ON cbd.ticker = dg.ticker
        WHERE dg.company_name IS NOT NULL AND TRIM(dg.company_name) != ''
        """
    )
    count = conn.execute("SELECT COUNT(*) FROM company_basic_data_new").fetchone()[0]
    return int(count)


def recreate_indexes(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_company_basic_data_ticker ON company_basic_data(ticker)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_company_basic_data_yahoo_code ON company_basic_data(yahoo_code)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_company_basic_data_cod_anloc ON company_basic_data(cod_anloc)"
    )


def swap_tables(conn: sqlite3.Connection) -> str:
    backup_table = f"company_basic_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute(f"ALTER TABLE company_basic_data RENAME TO {backup_table}")
    cur.execute("ALTER TABLE company_basic_data_new RENAME TO company_basic_data")
    conn.commit()
    recreate_indexes(conn)
    conn.commit()
    return backup_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Sincroniza company_basic_data com damodaran_global.")
    parser.add_argument("--db-path", default="data/damodaran_data_new.db")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        old_count = conn.execute("SELECT COUNT(*) FROM company_basic_data").fetchone()[0]
        create_new_table(conn)
        new_count = populate_new_table(conn)
        conn.commit()

        print(f"📊 company_basic_data atual: {old_count}")
        print(f"📊 company_basic_data_new: {new_count}")

        if args.dry_run:
            conn.execute("DROP TABLE IF EXISTS company_basic_data_new")
            conn.commit()
            print("🛡️ Dry-run: nenhuma troca de tabela foi executada.")
            return

        backup_table = swap_tables(conn)
        print(f"✅ Sync concluído. Backup lógico: {backup_table}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
