"""
Migração: adiciona colunas de dados financeiros em company_basic_data.

Novas colunas:
  - enterprise_value  (REAL)   — Enterprise Value do Yahoo Finance
  - market_cap        (REAL)   — Market Cap do Yahoo Finance
  - currency          (TEXT)   — Moeda dos valores (USD, BRL, INR, etc.)
  - dta_referencia    (TEXT)   — Data-base dos dados financeiros (YYYY-MM-DD)
"""
import argparse
import sqlite3
from pathlib import Path


COLUMNS = [
    ("enterprise_value", "REAL"),
    ("market_cap", "REAL"),
    ("currency", "TEXT"),
    ("dta_referencia", "TEXT"),
]


def migrate(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    existing = {row[1] for row in cursor.execute("PRAGMA table_info(company_basic_data)").fetchall()}

    added = []
    for col_name, col_type in COLUMNS:
        if col_name not in existing:
            cursor.execute(f"ALTER TABLE company_basic_data ADD COLUMN {col_name} {col_type}")
            added.append(col_name)

    conn.commit()
    conn.close()

    if added:
        print(f"Colunas adicionadas: {', '.join(added)}")
    else:
        print("Todas as colunas já existem. Nada a fazer.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Adiciona colunas financeiras em company_basic_data.")
    parser.add_argument("--db-path", default="data/damodaran_data_new.db", help="Caminho do banco SQLite")
    args = parser.parse_args()

    if not Path(args.db_path).exists():
        raise FileNotFoundError(f"Banco não encontrado: {args.db_path}")

    migrate(args.db_path)


if __name__ == "__main__":
    main()
