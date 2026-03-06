"""
Migração: adiciona colunas de classificação e geolocalização em company_basic_data.

Novas colunas:
  - yahoo_sector       (TEXT) — Setor do Yahoo Finance (ex: Technology)
  - yahoo_sector_key   (TEXT) — Chave do setor (ex: technology)
  - yahoo_industry     (TEXT) — Indústria do Yahoo Finance (ex: Consumer Electronics)
  - yahoo_industry_key (TEXT) — Chave da indústria (ex: consumer-electronics)
  - yahoo_city         (TEXT) — Cidade da sede
  - yahoo_country      (TEXT) — País da sede
  - yahoo_state        (TEXT) — Estado/província da sede
  - yahoo_website      (TEXT) — Website da empresa
"""
import sqlite3
from pathlib import Path


COLUMNS = [
    ("yahoo_sector", "TEXT"),
    ("yahoo_sector_key", "TEXT"),
    ("yahoo_industry", "TEXT"),
    ("yahoo_industry_key", "TEXT"),
    ("yahoo_city", "TEXT"),
    ("yahoo_country", "TEXT"),
    ("yahoo_state", "TEXT"),
    ("yahoo_website", "TEXT"),
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
        print("Todas as colunas já existiam.")


if __name__ == "__main__":
    db_path = "data/damodaran_data_new.db"
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")
    migrate(db_path)
