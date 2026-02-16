import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.yahoo_code_normalizer import normalize_yahoo_code


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Normaliza yahoo_code em company_basic_data usando ticker/exchange."
    )
    parser.add_argument(
        "--db-path",
        default="data/damodaran_data_new.db",
        help="Caminho do banco SQLite (padrão: data/damodaran_data_new.db)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limite de registros para processar (0 = todos).",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        sql = "SELECT id, yahoo_code, ticker FROM company_basic_data ORDER BY id"
        if args.limit and args.limit > 0:
            sql += f" LIMIT {int(args.limit)}"
        cur.execute(sql)
        rows = cur.fetchall()

        changed = 0
        for row_id, yahoo_code, ticker in rows:
            normalized = normalize_yahoo_code(yahoo_code, ticker)
            if normalized and normalized != (yahoo_code or ""):
                cur.execute(
                    """
                    UPDATE company_basic_data
                    SET yahoo_code = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (normalized, row_id),
                )
                if cur.rowcount > 0:
                    changed += 1

        conn.commit()
        print(f"✅ Registros analisados: {len(rows)}")
        print(f"🔧 yahoo_code normalizados: {changed}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
