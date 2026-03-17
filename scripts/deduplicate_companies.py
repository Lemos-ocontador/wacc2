"""
Deduplicação de company_basic_data por yahoo_code.

Quando múltiplos registros em company_basic_data compartilham o mesmo yahoo_code,
mantém o que tem mais dados financeiros históricos e remove os demais,
migrando registros financeiros soltos para o registro mantido.
"""

import argparse
import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "damodaran_data_new.db"


def find_duplicates(conn):
    """Encontra yahoo_codes com múltiplos registros em company_basic_data."""
    rows = conn.execute("""
        SELECT yahoo_code, COUNT(*) as cnt
        FROM company_basic_data
        WHERE yahoo_code IS NOT NULL AND yahoo_code != ''
        GROUP BY yahoo_code
        HAVING cnt > 1
        ORDER BY cnt DESC
    """).fetchall()
    return rows


def deduplicate(conn, dry_run=False):
    """Remove duplicatas, mantendo o registro com mais dados financeiros."""
    dups = find_duplicates(conn)
    log.info(f"Yahoo codes duplicados encontrados: {len(dups)}")

    total_removed = 0
    total_migrated = 0
    total_fin_deleted = 0

    for yahoo_code, cnt in dups:
        # Buscar todos os IDs para este yahoo_code, com contagem de financials
        entries = conn.execute("""
            SELECT cbd.id, cbd.company_name, cbd.yahoo_sector,
                   COUNT(cfh.id) as fin_count
            FROM company_basic_data cbd
            LEFT JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id
            WHERE cbd.yahoo_code = ?
            GROUP BY cbd.id
            ORDER BY fin_count DESC, cbd.id ASC
        """, (yahoo_code,)).fetchall()

        # Manter o primeiro (mais dados financeiros, ou menor ID em caso de empate)
        keep_id = entries[0][0]
        remove_ids = [e[0] for e in entries[1:]]

        for rid in remove_ids:
            # Contar financials do registro a ser removido
            fin_count = conn.execute(
                "SELECT COUNT(*) FROM company_financials_historical WHERE company_basic_data_id = ?",
                (rid,)
            ).fetchone()[0]

            if fin_count > 0:
                # Tentar migrar registros que não conflitam (UNIQUE constraint)
                migrated = 0
                fin_rows = conn.execute(
                    "SELECT id, period_type, period_date FROM company_financials_historical WHERE company_basic_data_id = ?",
                    (rid,)
                ).fetchall()

                for fid, ptype, pdate in fin_rows:
                    # Verificar se já existe no registro mantido
                    exists = conn.execute(
                        """SELECT 1 FROM company_financials_historical
                           WHERE company_basic_data_id = ? AND period_type = ? AND period_date = ?""",
                        (keep_id, ptype, pdate)
                    ).fetchone()

                    if not exists:
                        if not dry_run:
                            conn.execute(
                                "UPDATE company_financials_historical SET company_basic_data_id = ? WHERE id = ?",
                                (keep_id, fid)
                            )
                        migrated += 1
                    else:
                        # Duplicata real - deletar
                        if not dry_run:
                            conn.execute(
                                "DELETE FROM company_financials_historical WHERE id = ?",
                                (fid,)
                            )
                        total_fin_deleted += 1

                total_migrated += migrated

            # Remover registro da company_basic_data
            if not dry_run:
                conn.execute("DELETE FROM company_basic_data WHERE id = ?", (rid,))
            total_removed += 1

        if cnt > 5:
            log.info(f"  {yahoo_code}: mantido id={keep_id}, removidos {len(remove_ids)} registros")

    if not dry_run:
        conn.commit()

    return total_removed, total_migrated, total_fin_deleted


def main():
    parser = argparse.ArgumentParser(description="Deduplica company_basic_data por yahoo_code")
    parser.add_argument("--dry-run", action="store_true", help="Apenas simular, não alterar dados")
    parser.add_argument("--sector", help="Filtrar por setor (ex: Utilities)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    log.info(f"DB: {DB_PATH}")

    # Status inicial
    dups = find_duplicates(conn)
    log.info(f"Total yahoo_codes duplicados: {len(dups)}")
    top5 = dups[:5]
    for code, cnt in top5:
        log.info(f"  {code}: {cnt} registros")

    if args.dry_run:
        log.info("=== DRY RUN - nenhuma alteração será feita ===")

    removed, migrated, fin_deleted = deduplicate(conn, dry_run=args.dry_run)

    log.info(f"Resultado:")
    log.info(f"  Registros company_basic_data removidos: {removed}")
    log.info(f"  Registros financeiros migrados: {migrated}")
    log.info(f"  Registros financeiros duplicados deletados: {fin_deleted}")

    # Status final
    remaining = find_duplicates(conn)
    log.info(f"  Duplicatas restantes: {len(remaining)}")

    conn.close()


if __name__ == "__main__":
    main()
