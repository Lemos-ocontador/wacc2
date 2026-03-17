#!/usr/bin/env python3
"""Extrai holdings dos 57 ETFs sem dados usando os novos providers."""
import sqlite3
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from data_extractors.etf_extractor import ETFExtractor

DB = "data/damodaran_data_new.db"

def get_etfs_without_holdings():
    with sqlite3.connect(DB) as conn:
        rows = conn.execute("""
            SELECT e.ticker, e.name
            FROM etfs e
            LEFT JOIN etf_holdings h ON e.ticker = h.etf_ticker
            GROUP BY e.ticker
            HAVING COUNT(h.id) = 0
            ORDER BY e.ticker
        """).fetchall()
    return rows

def main():
    missing = get_etfs_without_holdings()
    print(f"ETFs sem holdings: {len(missing)}")

    ext = ETFExtractor(db_path=DB, use_sec=False, use_cvm=False)

    success = 0
    failed = []

    for i, (ticker, name) in enumerate(missing, 1):
        print(f"\n[{i}/{len(missing)}] {ticker}: {name or '?'}")

        try:
            holdings, source = ext.get_holdings_with_fallback(ticker)
            if holdings:
                count = ext.save_holdings(ticker, holdings, source=source)
                print(f"  ✓ {count} holdings salvos (fonte: {source})")
                success += 1
            else:
                print(f"  ✗ Nenhum holding encontrado")
                failed.append(ticker)
        except Exception as e:
            print(f"  ✗ Erro: {e}")
            failed.append(ticker)

        # Rate limit entre requests
        time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"Resultado: {success}/{len(missing)} ETFs com holdings extraídos")
    if failed:
        print(f"Falharam ({len(failed)}): {', '.join(failed)}")

    # Verificação final
    remaining = get_etfs_without_holdings()
    print(f"\nETFs ainda sem holdings: {len(remaining)}")

if __name__ == "__main__":
    main()
