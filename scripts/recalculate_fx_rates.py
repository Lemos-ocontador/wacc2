"""
Recalcula taxas de câmbio históricas e campos USD no banco de dados.

Não precisa buscar dados do Yahoo Finance novamente — apenas atualiza
fx_rate_to_usd e os campos *_usd usando taxas históricas por período.

Uso:
    python scripts/recalculate_fx_rates.py [--dry-run] [--sector "Energy"]
"""

import sqlite3
import logging
import argparse
import time
from pathlib import Path

import yfinance as yf
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "damodaran_data_new.db"

# Cache de séries FX
_fx_cache: dict[str, pd.DataFrame] = {}


def _get_fx_series(currency: str) -> pd.DataFrame:
    """Busca série histórica FX para USD via yfinance."""
    if currency in _fx_cache:
        return _fx_cache[currency]
    try:
        pair = f"{currency}USD=X"
        log.info(f"Buscando série FX: {pair}")
        ticker = yf.Ticker(pair)
        hist = ticker.history(period="10y", interval="1d")
        if hist.empty:
            log.warning(f"Sem cotação histórica para {pair}")
            hist = pd.DataFrame()
    except Exception as e:
        log.warning(f"Erro buscando FX {currency}: {e}")
        hist = pd.DataFrame()
    _fx_cache[currency] = hist
    return hist


def _get_fx_rate_for_date(fx_hist: pd.DataFrame, date_str: str) -> float:
    """Retorna taxa FX mais próxima da data informada."""
    if fx_hist.empty:
        return 1.0
    tz = fx_hist.index.tz
    pd_date = pd.Timestamp(date_str)
    if tz is not None:
        pd_date = pd_date.tz_localize(tz)
    closest_idx = fx_hist.index.get_indexer([pd_date], method="nearest")[0]
    if 0 <= closest_idx < len(fx_hist):
        return float(fx_hist.iloc[closest_idx]["Close"])
    return 1.0


def main():
    parser = argparse.ArgumentParser(description="Recalcula FX rates históricas")
    parser.add_argument("--dry-run", action="store_true", help="Apenas mostra o que seria feito")
    parser.add_argument("--sector", type=str, help="Filtrar por setor")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Buscar moedas únicas que precisam de conversão
    sector_filter = ""
    params = []
    if args.sector:
        sector_filter = """
            AND h.yahoo_code IN (
                SELECT yahoo_code FROM company_basic_data WHERE yahoo_sector = ?
            )
        """
        params.append(args.sector)

    cur = conn.execute(f"""
        SELECT DISTINCT original_currency
        FROM company_financials_historical h
        WHERE original_currency IS NOT NULL
          AND original_currency != 'USD'
          {sector_filter}
    """, params)

    currencies = [r[0] for r in cur.fetchall()]
    log.info(f"Moedas a processar: {len(currencies)} ({', '.join(currencies[:10])}{'...' if len(currencies) > 10 else ''})")

    # Pré-carregar séries FX para todas as moedas
    for currency in currencies:
        _get_fx_series(currency)
        time.sleep(0.5)  # Respeitar rate limit

    # Buscar registros que precisam de atualização
    cur = conn.execute(f"""
        SELECT id, original_currency, period_date,
               total_revenue, ebit, ebitda, net_income,
               free_cash_flow, enterprise_value_estimated,
               fx_rate_to_usd
        FROM company_financials_historical h
        WHERE original_currency IS NOT NULL
          AND original_currency != 'USD'
          {sector_filter}
    """, params)

    rows = cur.fetchall()
    total = len(rows)
    log.info(f"Registros para atualizar: {total}")

    if args.dry_run:
        # Mostrar amostra
        sample = rows[:5]
        for r in sample:
            fx_hist = _fx_cache.get(r["original_currency"], pd.DataFrame())
            new_rate = _get_fx_rate_for_date(fx_hist, r["period_date"])
            old_rate = r["fx_rate_to_usd"] or 1.0
            log.info(f"  {r['original_currency']} {r['period_date']}: "
                     f"taxa antiga={old_rate:.4f} -> nova={new_rate:.4f} "
                     f"(delta={((new_rate/old_rate)-1)*100:+.1f}%)")
        log.info("Dry run - nenhuma alteração feita.")
        conn.close()
        return

    # Atualizar em batch
    update_sql = """
        UPDATE company_financials_historical
        SET fx_rate_to_usd = ?,
            total_revenue_usd = total_revenue * ?,
            ebit_usd = ebit * ?,
            ebitda_usd = ebitda * ?,
            net_income_usd = net_income * ?,
            free_cash_flow_usd = free_cash_flow * ?,
            enterprise_value_usd = enterprise_value_estimated * ?
        WHERE id = ?
    """

    updated = 0
    unchanged = 0
    batch = []
    batch_size = 1000

    for i, r in enumerate(rows):
        fx_hist = _fx_cache.get(r["original_currency"], pd.DataFrame())
        new_rate = _get_fx_rate_for_date(fx_hist, r["period_date"])
        old_rate = r["fx_rate_to_usd"] or 1.0

        # Só atualiza se a taxa mudou significativamente (>0.01%)
        if abs(new_rate - old_rate) / max(old_rate, 0.0001) < 0.0001:
            unchanged += 1
            continue

        batch.append((new_rate, new_rate, new_rate, new_rate, new_rate, new_rate, new_rate, r["id"]))
        updated += 1

        if len(batch) >= batch_size:
            conn.executemany(update_sql, batch)
            conn.commit()
            batch = []

        if (i + 1) % 10000 == 0:
            log.info(f"  Progresso: {i+1}/{total} ({(i+1)*100//total}%) | "
                     f"Atualizados: {updated} | Sem mudança: {unchanged}")

    # Flush batch restante
    if batch:
        conn.executemany(update_sql, batch)
        conn.commit()

    log.info("=" * 60)
    log.info(f"CONCLUÍDO")
    log.info(f"  Total registros: {total}")
    log.info(f"  Atualizados: {updated}")
    log.info(f"  Sem mudança: {unchanged}")
    log.info("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
