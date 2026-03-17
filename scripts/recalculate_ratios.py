"""
Recalcula ratios/margens em registros existentes aplicando as novas guardas:
- Receita < $1.000 USD: margins → NULL
- EBITDA < $100 USD: fcf_ebitda_ratio, debt_ebitda → NULL
- Equity < $100 USD: debt_equity → NULL
- MCap/Receita > 50.000x com MCap > 1B: MCap e EV → NULL
- Clamp: margins ±100, EV multiples ±100.000, debt ratios ±100
Após recalcular, re-executa validação de consistência.
"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "damodaran_data_new.db"


def _clamp(numerator, denominator, limit=100):
    if numerator is None or denominator is None or denominator == 0:
        return None
    ratio = numerator / denominator
    if abs(ratio) > limit:
        return None
    return ratio


def recalculate(db_path=DB):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT id, total_revenue, gross_profit, ebit, ebitda, net_income,
               free_cash_flow, capital_expenditure, total_debt, stockholders_equity,
               market_cap_estimated, enterprise_value_estimated,
               fx_rate_to_usd, ordinary_shares_number
        FROM company_financials_historical
    """).fetchall()

    total = len(rows)
    nullified_mcap = 0
    nullified_margin = 0
    nullified_ratio = 0
    updated = 0

    print(f"Processando {total} registros...")

    batch = []
    for i, r in enumerate(rows):
        fx = r["fx_rate_to_usd"] or 1.0
        rev = r["total_revenue"]
        rev_usd = abs(rev * fx) if rev else 0
        rev_ok = rev and rev != 0 and rev_usd >= 1000

        ebitda_val = r["ebitda"]
        ebitda_usd = abs(ebitda_val * fx) if ebitda_val else 0
        ebitda_ok = ebitda_val and ebitda_val != 0 and ebitda_usd >= 100

        equity = r["stockholders_equity"]
        equity_usd = abs(equity * fx) if equity else 0
        equity_ok = equity and equity != 0 and equity_usd >= 100

        # MCap guard
        mcap = r["market_cap_estimated"]
        new_mcap = mcap
        new_ev = r["enterprise_value_estimated"]
        if mcap and rev_usd > 0 and mcap > 1e9 and (mcap / abs(rev)) > 50000:
            new_mcap = None
            new_ev = None
            nullified_mcap += 1

        # Margins
        if rev_ok:
            ebit_margin = _clamp(r["ebit"], rev)
            ebitda_margin = _clamp(r["ebitda"], rev)
            gross_margin = _clamp(r["gross_profit"], rev)
            net_margin = _clamp(r["net_income"], rev)
            fcf_rev = _clamp(r["free_cash_flow"], rev)
            capex_rev = abs(r["capital_expenditure"]) / rev if r["capital_expenditure"] is not None else None
        else:
            ebit_margin = ebitda_margin = gross_margin = net_margin = fcf_rev = capex_rev = None
            if rev is not None and rev != 0:
                nullified_margin += 1

        # Debt / EBITDA, FCF / EBITDA
        if ebitda_ok:
            fcf_ebitda = _clamp(r["free_cash_flow"], ebitda_val)
            debt_ebitda = _clamp(r["total_debt"], ebitda_val)
        else:
            fcf_ebitda = debt_ebitda = None

        # Debt / Equity
        if equity_ok:
            debt_equity = _clamp(r["total_debt"], equity)
        else:
            debt_equity = None
            if equity and equity != 0:
                nullified_ratio += 1

        # EV multiples
        ev = new_ev
        ev_revenue = _clamp(ev, rev, limit=100000) if ev and rev_ok else None
        ev_ebitda = _clamp(ev, ebitda_val, limit=100000) if ev and ebitda_ok else None
        ebit_val = r["ebit"]
        ev_ebit = _clamp(ev, ebit_val, limit=100000) if ev and ebit_val and ebit_val > 0 else None

        # EV USD
        ev_usd = new_ev * fx if new_ev else None

        batch.append((
            new_mcap, new_ev, ev_usd,
            ebit_margin, ebitda_margin, gross_margin, net_margin,
            fcf_rev, capex_rev, fcf_ebitda, debt_ebitda, debt_equity,
            ev_revenue, ev_ebitda, ev_ebit,
            r["id"]
        ))

        if len(batch) >= 5000:
            _flush(cur, batch)
            updated += len(batch)
            batch = []
            print(f"  {updated}/{total} atualizados...")

    if batch:
        _flush(cur, batch)
        updated += len(batch)

    conn.commit()
    conn.close()

    print(f"\nConcluído: {updated} registros atualizados")
    print(f"  MCap/EV nullificados (shares inflados): {nullified_mcap}")
    print(f"  Margins nullificados (receita < $1k): {nullified_margin}")
    print(f"  Ratios nullificados (equity < $100): {nullified_ratio}")

    # Re-executar validação
    print("\nRe-executando validação de consistência...")
    try:
        from scripts.validate_data_consistency import run_validation, update_data_quality
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        results = run_validation(conn)
        conn.close()
        conn2 = sqlite3.connect(str(db_path))
        update_data_quality(conn2, results)
        conn2.close()
        crit = sum(1 for r in results["issues"] if r["severity"] == "critical")
        warn = sum(1 for r in results["issues"] if r["severity"] == "warning")
        print(f"  Validação: {crit} críticos, {warn} warnings")
    except Exception as e:
        print(f"  Erro na validação: {e}")


def _flush(cur, batch):
    cur.executemany("""
        UPDATE company_financials_historical SET
            market_cap_estimated = ?,
            enterprise_value_estimated = ?,
            enterprise_value_usd = ?,
            ebit_margin = ?,
            ebitda_margin = ?,
            gross_margin = ?,
            net_margin = ?,
            fcf_revenue_ratio = ?,
            capex_revenue = ?,
            fcf_ebitda_ratio = ?,
            debt_ebitda = ?,
            debt_equity = ?,
            ev_revenue = ?,
            ev_ebitda = ?,
            ev_ebit = ?
        WHERE id = ?
    """, batch)


if __name__ == "__main__":
    recalculate()
