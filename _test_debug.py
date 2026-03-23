import sqlite3, json

conn = sqlite3.connect('data/damodaran_data_new.db')

# Check columns
cols = conn.execute('PRAGMA table_info(company_financials_historical)').fetchall()
print('CFH columns:', [c[1] for c in cols])

cols2 = conn.execute('PRAGMA table_info(company_basic_data)').fetchall()
print('CBD columns:', [c[1] for c in cols2])

cols3 = conn.execute('PRAGMA table_info(damodaran_global)').fetchall()
print('DG columns:', [c[1] for c in cols3])

# Try the query
try:
    query = """
        SELECT cbd.company_id, cbd.ticker, cbd.company_name, cbd.yahoo_sector,
               cbd.yahoo_industry, cbd.country, cbd.exchange_currency,
               COALESCE(dg.sub_group, 'Other') as region,
               cbd.enterprise_value as ev,
               cfh.revenue, cfh.ebitda, cfh.free_cash_flow as fcf,
               cfh.net_income, cfh.total_debt, cfh.cash_and_equivalents as cash,
               cfh.fiscal_year, cfh.period_type
        FROM company_basic_data cbd
        LEFT JOIN company_financials_historical cfh ON cbd.company_id = cfh.company_id
        LEFT JOIN damodaran_global dg ON LOWER(cbd.ticker) = LOWER(dg.ticker)
        WHERE cbd.yahoo_sector = ?
          AND cbd.enterprise_value IS NOT NULL
          AND cbd.enterprise_value > 0
          AND cfh.revenue IS NOT NULL
          AND cfh.revenue > 0
          AND (cfh.period_type = 'TTM' OR (cfh.period_type = 'Annual' AND cfh.fiscal_year = ?))
    """
    cursor = conn.execute(query, ['Technology', 2025])
    rows = cursor.fetchall()
    print(f'\nQuery returned {len(rows)} rows')
    if rows:
        print('First row:', rows[0])
except Exception as e:
    print(f'QUERY ERROR: {e}')

conn.close()
