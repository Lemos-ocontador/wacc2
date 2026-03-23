import sqlite3, pandas as pd, numpy as np

conn = sqlite3.connect('data/damodaran_data_new.db')

# Test the corrected query
query = """
    SELECT cbd.id as company_id, cbd.ticker, cbd.company_name, cbd.yahoo_sector,
           cbd.yahoo_industry, cbd.yahoo_country as country,
           COALESCE(dg.sub_group, 'Other') as region,
           cbd.enterprise_value as ev,
           cfh.total_revenue as revenue, cfh.normalized_ebitda as ebitda,
           cfh.free_cash_flow as fcf,
           cfh.net_income, cfh.total_debt, cfh.cash_and_equivalents as cash,
           cfh.fiscal_year, cfh.period_type
    FROM company_basic_data cbd
    JOIN company_financials_historical cfh ON cfh.company_basic_data_id = cbd.id
    LEFT JOIN damodaran_global dg ON cbd.damodaran_company_id = dg.id
    WHERE cbd.yahoo_sector = ?
      AND cbd.enterprise_value IS NOT NULL
      AND cbd.enterprise_value > 0
      AND cfh.total_revenue IS NOT NULL
      AND cfh.total_revenue > 0
      AND (cfh.period_type = 'annual' OR (cfh.period_type = 'annual' AND cfh.fiscal_year = ?))
"""

# Get a sector to test
sectors = pd.read_sql("SELECT DISTINCT yahoo_sector FROM company_basic_data WHERE yahoo_sector IS NOT NULL LIMIT 5", conn)
print("Sectors:", sectors['yahoo_sector'].tolist())

sector = sectors['yahoo_sector'].iloc[0]
print(f"\nTesting with sector: {sector}")

df = pd.read_sql_query(query, conn, params=[sector, 2025])
print(f"Rows: {len(df)}")
if not df.empty:
    print(f"Columns: {list(df.columns)}")
    print(f"Sample:\n{df.head(2).to_string()}")
    
    # Test multiples calculation
    df['ev_ebitda'] = np.where((df['ebitda'] > 0) & (df['ev'] > 0), df['ev'] / df['ebitda'], np.nan)
    df['ev_revenue'] = np.where(df['revenue'] > 0, df['ev'] / df['revenue'], np.nan)
    valid_ev = df['ev_ebitda'].dropna()
    print(f"\nEV/EBITDA válidos: {len(valid_ev)}, mediana: {np.median(valid_ev):.2f}")
    print(f"Industries: {df['yahoo_industry'].nunique()}")
    print(f"Countries: {df['country'].nunique()}")

conn.close()
print("\nSUCCESS!")
