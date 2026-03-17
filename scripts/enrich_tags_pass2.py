"""
Segunda passada de enriquecimento:
1. ETFs brasileiros (.SA) → geography=Brazil, asset_class baseado no nome
2. ETFs HK → geography=Hong Kong
3. ETFs .L → geography=UK/International
4. Equity sectors + commodities sem geography → geography=US (maioria são ETFs US)
5. Large/Mid/Small Blend/Value/Growth sem geography → geography=US
"""
import sqlite3

DB = 'data/damodaran_data_new.db'

def enrich_pass2():
    conn = sqlite3.connect(DB)
    
    existing = set()
    for r in conn.execute("SELECT etf_ticker, tag_type, tag_value FROM etf_tags"):
        existing.add((r[0], r[1], r[2]))
    
    print(f"Tags antes: {len(existing)}")
    new_tags = []
    
    def add(ticker, tag_type, tag_value):
        key = (ticker, tag_type, tag_value)
        if key not in existing:
            new_tags.append(key)
            existing.add(key)
    
    # ── ETFs brasileiros (.SA) ──
    br_etfs = conn.execute("SELECT ticker, name FROM etfs WHERE ticker LIKE '%.SA'").fetchall()
    for ticker, name in br_etfs:
        add(ticker, 'geography', 'Brazil')
        name_upper = (name or '').upper()
        
        # Asset class por nome
        if any(x in name_upper for x in ['BITCOIN', 'ETHER', 'HASH', 'CRYPTO', 'NCI']):
            add(ticker, 'asset_class', 'Crypto')
        elif any(x in name_upper for x in ['OURO', 'GOLD']):
            add(ticker, 'asset_class', 'Commodity')
        elif any(x in name_upper for x in ['FIXA', 'B5P2', 'IMA', 'IRF', 'BOND']):
            add(ticker, 'asset_class', 'Fixed Income')
        elif any(x in name_upper for x in ['IBOV', 'BOVA', 'BOVB', 'BRAX', 'IDIV', 'IBRX', 'SMLL', 'SMALL', 
                                            'IFNC', 'PIBB', 'ECOO', 'TECK', 'HCARE']):
            add(ticker, 'asset_class', 'Equity')
        elif 'NASDAQ' in name_upper or 'NASD' in name_upper or 'SPXI' in name_upper or 'S&P' in name_upper:
            add(ticker, 'asset_class', 'Equity')
            add(ticker, 'geography', 'US')  # Esses replicam índices US
        elif 'CHINA' in name_upper or 'XINA' in name_upper:
            add(ticker, 'asset_class', 'Equity')
            add(ticker, 'geography', 'China')
        elif 'ACWI' in name_upper:
            add(ticker, 'asset_class', 'Equity')
            add(ticker, 'geography', 'Global')
        elif 'MBA' in ticker:
            add(ticker, 'asset_class', 'Equity')
        
        # Strategy
        add(ticker, 'strategy', 'Passive')
    
    # ── HK ETFs ──
    hk_etfs = conn.execute("SELECT ticker FROM etfs WHERE ticker LIKE '%.HK'").fetchall()
    for (ticker,) in hk_etfs:
        add(ticker, 'geography', 'Hong Kong')
        add(ticker, 'asset_class', 'Equity')
        add(ticker, 'strategy', 'Passive')
    
    # ── .L ETFs ──
    l_etfs = conn.execute("SELECT ticker FROM etfs WHERE ticker LIKE '%.L'").fetchall()
    for (ticker,) in l_etfs:
        add(ticker, 'strategy', 'Passive')
    
    # ── US Equity ETFs sem geography (ticker simples, sem sufixo) ──
    us_equity_cats = [
        'Large Blend', 'Large Value', 'Large Growth',
        'Mid-Cap Blend', 'Mid-Cap Value', 'Mid-Cap Growth', 
        'Small Blend', 'Small Value', 'Small Growth',
        'Technology', 'Health', 'Financial', 'Industrials', 
        'Communications', 'Consumer Cyclical', 'Consumer Defensive',
        'Utilities', 'Real Estate', 'Equity Energy', 'Equity Precious Metals',
        'Natural Resources', 'Infrastructure', 'Miscellaneous Sector',
        'Digital Assets', 'Trading--Leveraged Equity', 'Trading--Inverse Equity',
    ]
    rows = conn.execute("""
        SELECT e.ticker, e.category FROM etfs e 
        WHERE e.ticker NOT IN (SELECT etf_ticker FROM etf_tags WHERE tag_type='geography')
        AND e.category IS NOT NULL
        AND e.ticker NOT LIKE '%.SA' AND e.ticker NOT LIKE '%.HK' AND e.ticker NOT LIKE '%.L'
        AND e.ticker NOT LIKE '%.TO' AND e.ticker NOT LIKE '%.AX'
    """).fetchall()
    for ticker, category in rows:
        if category in us_equity_cats:
            add(ticker, 'geography', 'US')
    
    # ── Commodities sem geography → assume US-listed ──
    rows = conn.execute("""
        SELECT e.ticker FROM etfs e 
        WHERE e.ticker NOT IN (SELECT etf_ticker FROM etf_tags WHERE tag_type='geography')
        AND e.category IN ('Commodities Focused', 'Commodities Broad Basket')
        AND e.ticker NOT LIKE '%.SA' AND e.ticker NOT LIKE '%.HK'
    """).fetchall()
    for (ticker,) in rows:
        add(ticker, 'geography', 'US')
    
    print(f"Novas tags: {len(new_tags)}")
    
    if new_tags:
        conn.executemany(
            "INSERT INTO etf_tags (etf_ticker, tag_type, tag_value) VALUES (?, ?, ?)",
            new_tags
        )
        conn.commit()
        print("Inseridas!")
    
    # Report
    print("\n=== COBERTURA FINAL ===")
    total = conn.execute("SELECT COUNT(*) FROM etfs").fetchone()[0]
    for tt in ['asset_class','geography','cap_size','style','sector','strategy','issuer','index']:
        tagged = conn.execute("SELECT COUNT(DISTINCT etf_ticker) FROM etf_tags WHERE tag_type=?", [tt]).fetchone()[0]
        pct = tagged*100//total
        print(f"  {tt}: {tagged}/{total} ({pct}%)")
    
    total_tags = conn.execute("SELECT COUNT(*) FROM etf_tags").fetchone()[0]
    print(f"\nTotal tags: {total_tags}")
    
    # Remaining gaps
    gaps = conn.execute("""SELECT COUNT(*) FROM etfs 
        WHERE ticker NOT IN (SELECT etf_ticker FROM etf_tags WHERE tag_type='asset_class')""").fetchone()[0]
    print(f"Ainda sem asset_class: {gaps}")
    gaps = conn.execute("""SELECT COUNT(*) FROM etfs 
        WHERE ticker NOT IN (SELECT etf_ticker FROM etf_tags WHERE tag_type='geography')""").fetchone()[0]
    print(f"Ainda sem geography: {gaps}")
    
    conn.close()

if __name__ == '__main__':
    enrich_pass2()
