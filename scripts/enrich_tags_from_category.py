"""
Enriquecimento de tags ETF baseado na coluna 'category' do yfinance.
Mapeia categorias Morningstar → tag_type/tag_value sem nenhum scraping.
"""
import sqlite3

DB = 'data/damodaran_data_new.db'

# ── Mapeamento: category → lista de (tag_type, tag_value) ──
CATEGORY_TAG_MAP = {
    # ═══ EQUITY - Large Cap ═══
    'Large Blend':          [('asset_class','Equity'), ('cap_size','Large-Cap'), ('style','Blend')],
    'Large Value':          [('asset_class','Equity'), ('cap_size','Large-Cap'), ('style','Value')],
    'Large Growth':         [('asset_class','Equity'), ('cap_size','Large-Cap'), ('style','Growth')],
    'Foreign Large Blend':  [('asset_class','Equity'), ('cap_size','Large-Cap'), ('style','Blend'), ('geography','International')],
    'Foreign Large Value':  [('asset_class','Equity'), ('cap_size','Large-Cap'), ('style','Value'), ('geography','International')],
    'Foreign Large Growth': [('asset_class','Equity'), ('cap_size','Large-Cap'), ('style','Growth'), ('geography','International')],

    # ═══ EQUITY - Mid Cap ═══
    'Mid-Cap Blend':        [('asset_class','Equity'), ('cap_size','Mid-Cap'), ('style','Blend')],
    'Mid-Cap Value':        [('asset_class','Equity'), ('cap_size','Mid-Cap'), ('style','Value')],
    'Mid-Cap Growth':       [('asset_class','Equity'), ('cap_size','Mid-Cap'), ('style','Growth')],

    # ═══ EQUITY - Small Cap ═══
    'Small Blend':          [('asset_class','Equity'), ('cap_size','Small-Cap'), ('style','Blend')],
    'Small Value':          [('asset_class','Equity'), ('cap_size','Small-Cap'), ('style','Value')],
    'Small Growth':         [('asset_class','Equity'), ('cap_size','Small-Cap'), ('style','Growth')],

    # ═══ EQUITY - Global/Regional ═══
    'Global Small/Mid Stock':   [('asset_class','Equity'), ('geography','Global')],
    'Global Real Estate':       [('asset_class','Real Estate'), ('geography','Global')],
    'Global Moderately Conservative Allocation': [('asset_class','Multi-Asset'), ('geography','Global'), ('strategy','Passive')],
    'Global Moderately Aggressive Allocation':   [('asset_class','Multi-Asset'), ('geography','Global'), ('strategy','Passive')],
    'Global Moderate Allocation':                [('asset_class','Multi-Asset'), ('geography','Global'), ('strategy','Passive')],
    'Global Conservative Allocation':            [('asset_class','Multi-Asset'), ('geography','Global'), ('strategy','Passive')],
    'Diversified Emerging Mkts': [('asset_class','Equity'), ('geography','Emerging Markets')],
    'Diversified Pacific/Asia':  [('asset_class','Equity'), ('geography','Asia')],
    'China Region':             [('asset_class','Equity'), ('geography','China')],
    'Japan Stock':              [('asset_class','Equity'), ('geography','Japan')],
    'Pacific/Asia ex-Japan Stk':[('asset_class','Equity'), ('geography','Asia')],
    'Europe Stock':             [('asset_class','Equity'), ('geography','Europe')],
    'Latin America Stock':      [('asset_class','Equity'), ('geography','Latin America')],
    'India Equity':             [('asset_class','Equity'), ('geography','India')],
    'Miscellaneous Region':     [('asset_class','Equity'), ('geography','International')],

    # ═══ EQUITY - Sectors ═══
    'Technology':           [('asset_class','Equity'), ('sector','Technology')],
    'Health':               [('asset_class','Equity'), ('sector','Healthcare')],
    'Financial':            [('asset_class','Equity'), ('sector','Financials')],
    'Industrials':          [('asset_class','Equity'), ('sector','Industrials')],
    'Communications':       [('asset_class','Equity'), ('sector','Communications')],
    'Consumer Cyclical':    [('asset_class','Equity'), ('sector','Consumer Discretionary')],
    'Consumer Defensive':   [('asset_class','Equity'), ('sector','Consumer Staples')],
    'Utilities':            [('asset_class','Equity'), ('sector','Utilities')],
    'Real Estate':          [('asset_class','Real Estate'), ('sector','Real Estate')],
    'Equity Energy':        [('asset_class','Equity'), ('sector','Energy')],
    'Equity Precious Metals': [('asset_class','Equity'), ('sector','Materials')],
    'Natural Resources':    [('asset_class','Equity'), ('sector','Energy')],
    'Infrastructure':       [('asset_class','Equity'), ('sector','Industrials')],
    'Miscellaneous Sector': [('asset_class','Equity')],

    # ═══ FIXED INCOME ═══
    'Corporate Bond':       [('asset_class','Fixed Income'), ('geography','US')],
    'High Yield Bond':      [('asset_class','Fixed Income'), ('geography','US')],
    'Intermediate Core Bond': [('asset_class','Fixed Income'), ('geography','US')],
    'Intermediate Government': [('asset_class','Fixed Income'), ('geography','US')],
    'Long Government':      [('asset_class','Fixed Income'), ('geography','US')],
    'Short Government':     [('asset_class','Fixed Income'), ('geography','US')],
    'Short-Term Bond':      [('asset_class','Fixed Income'), ('geography','US')],
    'Ultrashort Bond':      [('asset_class','Fixed Income'), ('geography','US')],
    'Inflation-Protected Bond': [('asset_class','Fixed Income'), ('geography','US')],
    'Government Mortgage-Backed Bond': [('asset_class','Fixed Income'), ('geography','US')],
    'Muni National Interm': [('asset_class','Fixed Income'), ('geography','US')],
    'Emerging Markets Bond': [('asset_class','Fixed Income'), ('geography','Emerging Markets')],
    'Emerging-Markets Local-Currency Bond': [('asset_class','Fixed Income'), ('geography','Emerging Markets')],
    'Global Bond':          [('asset_class','Fixed Income'), ('geography','Global')],
    'Global Bond-USD Hedged': [('asset_class','Fixed Income'), ('geography','Global')],

    # ═══ COMMODITY ═══
    'Commodities Focused':  [('asset_class','Commodity')],
    'Commodities Broad Basket': [('asset_class','Commodity'), ('strategy','Passive')],

    # ═══ DIGITAL ASSETS ═══
    'Digital Assets':       [('asset_class','Crypto')],

    # ═══ LEVERAGED / INVERSE ═══
    'Trading--Leveraged Equity': [('asset_class','Equity'), ('strategy','Leveraged')],
    'Trading--Inverse Equity':   [('asset_class','Equity'), ('strategy','Inverse')],
    'Trading--Miscellaneous':    [('strategy','Leveraged')],
}

# ── Strategy: se a category não tem Leveraged/Inverse, assumir Passive ──
PASSIVE_CATEGORIES = set(CATEGORY_TAG_MAP.keys()) - {
    'Trading--Leveraged Equity', 'Trading--Inverse Equity', 'Trading--Miscellaneous'
}


def enrich_tags():
    conn = sqlite3.connect(DB)
    
    # Existing tags set for dedup
    existing = set()
    for r in conn.execute("SELECT etf_ticker, tag_type, tag_value FROM etf_tags"):
        existing.add((r[0], r[1], r[2]))
    
    print(f"Tags existentes: {len(existing)}")
    
    # Get all ETFs with categories
    etfs = conn.execute("SELECT ticker, category FROM etfs WHERE category IS NOT NULL").fetchall()
    print(f"ETFs com category: {len(etfs)}")
    
    new_tags = []
    unmapped = set()
    
    for ticker, category in etfs:
        tags = CATEGORY_TAG_MAP.get(category, [])
        if not tags:
            unmapped.add(category)
            continue
        
        # Add strategy=Passive if not a leveraged/inverse category and no strategy tag exists
        has_strategy = any(t[0] == 'strategy' for t in tags)
        if not has_strategy and category in PASSIVE_CATEGORIES:
            tags = tags + [('strategy', 'Passive')]
        
        for tag_type, tag_value in tags:
            key = (ticker, tag_type, tag_value)
            if key not in existing:
                new_tags.append(key)
                existing.add(key)
    
    if unmapped:
        print(f"\nCategorias sem mapeamento: {unmapped}")
    
    print(f"\nNovas tags a inserir: {len(new_tags)}")
    
    if new_tags:
        conn.executemany(
            "INSERT INTO etf_tags (etf_ticker, tag_type, tag_value) VALUES (?, ?, ?)",
            new_tags
        )
        conn.commit()
        print("Tags inseridas com sucesso!")
    
    # Report new coverage
    print("\n=== COBERTURA ATUALIZADA ===")
    total = conn.execute("SELECT COUNT(*) FROM etfs").fetchone()[0]
    for tt in ['asset_class','geography','cap_size','style','sector','strategy']:
        tagged = conn.execute("SELECT COUNT(DISTINCT etf_ticker) FROM etf_tags WHERE tag_type=?", [tt]).fetchone()[0]
        print(f"  {tt}: {tagged}/{total} ({tagged*100//total}%)")
    
    total_tags = conn.execute("SELECT COUNT(*) FROM etf_tags").fetchone()[0]
    print(f"\nTotal tags: {total_tags}")
    
    conn.close()


if __name__ == '__main__':
    enrich_tags()
