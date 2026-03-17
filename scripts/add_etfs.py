"""
Adicionar IGV ao BD e extrair holdings via SEC (trust map).
Re-extrair SKYY com dados completos via SEC (buscar CIK direto).
"""
import sqlite3, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')
from data_extractors.etf_extractor import ETFExtractor

DB = 'data/damodaran_data_new.db'

def add_and_extract(tickers):
    ext = ETFExtractor(db_path=DB)
    
    for ticker in tickers:
        print(f'\n{"="*50}')
        print(f'Processando: {ticker}')
        print(f'{"="*50}')
        
        # 1. Get metadata via yfinance
        print(f'  [1] Obtendo metadados via yfinance...')
        meta = ext.get_etf_metadata(ticker)
        if meta:
            ext.save_etf(meta)
            print(f'  -> Nome: {meta.get("name")}')
            print(f'  -> Categoria: {meta.get("category")}')
            print(f'  -> Issuer: {meta.get("issuer")}')
            print(f'  -> AUM: {meta.get("aum")}')
        else:
            print(f'  -> Metadados não encontrados, criando entry básico')
        
        # 2. Get holdings with fallback (SEC → yfinance)
        print(f'  [2] Extraindo holdings...')
        holdings, source = ext.get_holdings_with_fallback(ticker)
        print(f'  -> Fonte: {source}, Holdings: {len(holdings)}')
        
        if holdings:
            # 3. Save holdings
            print(f'  [3] Salvando holdings...')
            saved = ext.save_holdings(ticker, holdings, source)
            print(f'  -> Salvos: {saved}')
            
            # Show top 5
            for h in holdings[:5]:
                print(f'      {h.get("holding_ticker","?")}: {h.get("holding_name","?")} ({h.get("weight","?")}%)')
        else:
            print(f'  -> Nenhum holding encontrado!')
        
        # 4. Update data_source in etfs table
        if source != 'none':
            conn = sqlite3.connect(DB)
            conn.execute("UPDATE etfs SET data_source=?, total_holdings=? WHERE ticker=?",
                        [source, len(holdings), ticker])
            conn.commit()
            conn.close()
            print(f'  [4] Fonte atualizada: {source}')

if __name__ == '__main__':
    add_and_extract(['IGV', 'SKYY'])
