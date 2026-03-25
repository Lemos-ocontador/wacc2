"""
Script de correção automática dos yahoo_codes das empresas sem sufixo de exchange.

O campo 'ticker' do Damodaran contém o prefixo da exchange (ex: ENXTBR:ABI, OM:ALFA).
O yahoo_code precisa do sufixo Yahoo correspondente (ex: ABI.BR, ALFA.ST).

Este script:
1. Identifica empresas com about mas sem dados financeiros anuais e sem sufixo no yahoo_code
2. Mapeia o prefixo do Damodaran para o sufixo Yahoo usando dados de referência
3. Atualiza o yahoo_code no banco de dados

Uso:
  python scripts/fix_yahoo_code_suffix.py              # Dry-run (apenas mostra o que faria)
  python scripts/fix_yahoo_code_suffix.py --apply      # Aplica as correções
  python scripts/fix_yahoo_code_suffix.py --test 10    # Testa N tickers com yfinance antes de aplicar
"""

import sqlite3
import argparse
import sys
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'damodaran_data_new.db')

# ========================================================================
# Mapeamento: prefixo da exchange Damodaran → sufixo Yahoo Finance
# Baseado em dados de referência (empresas que JÁ possuem dados financeiros)
# ========================================================================
EXCHANGE_TO_YAHOO_SUFFIX = {
    # --- Bolsas dos EUA (sem sufixo no Yahoo) ---
    'OTCPK':    None,   # OTC Pink Sheets
    'NasdaqGM': None,   # Nasdaq Global Market
    'NasdaqCM': None,   # Nasdaq Capital Market
    'NasdaqGS': None,   # Nasdaq Global Select
    'NYSE':     None,   # New York Stock Exchange
    'NYSEAM':   None,   # NYSE American (AMEX)

    # --- Canadá ---
    'CNSX':  '.CN',     # Canadian Securities Exchange      (ref: 237 com .CN)
    'NEOE':  '.NE',     # NEO Exchange                      (ref: 2 com .NE)

    # --- Reino Unido ---
    'AIM':   '.L',      # London AIM Market                 (ref: 266 com .L)

    # --- Nórdicos ---
    'OM':    '.ST',     # OMX Stockholm                     (ref: 494 com .ST)
    'NGM':   '.ST',     # Nasdaq Nordic / NGM               (ref: 21 com .ST)
    'XSAT':  '.ST',     # First North Stockholm             (ref: 32 com .ST)
    'OB':    '.OL',     # Oslo Børs                         (ref: 150 com .OL)
    'CPSE':  '.CO',     # Copenhagen                        (ref: 61 com .CO)

    # --- Europa Ocidental ---
    'ENXTBR': '.BR',    # Euronext Bruxelas                 (ref: 62 com .BR)
    'ENXTAM': '.AS',    # Euronext Amsterdã                 (ref: 49 com .AS)
    'ENXTLS': '.LS',    # Euronext Lisboa                   (ref: 12 com .LS)
    'WBAG':   '.VI',    # Wiener Börse (Viena)              (ref: 16 com .VI)
    'BRSE':   '.SW',    # Bern Stock Exchange (Suíça)
    'DB':     '.F',     # Deutsche Börse / Frankfurt        (ref: 33 com .F)
    'HMSE':   '.HM',    # Hamburg Stock Exchange             (ref: 22 com .HM)

    # --- Europa do Sul / Leste ---
    'ATSE':   '.AT',    # Athens Stock Exchange              (ref: 36 com .AT)
    'IBSE':   '.IS',    # Borsa Istanbul                    (ref: 334 com .IS)
    'SEP':    '.PR',    # Prague Stock Exchange              (ref: 9 com .PR, validado CEZ.PR)
    'LJSE':   '.LJ',    # Ljubljana SE (Eslovênia)
    'BELEX':  '.BE',    # Belgrade SE (Sérvia)
    'BUL':    '.SO',    # Bulgarian Stock Exchange (Sófia)
    'ZGSE':   '.ZA',    # Zagreb Stock Exchange (Croácia)
    'CSE':    '.CY',    # Cyprus Stock Exchange

    # --- Ásia-Pacífico ---
    'HOSE':     '.VN',  # Ho Chi Minh SE                    (ref: 54 com .VN)
    'HNX':      '.VN',  # Hanoi Stock Exchange (Vietnam)
    'PSE':      '.PS',  # Philippine Stock Exchange
    'NZSE':     '.NZ',  # New Zealand SE                    (ref: 44 com .NZ)
    'Catalist': '.SI',  # Singapore Catalist                (ref: 119 com .SI)

    # --- Oriente Médio ---
    'ADX':   '.AD',     # Abu Dhabi Securities Exchange
    'DFM':   '.AE',     # Dubai Financial Market            (ref: 20 com .AE)
    'DIFX':  '.AE',     # Dubai International Financial Ex.
    'DSM':   '.QA',     # Doha / Qatar                      (ref: 31 com .QA)
    'KWSE':  '.KW',     # Kuwait SE                         (ref: 68 com .KW)
    'MSM':   '.OM',     # Muscat Securities Market (Omã)
    'ASE':   '.AM',     # Amman SE (Jordânia)
    'PLSE':  '.AM',     # Palestine SE → listada em Amman

    # --- África ---
    'CASE':  '.CA',     # Cairo SE (Egito)
    'CBSE':  '.CS',     # Casablanca SE (Marrocos)
    'NASE':  '.NR',     # Nairobi SE (Quênia)
    'DAR':   '.TZ',     # Dar es Salaam SE (Tanzânia)
    'UGSE':  '.UG',     # Uganda SE
    'LUSE':  '.LK',     # Lusaka SE (Zâmbia) — tentativo
    'MAL':   '.MW',     # Malawi SE
    'ZMSE':  '.ZW',     # Zimbabwe SE

    # --- América do Sul ---
    'SNSE':  '.SN',     # Santiago SE (Chile)               (ref: 60 com .SN, validado BCI.SN)
    'BASE':  '.BA',     # Buenos Aires SE (Argentina)       (ref: 31 com .BA)

    # --- Paquistão ---
    'KASE':  '.KA',     # Karachi SE

    # --- Outros ---
    'JMSE':   '.JM',    # Jamaica SE
    'DSE':    '.BD',    # Dhaka SE (Bangladesh)
    'BVMT':   '.TU',    # Bourse de Tunis
    'BRVM':   '.IC',    # BRVM (África Ocidental)
    'MTSE':   '.MT',    # Malta SE (Valletta)
    'XSAT':   '.ST',    # First North Stockholm (já listado)
}


def get_companies_to_fix(conn):
    """Retorna empresas sem sufixo no yahoo_code que precisam de correção."""
    rows = conn.execute("""
        SELECT cbd.id, cbd.yahoo_code, cbd.ticker, cbd.company_name, cbd.country
        FROM company_basic_data cbd
        WHERE cbd.yahoo_code IS NOT NULL AND cbd.yahoo_code != ''
          AND COALESCE(cbd.yahoo_no_data, 0) = 0
          AND cbd.about IS NOT NULL
          AND cbd.yahoo_code NOT LIKE '%.%'
          AND cbd.id NOT IN (
              SELECT DISTINCT company_basic_data_id 
              FROM company_financials_historical WHERE period_type='annual'
          )
    """).fetchall()
    return rows


def compute_fixes(rows):
    """Calcula as correções necessárias.
    
    Returns:
        fixes: lista de (id, yahoo_code_atual, yahoo_code_novo, exchange_prefix)
        skipped_us: lista de empresas US sem sufixo (já corretas)
        unknown: lista de empresas com exchange não mapeada
    """
    fixes = []
    skipped_us = []
    unknown = []

    for row_id, yahoo_code, ticker, name, country in rows:
        if not ticker or ':' not in ticker:
            unknown.append((row_id, yahoo_code, ticker, name, country, 'NO_PREFIX'))
            continue

        exchange_prefix = ticker.split(':')[0]
        
        if exchange_prefix not in EXCHANGE_TO_YAHOO_SUFFIX:
            unknown.append((row_id, yahoo_code, ticker, name, country, exchange_prefix))
            continue

        suffix = EXCHANGE_TO_YAHOO_SUFFIX[exchange_prefix]
        
        if suffix is None:
            # Bolsa americana — yahoo_code já está correto sem sufixo
            skipped_us.append((row_id, yahoo_code, ticker, name, country, exchange_prefix))
            continue

        new_yahoo_code = yahoo_code + suffix
        fixes.append((row_id, yahoo_code, new_yahoo_code, exchange_prefix))

    return fixes, skipped_us, unknown


def test_with_yfinance(fixes, n=10):
    """Testa N correções com yfinance para validar."""
    try:
        import yfinance as yf
    except ImportError:
        print("  [ERRO] yfinance não instalado. Instale com: pip install yfinance")
        return

    import random
    sample = random.sample(fixes, min(n, len(fixes)))
    
    print(f"\n{'='*70}")
    print(f"TESTE COM YFINANCE ({len(sample)} tickers)")
    print(f"{'='*70}")
    
    success = 0
    for row_id, old_code, new_code, exchange in sample:
        try:
            t = yf.Ticker(new_code)
            fin = t.financials
            periods = len(fin.columns) if fin is not None and not fin.empty else 0
        except Exception:
            periods = 0
        
        status = "OK" if periods > 0 else "SEM DADOS"
        if periods > 0:
            success += 1
        print(f"  {old_code:14s} -> {new_code:18s} [{exchange:10s}]  {status} ({periods} períodos)")
    
    print(f"\n  Resultado: {success}/{len(sample)} com dados financeiros ({100*success/len(sample):.0f}%)")


def apply_fixes(conn, fixes):
    """Aplica as correções no banco de dados."""
    cursor = conn.cursor()
    for row_id, old_code, new_code, exchange in fixes:
        cursor.execute(
            "UPDATE company_basic_data SET yahoo_code = ? WHERE id = ?",
            (new_code, row_id)
        )
    conn.commit()
    return cursor.rowcount


def main():
    parser = argparse.ArgumentParser(description='Corrige sufixos dos yahoo_codes')
    parser.add_argument('--apply', action='store_true', help='Aplica as correções (sem isso, apenas dry-run)')
    parser.add_argument('--test', type=int, default=0, help='Testa N tickers com yfinance antes de aplicar')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=30)
    
    print("Buscando empresas sem sufixo no yahoo_code...")
    rows = get_companies_to_fix(conn)
    print(f"Total encontradas: {len(rows)}")

    fixes, skipped_us, unknown = compute_fixes(rows)

    # --- Relatório ---
    print(f"\n{'='*70}")
    print(f"RELATÓRIO DE CORREÇÃO")
    print(f"{'='*70}")
    print(f"  Total sem sufixo:          {len(rows):5d}")
    print(f"  Bolsas EUA (sem mudança):  {len(skipped_us):5d}")
    print(f"  Correções a aplicar:       {len(fixes):5d}")
    print(f"  Exchange desconhecida:     {len(unknown):5d}")

    # Breakdown por exchange das correções
    from collections import Counter
    fix_exchanges = Counter(f[3] for f in fixes)
    print(f"\n--- Correções por exchange ---")
    for exch, count in fix_exchanges.most_common():
        suffix = EXCHANGE_TO_YAHOO_SUFFIX.get(exch, '?')
        print(f"  {exch:15s} -> {suffix:6s}  ({count:3d} empresas)")

    # Exemplos de correções
    print(f"\n--- Exemplos de correções ---")
    for _, old, new, exch in fixes[:20]:
        print(f"  {old:14s} -> {new:18s}  [{exch}]")

    if unknown:
        print(f"\n--- Exchanges desconhecidas ---")
        for _, yc, tk, nm, cty, exch in unknown[:10]:
            print(f"  exchange={exch:12s}  yahoo={yc:12s}  ticker={str(tk):20s}  {str(nm)[:25]:25s}  {str(cty)}")

    # Teste com yfinance
    if args.test > 0:
        test_with_yfinance(fixes, args.test)

    # Aplicar correções
    if args.apply:
        print(f"\n{'='*70}")
        print(f"APLICANDO {len(fixes)} CORREÇÕES...")
        print(f"{'='*70}")
        
        apply_fixes(conn, fixes)
        print(f"  {len(fixes)} yahoo_codes atualizados com sucesso!")
        
        # Verificação
        remaining = conn.execute("""
            SELECT COUNT(*) FROM company_basic_data cbd
            WHERE cbd.yahoo_code IS NOT NULL AND cbd.yahoo_code != ''
              AND COALESCE(cbd.yahoo_no_data, 0) = 0
              AND cbd.about IS NOT NULL
              AND cbd.yahoo_code NOT LIKE '%.%'
              AND cbd.id NOT IN (
                  SELECT DISTINCT company_basic_data_id 
                  FROM company_financials_historical WHERE period_type='annual'
              )
        """).fetchone()[0]
        print(f"  Restantes sem sufixo (EUA + desconhecidas): {remaining}")
    else:
        print(f"\n  [DRY-RUN] Nenhuma alteração feita. Use --apply para aplicar.")
        print(f"  [DICA]    Use --test 10 para testar 10 tickers com yfinance primeiro.")

    conn.close()


if __name__ == '__main__':
    main()
