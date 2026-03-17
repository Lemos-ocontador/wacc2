"""Verifica campos TTM disponíveis nos quarterly statements do Yahoo Finance."""
import os, warnings
os.environ.setdefault('CURL_CA_BUNDLE', r'C:\cacerts\cacert.pem')
warnings.filterwarnings('ignore')
import yfinance as yf
import pandas as pd

ticker = yf.Ticker('POSI3.SA')

print('=== QUARTERLY INCOME STATEMENT ===')
qi = ticker.quarterly_income_stmt
if qi is not None and not qi.empty:
    print(f'Períodos: {[str(c.date()) for c in qi.columns]}')
    print(f'Campos ({len(qi.index)}):')
    for field in qi.index:
        vals = [qi.loc[field, c] for c in qi.columns]
        non_null = sum(1 for v in vals if not pd.isna(v))
        print(f'  {field:50s} ({non_null}/{len(vals)} preenchidos)')

print()
print('=== QUARTERLY CASH FLOW ===')
qcf = ticker.quarterly_cash_flow
if qcf is not None and not qcf.empty:
    print(f'Períodos: {[str(c.date()) for c in qcf.columns]}')
    for field in qcf.index:
        vals = [qcf.loc[field, c] for c in qcf.columns]
        non_null = sum(1 for v in vals if not pd.isna(v))
        print(f'  {field:50s} ({non_null}/{len(vals)} preenchidos)')

print()
print('=== QUARTERLY BALANCE SHEET (campos-chave) ===')
qbs = ticker.quarterly_balance_sheet
if qbs is not None and not qbs.empty:
    print(f'Períodos: {[str(c.date()) for c in qbs.columns]}')
    key_fields = ['Total Debt', 'Cash And Cash Equivalents', 'Ordinary Shares Number', 
                  'Preferred Stock', 'Minority Interest', 'Total Assets', 'Stockholders Equity']
    for field in key_fields:
        if field in qbs.index:
            vals = [qbs.loc[field, c] for c in qbs.columns]
            print(f'  {field:50s} = {[f"{v/1e6:.1f}M" if not pd.isna(v) else "NaN" for v in vals]}')
        else:
            print(f'  {field:50s} = [ausente]')

print()
print('=== CAMPOS TTM DO info ===')
info = ticker.get_info()
ttm_fields = {
    'totalRevenue': 'Receita Total (TTM)',
    'ebitda': 'EBITDA (TTM)',
    'operatingMargins': 'Margem Operacional (TTM)',
    'ebitdaMargins': 'Margem EBITDA (TTM)',
    'grossMargins': 'Margem Bruta (TTM)',
    'profitMargins': 'Margem Líquida (TTM)',
    'grossProfits': 'Lucro Bruto (TTM)',
    'freeCashflow': 'FCF (TTM)',
    'operatingCashflow': 'Fluxo Cx Operacional (TTM)',
    'netIncomeToCommon': 'Lucro Líquido (TTM)',
    'revenuePerShare': 'Receita/Ação (TTM)',
    'returnOnAssets': 'ROA (TTM)',
    'returnOnEquity': 'ROE (TTM)',
    'earningsGrowth': 'Crescimento Lucros (TTM)',
    'revenueGrowth': 'Crescimento Receita (TTM)',
    'currentRatio': 'Índice Liquidez Corrente',
    'quickRatio': 'Índice Liquidez Seca',
    'debtToEquity': 'Dívida/PL',
    'enterpriseValue': 'Enterprise Value (corrente)',
    'enterpriseToRevenue': 'EV/Revenue (TTM)',
    'enterpriseToEbitda': 'EV/EBITDA (TTM)',
    'marketCap': 'Market Cap (corrente)',
    'totalCash': 'Caixa Total',
    'totalDebt': 'Dívida Total',
    'priceToSalesTrailing12Months': 'P/S (TTM)',
    'trailingEps': 'LPA (TTM)',
    'forwardEps': 'LPA (Forward)',
    'forwardPE': 'P/E (Forward)',
    'trailingPegRatio': 'PEG (TTM)',
    'bookValue': 'Valor Patrimonial/Ação',
    'priceToBook': 'P/VP',
    'payoutRatio': 'Payout Ratio',
    'dividendYield': 'Dividend Yield',
    'dividendRate': 'Dividendo/Ação',
    'beta': 'Beta',
}

for key, label in ttm_fields.items():
    val = info.get(key)
    if val is not None:
        if isinstance(val, (int, float)) and abs(val) >= 1e6:
            print(f'  {key:45s} ({label:35s}) = {val/1e6:,.2f}M')
        elif isinstance(val, float) and abs(val) < 1:
            print(f'  {key:45s} ({label:35s}) = {val:.4f} ({val*100:.2f}%)')
        else:
            print(f'  {key:45s} ({label:35s}) = {val}')
    else:
        print(f'  {key:45s} ({label:35s}) = N/A')

# TTM calculado manualmente a partir dos trimestrais
print()
print('=== TTM CALCULADO MANUALMENTE (soma 4 últimos trimestres) ===')
if qi is not None and not qi.empty and len(qi.columns) >= 4:
    last4 = qi.columns[:4]
    ttm_calc_fields = ['Total Revenue', 'EBIT', 'EBITDA', 'Net Income', 'Gross Profit',
                       'Operating Income', 'Interest Expense', 'Tax Provision',
                       'Research And Development', 'Selling General And Administration',
                       'Cost Of Revenue', 'Normalized EBITDA']
    for field in ttm_calc_fields:
        if field in qi.index:
            vals = [qi.loc[field, c] for c in last4]
            non_null = [v for v in vals if not pd.isna(v)]
            if non_null:
                ttm = sum(non_null)
                print(f'  {field:45s} TTM = {ttm/1e6:,.2f}M  (de {len(non_null)}/4 trimestres)')
            else:
                print(f'  {field:45s} TTM = N/A (todos NaN)')
        else:
            print(f'  {field:45s} = [campo ausente]')
    
    print()
    print('  Trimestres usados para TTM:')
    for c in last4:
        print(f'    {c.date()}')

if qcf is not None and not qcf.empty and len(qcf.columns) >= 4:
    last4cf = qcf.columns[:4]
    print()
    print('=== TTM CASH FLOW (soma 4 últimos trimestres) ===')
    cf_fields = ['Free Cash Flow', 'Operating Cash Flow', 'Capital Expenditure']
    for field in cf_fields:
        if field in qcf.index:
            vals = [qcf.loc[field, c] for c in last4cf]
            non_null = [v for v in vals if not pd.isna(v)]
            if non_null:
                ttm = sum(non_null)
                print(f'  {field:45s} TTM = {ttm/1e6:,.2f}M  (de {len(non_null)}/4 trimestres)')
