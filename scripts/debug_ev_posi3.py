"""
Debug completo do cálculo do EV para POSI3.SA
Mostra todos os valores obtidos do Yahoo Finance e o cálculo efetivo.
"""
import os
import sys
import warnings
import sqlite3
from pathlib import Path

os.environ.setdefault("CURL_CA_BUNDLE", r"C:\cacerts\cacert.pem")
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*Timestamp.utcnow.*")

import yfinance as yf
import pandas as pd
import numpy as np

YAHOO_CODE = "POSI3.SA"
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "damodaran_data_new.db"

def fmt(value, decimals=2):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    if abs(value) >= 1e9:
        return f"{value/1e9:,.{decimals}f}B"
    if abs(value) >= 1e6:
        return f"{value/1e6:,.{decimals}f}M"
    if abs(value) >= 1e3:
        return f"{value/1e3:,.{decimals}f}K"
    return f"{value:,.{decimals}f}"

def fmt_raw(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"{value:,.2f}"

print("=" * 100)
print(f"  DEBUG EV CALCULATION - {YAHOO_CODE}")
print("=" * 100)

# 1. Dados correntes do Yahoo (info)
ticker = yf.Ticker(YAHOO_CODE)
info = ticker.get_info()

print("\n" + "=" * 100)
print("  PARTE 1: DADOS CORRENTES DO YAHOO (.info)")
print("=" * 100)
print(f"  quoteType:              {info.get('quoteType')}")
print(f"  currency:               {info.get('currency')}")
print(f"  financialCurrency:      {info.get('financialCurrency')}")
print(f"  sharesOutstanding:      {fmt(info.get('sharesOutstanding'))}")
print(f"  floatShares:            {fmt(info.get('floatShares'))}")
print(f"  marketCap:              {fmt(info.get('marketCap'))}")
print(f"  enterpriseValue:        {fmt(info.get('enterpriseValue'))}")
print(f"  currentPrice:           {fmt_raw(info.get('currentPrice'))}")
print(f"  totalDebt:              {fmt(info.get('totalDebt'))}")
print(f"  totalCash:              {fmt(info.get('totalCash'))}")
print(f"  enterpriseToRevenue:    {fmt_raw(info.get('enterpriseToRevenue'))}")
print(f"  enterpriseToEbitda:     {fmt_raw(info.get('enterpriseToEbitda'))}")

# 2. Balance Sheet histórico
print("\n" + "=" * 100)
print("  PARTE 2: BALANCE SHEET ANUAL (yfinance)")
print("=" * 100)

balance = ticker.balance_sheet
if balance is not None and not balance.empty:
    fields_bs = [
        "Total Debt", "Current Debt", "Long Term Debt",
        "Cash And Cash Equivalents", "Ordinary Shares Number",
        "Preferred Stock", "Minority Interest",
        "Total Assets", "Stockholders Equity",
        "Total Liabilities Net Minority Interest",
    ]
    for col in balance.columns:
        period_date = str(col.date())
        print(f"\n  --- Período: {period_date} ---")
        for field in fields_bs:
            if field in balance.index:
                val = balance.loc[field, col]
                if pd.isna(val):
                    print(f"    {field:45s} = NaN")
                else:
                    print(f"    {field:45s} = {fmt(val)} ({fmt_raw(val)})")
            else:
                print(f"    {field:45s} = [campo ausente]")
else:
    print("  Balance Sheet vazio ou indisponível!")

# 3. Income Statement
print("\n" + "=" * 100)
print("  PARTE 3: INCOME STATEMENT ANUAL (campos-chave)")
print("=" * 100)

income = ticker.income_stmt
if income is not None and not income.empty:
    fields_is = ["Total Revenue", "EBIT", "EBITDA", "Net Income"]
    for col in income.columns:
        period_date = str(col.date())
        print(f"\n  --- Período: {period_date} ---")
        for field in fields_is:
            if field in income.index:
                val = income.loc[field, col]
                if pd.isna(val):
                    print(f"    {field:30s} = NaN")
                else:
                    print(f"    {field:30s} = {fmt(val)} ({fmt_raw(val)})")
            else:
                print(f"    {field:30s} = [campo ausente]")

# 4. Preços históricos
print("\n" + "=" * 100)
print("  PARTE 4: PREÇOS HISTÓRICOS NAS DATAS-BASE")
print("=" * 100)

hist = ticker.history(period="10y", interval="1d")
if not hist.empty:
    tz = hist.index.tz
    period_dates = [str(col.date()) for col in (income.columns if income is not None and not income.empty else [])]
    
    for period_date_str in period_dates:
        pd_date = pd.Timestamp(period_date_str)
        if tz is not None:
            pd_date = pd_date.tz_localize(tz)
        closest_idx = hist.index.get_indexer([pd_date], method="nearest")[0]
        if 0 <= closest_idx < len(hist):
            price = float(hist.iloc[closest_idx]["Close"])
            actual_date = hist.index[closest_idx]
            print(f"  Período {period_date_str}:  preço = {price:.4f}  (data real: {actual_date.date()}, diff = {abs((actual_date.date() - pd.Timestamp(period_date_str).date()).days)} dias)")
        else:
            print(f"  Período {period_date_str}:  preço NÃO ENCONTRADO")
else:
    print("  Histórico de preços vazio!")

# 5. Taxas de câmbio
print("\n" + "=" * 100)
print("  PARTE 5: TAXAS DE CÂMBIO (BRL -> USD)")
print("=" * 100)

financial_currency = info.get("financialCurrency") or info.get("currency")
if financial_currency and financial_currency != "USD":
    pair = f"{financial_currency}USD=X"
    fx_ticker = yf.Ticker(pair)
    fx_hist = fx_ticker.history(period="10y", interval="1d")
    if not fx_hist.empty:
        fx_tz = fx_hist.index.tz
        for period_date_str in period_dates:
            pd_date = pd.Timestamp(period_date_str)
            if fx_tz is not None:
                pd_date = pd_date.tz_localize(fx_tz)
            closest_idx = fx_hist.index.get_indexer([pd_date], method="nearest")[0]
            if 0 <= closest_idx < len(fx_hist):
                fx_rate = float(fx_hist.iloc[closest_idx]["Close"])
                actual_date = fx_hist.index[closest_idx]
                print(f"  Período {period_date_str}:  FX {financial_currency}/USD = {fx_rate:.6f}  (data real: {actual_date.date()})")
            else:
                print(f"  Período {period_date_str}:  FX NÃO ENCONTRADO")
    else:
        print(f"  Sem dados FX para {pair}")
else:
    print(f"  Moeda = {financial_currency} (sem conversão necessária)")

# 6. Cálculo passo-a-passo do EV
print("\n" + "=" * 100)
print("  PARTE 6: CÁLCULO DO EV PASSO-A-PASSO")
print("=" * 100)

shares_outstanding_current = info.get("sharesOutstanding")
print(f"  Shares Outstanding (corrente/fallback): {fmt(shares_outstanding_current)}")

if income is not None and not income.empty and balance is not None and not balance.empty:
    for col in income.columns:
        period_date = str(col.date())
        print(f"\n  {'='*80}")
        print(f"  PERÍODO: {period_date}")
        print(f"  {'='*80}")
        
        # Shares do balance sheet
        bs_col_match = None
        for bs_col in balance.columns:
            if str(bs_col.date()) == period_date:
                bs_col_match = bs_col
                break
        
        if bs_col_match is not None:
            shares_bs = balance.loc["Ordinary Shares Number", bs_col_match] if "Ordinary Shares Number" in balance.index else None
            if pd.isna(shares_bs):
                shares_bs = None
        else:
            shares_bs = None
            print(f"    [!] Balance Sheet não tem dados para {period_date}")
        
        shares = shares_bs if shares_bs else shares_outstanding_current
        print(f"    Shares (balance sheet):  {fmt(shares_bs) if shares_bs else 'N/A'}")
        print(f"    Shares (usado):          {fmt(shares)} {'(fallback corrente)' if not shares_bs else '(balance sheet)'}")
        
        # Preço
        pd_date = pd.Timestamp(period_date)
        if tz is not None:
            pd_date = pd_date.tz_localize(tz)
        closest_idx = hist.index.get_indexer([pd_date], method="nearest")[0]
        price = float(hist.iloc[closest_idx]["Close"]) if 0 <= closest_idx < len(hist) else None
        print(f"    Preço (close):           {fmt_raw(price) if price else 'N/A'}")
        
        # Market Cap
        if price and shares:
            mcap = price * shares
            # Check sanidade
            rev = None
            if "Total Revenue" in income.index:
                rev_val = income.loc["Total Revenue", col]
                if not pd.isna(rev_val):
                    rev = float(rev_val)
            
            if rev and rev > 0 and mcap > 1e9 and (mcap / rev) > 50000:
                print(f"    Market Cap bruto:        {fmt(mcap)} ({fmt_raw(mcap)})")
                print(f"    [!] REJEITADO: MCap/Revenue = {mcap/rev:,.0f}x > 50,000x e MCap > 1B")
                mcap = None
            else:
                print(f"    Market Cap estimado:     {fmt(mcap)} ({fmt_raw(mcap)})")
                if rev and rev > 0:
                    print(f"    MCap/Revenue check:      {mcap/rev:,.2f}x (OK)")
        else:
            mcap = None
            print(f"    Market Cap:              N/A (preço ou shares ausente)")
        
        # Componentes EV do balance sheet
        if bs_col_match is not None:
            def get_bs(field):
                if field in balance.index:
                    v = balance.loc[field, bs_col_match]
                    return None if pd.isna(v) else float(v)
                return None
            
            debt = get_bs("Total Debt") or 0
            pref = get_bs("Preferred Stock") or 0
            mi = get_bs("Minority Interest") or 0
            cash = get_bs("Cash And Cash Equivalents") or 0
            
            print(f"    Total Debt:              {fmt(debt)} ({fmt_raw(debt)})")
            print(f"    Preferred Stock:         {fmt(pref)} ({fmt_raw(pref)})")
            print(f"    Minority Interest:       {fmt(mi)} ({fmt_raw(mi)})")
            print(f"    Cash & Equivalents:      {fmt(cash)} ({fmt_raw(cash)})")
        else:
            debt = pref = mi = cash = 0
            print(f"    [!] Sem dados de balance sheet")
        
        # EV Final
        if mcap is not None:
            ev = mcap + debt + pref + mi - cash
            print(f"\n    >>> FÓRMULA: EV = MCap + Debt + Pref + MI - Cash")
            print(f"    >>> EV = {fmt(mcap)} + {fmt(debt)} + {fmt(pref)} + {fmt(mi)} - {fmt(cash)}")
            print(f"    >>> EV = {fmt(ev)} ({fmt_raw(ev)})")
            
            # Conversão USD
            if financial_currency and financial_currency != "USD" and not fx_hist.empty:
                pd_date_fx = pd.Timestamp(period_date)
                if fx_tz is not None:
                    pd_date_fx = pd_date_fx.tz_localize(fx_tz)
                fx_idx = fx_hist.index.get_indexer([pd_date_fx], method="nearest")[0]
                if 0 <= fx_idx < len(fx_hist):
                    fx_rate = float(fx_hist.iloc[fx_idx]["Close"])
                    ev_usd = ev * fx_rate
                    print(f"    >>> EV (USD) = {fmt(ev)} × {fx_rate:.6f} = {fmt(ev_usd)} ({fmt_raw(ev_usd)})")
            
            # Múltiplos
            if rev and rev > 0:
                ev_rev = ev / rev
                print(f"    >>> EV/Revenue = {ev_rev:.2f}x")
            
            ebitda = None
            if "EBITDA" in income.index:
                ebitda_val = income.loc["EBITDA", col]
                if not pd.isna(ebitda_val):
                    ebitda = float(ebitda_val)
            if ebitda and ebitda != 0:
                ev_ebitda = ev / ebitda
                print(f"    >>> EV/EBITDA = {ev_ebitda:.2f}x")
            
            ebit = None
            if "EBIT" in income.index:
                ebit_val = income.loc["EBIT", col]
                if not pd.isna(ebit_val):
                    ebit = float(ebit_val)
            if ebit and ebit > 0:
                ev_ebit = ev / ebit
                print(f"    >>> EV/EBIT = {ev_ebit:.2f}x")
        else:
            print(f"\n    >>> EV = N/A (Market Cap indisponível)")

# 7. Dados armazenados no banco
print("\n" + "=" * 100)
print("  PARTE 7: DADOS ARMAZENADOS NO BANCO (company_financials_historical)")
print("=" * 100)

if DB_PATH.exists():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT cfh.period_date, cfh.fiscal_year, cfh.period_type,
               cfh.market_cap_estimated, cfh.enterprise_value_estimated,
               cfh.enterprise_value_usd, cfh.total_debt, cfh.cash_and_equivalents,
               cfh.preferred_stock, cfh.minority_interest,
               cfh.ordinary_shares_number, cfh.fx_rate_to_usd,
               cfh.total_revenue, cfh.ebitda, cfh.ebit,
               cfh.ev_revenue, cfh.ev_ebitda, cfh.ev_ebit,
               cfh.original_currency
        FROM company_financials_historical cfh
        JOIN company_basic_data cbd ON cbd.id = cfh.company_basic_data_id
        WHERE cbd.yahoo_code = ? AND cfh.period_type = 'annual'
        ORDER BY cfh.period_date
    """, (YAHOO_CODE,)).fetchall()
    
    if rows:
        for r in rows:
            print(f"\n  --- {r['period_date']} (FY {r['fiscal_year']}) [{r['original_currency']}] ---")
            print(f"    Shares (stored):         {fmt(r['ordinary_shares_number'])}")
            print(f"    Market Cap (stored):     {fmt(r['market_cap_estimated'])}")
            print(f"    Total Debt (stored):     {fmt(r['total_debt'])}")
            print(f"    Cash (stored):           {fmt(r['cash_and_equivalents'])}")
            print(f"    Preferred (stored):      {fmt(r['preferred_stock'])}")
            print(f"    Minority Int (stored):   {fmt(r['minority_interest'])}")
            print(f"    EV (stored):             {fmt(r['enterprise_value_estimated'])}")
            print(f"    EV USD (stored):         {fmt(r['enterprise_value_usd'])}")
            print(f"    FX Rate (stored):        {r['fx_rate_to_usd']}")
            print(f"    Revenue (stored):        {fmt(r['total_revenue'])}")
            print(f"    EBITDA (stored):         {fmt(r['ebitda'])}")
            print(f"    EV/Revenue (stored):     {r['ev_revenue']}")
            print(f"    EV/EBITDA (stored):      {r['ev_ebitda']}")
            print(f"    EV/EBIT (stored):        {r['ev_ebit']}")
    else:
        print(f"  Nenhum dado encontrado para {YAHOO_CODE} no banco.")
    conn.close()
else:
    print(f"  Banco não encontrado: {DB_PATH}")

print("\n" + "=" * 100)
print("  FIM DO DEBUG")
print("=" * 100)
