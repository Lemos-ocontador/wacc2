"""
test_data_quality.py
====================
Teste abrangente de qualidade dos dados do relatório EstudoAnloc.
Compara dados brutos do DB (company_financials_historical) com Yahoo Finance ao vivo.

Uso:
  python scripts/test_data_quality.py                   # Roda teste completo
  python scripts/test_data_quality.py --quick            # Amostra reduzida (2 por grupo)
  python scripts/test_data_quality.py --ticker AAPL      # Testa ticker específico
  python scripts/test_data_quality.py --output report    # Salva CSV em cache/

Métricas comparadas (DB vs YF live):
  - Total Revenue, EBITDA, Net Income, Operating Income
  - Total Debt, Cash, Stockholders Equity
  - Enterprise Value, Market Cap
  - EV/EBITDA, EV/Revenue (calculados)

Critérios de outlier:
  - Desvio > 3σ da distribuição de % de desvio por métrica
  - Valores ausentes no DB mas presentes no Yahoo (ou vice-versa)
  - Sinais invertidos (DB positivo, Yahoo negativo)
"""

import argparse
import os
import sys
import sqlite3
import time
import warnings
from datetime import datetime
from pathlib import Path

os.environ.setdefault("CURL_CA_BUNDLE", r"C:\cacerts\cacert.pem")

import yfinance as yf
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")
warnings.filterwarnings("ignore", message=".*Timestamp.utcnow.*")

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "damodaran_data_new.db"
CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"

# ══════════════════════════════════════════════════════════════════════════════
# Amostra diversificada: ~5 tickers por setor × vários países
# Critério: empresas grandes/conhecidas para maximizar chance de dados no Yahoo
# ══════════════════════════════════════════════════════════════════════════════
SAMPLE_TICKERS = {
    # ── EUA (Large Caps) ──
    "Technology|United States": ["AAPL", "MSFT", "GOOGL", "NVDA", "CRM"],
    "Healthcare|United States": ["JNJ", "UNH", "PFE", "ABBV", "TMO"],
    "Financial Services|United States": ["JPM", "BAC", "GS", "BLK", "AXP"],
    "Energy|United States": ["XOM", "CVX", "COP", "SLB", "EOG"],
    "Consumer Cyclical|United States": ["AMZN", "TSLA", "HD", "NKE", "SBUX"],
    "Industrials|United States": ["CAT", "UNP", "HON", "GE", "RTX"],
    "Consumer Defensive|United States": ["PG", "KO", "PEP", "WMT", "COST"],
    "Utilities|United States": ["NEE", "DUK", "SO", "AEP", "SRE"],
    "Communication Services|United States": ["META", "GOOG", "DIS", "NFLX", "CMCSA"],
    "Real Estate|United States": ["PLD", "AMT", "EQIX", "SPG", "O"],
    "Basic Materials|United States": ["LIN", "APD", "ECL", "NEM", "FCX"],

    # ── Brasil ──
    "Energy|Brazil": ["PETR4.SA", "PETR3.SA", "CSAN3.SA", "PRIO3.SA", "UGPA3.SA"],
    "Basic Materials|Brazil": ["VALE3.SA", "SUZB3.SA", "CSNA3.SA", "CMIN3.SA", "DXCO3.SA"],
    "Financial Services|Brazil": ["ITUB4.SA", "BBDC4.SA", "BBAS3.SA", "B3SA3.SA", "SANB11.SA"],
    "Consumer Cyclical|Brazil": ["MGLU3.SA", "LREN3.SA", "VBBR3.SA", "PCAR3.SA", "CYRE3.SA"],
    "Industrials|Brazil": ["WEGE3.SA", "EMBR3.SA", "RENT3.SA", "CCRO3.SA", "RAIL3.SA"],
    "Utilities|Brazil": ["ELET3.SA", "SBSP3.SA", "CMIG4.SA", "CPFE3.SA", "ENGI11.SA"],
    "Technology|Brazil": ["TOTS3.SA", "LWSA3.SA", "POSI3.SA", "INTB3.SA", "CASH3.SA"],
    "Communication Services|Brazil": ["VIVT3.SA", "TIMS3.SA", "DESK3.SA"],

    # ── LATAM (México, Chile, Argentina, Colômbia) ──
    "Industrials|Mexico": ["CEMEXCPO.MX", "GMEXICOB.MX", "ORIKIAB.MX"],
    "Financial Services|Mexico": ["GFNORTEO.MX", "GFINBURO.MX"],
    "Consumer Defensive|Chile": ["FALABELLA.SN", "CENCOSUD.SN", "CCU.SN"],
    "Energy|Argentina": ["YPF", "CEPU", "PAM"],
    "Energy|Colombia": ["EC", "CNEC.CL"],

    # ── Europa ──
    "Energy|United Kingdom": ["SHEL.L", "BP.L", "SSE.L"],
    "Technology|Germany": ["SAP.DE", "IFX.DE", "AIXA.DE"],
    "Consumer Defensive|Switzerland": ["NESN.SW", "NOVN.SW"],
    "Healthcare|France": ["SAN.PA", "AI.PA"],
    "Industrials|Sweden": ["VOLV-B.ST", "SAND.ST", "ATCO-A.ST"],

    # ── Ásia ──
    "Technology|India": ["TCS.NS", "INFY.NS", "WIPRO.NS", "HCLTECH.NS"],
    "Financial Services|India": ["HDFCBANK.NS", "ICICIBANK.NS", "SBIN.NS"],
    "Technology|Japan": ["6758.T", "6501.T", "7203.T"],
    "Technology|South Korea": ["005930.KS", "000660.KS"],
    "Technology|Taiwan": ["2330.TW", "2317.TW", "2454.TW"],
    "Technology|China": ["BABA", "JD", "BIDU", "PDD", "TCEHY"],

    # ── Oceania / África ──
    "Basic Materials|Australia": ["BHP.AX", "RIO.AX", "FMG.AX"],
    "Financial Services|South Africa": ["FSR.JO", "SBK.JO"],
}

# Campos a comparar: (nome_db, nome_yfinance_income/bs/cf, statement_type)
FIELDS_TO_COMPARE = [
    # Income Statement
    ("total_revenue",      "Total Revenue",          "income"),
    ("ebitda",             "EBITDA",                  "income"),
    ("ebit",               "EBIT",                    "income"),
    ("net_income",         "Net Income",              "income"),
    ("operating_income",   "Operating Income",        "income"),
    ("gross_profit",       "Gross Profit",            "income"),
    ("interest_expense",   "Interest Expense",        "income"),
    # Cash Flow
    ("free_cash_flow",     "Free Cash Flow",          "cashflow"),
    ("operating_cash_flow","Operating Cash Flow",      "cashflow"),
    ("capital_expenditure","Capital Expenditure",      "cashflow"),
    # Balance Sheet
    ("total_debt",         "Total Debt",              "balance"),
    ("total_assets",       "Total Assets",            "balance"),
    ("stockholders_equity","Stockholders Equity",      "balance"),
    ("cash_and_equivalents","Cash And Cash Equivalents","balance"),
    ("current_assets",     "Current Assets",          "balance"),
    ("current_liabilities","Current Liabilities",      "balance"),
]

# ══════════════════════════════════════════════════════════════════════════════
# Funções auxiliares
# ══════════════════════════════════════════════════════════════════════════════

def get_db_data(yahoo_code: str) -> list[dict]:
    """Retorna todos os registros anuais de uma empresa no DB."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT cfh.*, cbd.yahoo_sector, cbd.yahoo_industry, cbd.yahoo_country
        FROM company_financials_historical cfh
        JOIN company_basic_data cbd ON cbd.id = cfh.company_basic_data_id
        WHERE cfh.yahoo_code = ? AND cfh.period_type = 'annual'
        ORDER BY cfh.fiscal_year
    """, (yahoo_code,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_yf_data(yahoo_code: str) -> dict:
    """Busca dados anuais do Yahoo Finance ao vivo."""
    try:
        ticker = yf.Ticker(yahoo_code)
        income = ticker.income_stmt
        cashflow = ticker.cash_flow
        balance = ticker.balance_sheet
        info = ticker.get_info()
        return {
            "income": income,
            "cashflow": cashflow,
            "balance": balance,
            "info": info,
        }
    except Exception as e:
        print(f"  ERRO ao buscar {yahoo_code} no Yahoo: {e}")
        return None


def safe_val(df, field, col):
    """Extrai valor de um DataFrame do yfinance de forma segura."""
    if df is None or df.empty:
        return None
    if col not in df.columns:
        return None
    if field not in df.index:
        return None
    val = df.loc[field, col]
    if pd.isna(val):
        return None
    return float(val)


def pct_diff(db_val, yf_val):
    """Calcula % de diferença. Retorna None se impossível."""
    if db_val is None or yf_val is None:
        return None
    if yf_val == 0 and db_val == 0:
        return 0.0
    if yf_val == 0:
        return None  # divisão por zero
    return ((db_val - yf_val) / abs(yf_val)) * 100


def sign_match(db_val, yf_val):
    """Verifica se sinais são iguais."""
    if db_val is None or yf_val is None:
        return None
    if db_val == 0 or yf_val == 0:
        return True
    return (db_val > 0) == (yf_val > 0)


# ══════════════════════════════════════════════════════════════════════════════
# Comparação de um ticker
# ══════════════════════════════════════════════════════════════════════════════

def compare_ticker(yahoo_code: str) -> list[dict]:
    """Compara dados do DB com Yahoo Finance para um ticker. Retorna lista de comparações."""
    db_records = get_db_data(yahoo_code)
    if not db_records:
        return [{"yahoo_code": yahoo_code, "status": "NO_DB_DATA"}]

    yf_data = get_yf_data(yahoo_code)
    if yf_data is None:
        return [{"yahoo_code": yahoo_code, "status": "YF_FETCH_ERROR"}]

    results = []
    income_df = yf_data["income"]
    cashflow_df = yf_data["cashflow"]
    balance_df = yf_data["balance"]

    # Mapear colunas do YF por ano fiscal
    yf_years = {}
    if income_df is not None and not income_df.empty:
        for col in income_df.columns:
            yf_years[col.year] = col

    for db_rec in db_records:
        fy = db_rec["fiscal_year"]
        if fy not in yf_years:
            continue  # Ano não disponível no YF live

        yf_col = yf_years[fy]

        for db_field, yf_field, stmt_type in FIELDS_TO_COMPARE:
            db_val = db_rec.get(db_field)

            if stmt_type == "income":
                yf_val = safe_val(income_df, yf_field, yf_col)
            elif stmt_type == "cashflow":
                yf_val = safe_val(cashflow_df, yf_field, yf_col)
            elif stmt_type == "balance":
                yf_val = safe_val(balance_df, yf_field, yf_col)
            else:
                yf_val = None

            diff = pct_diff(db_val, yf_val)
            sign_ok = sign_match(db_val, yf_val)

            # Classificar status
            if db_val is None and yf_val is None:
                status = "BOTH_NULL"
            elif db_val is None and yf_val is not None:
                status = "MISSING_DB"
            elif db_val is not None and yf_val is None:
                status = "MISSING_YF"
            elif diff is not None and abs(diff) < 1.0:
                status = "MATCH"
            elif diff is not None and abs(diff) < 5.0:
                status = "CLOSE"
            elif diff is not None and abs(diff) < 20.0:
                status = "DEVIATION"
            elif sign_ok is False:
                status = "SIGN_MISMATCH"
            elif diff is not None:
                status = "LARGE_DEVIATION"
            else:
                status = "UNKNOWN"

            results.append({
                "yahoo_code": yahoo_code,
                "sector": db_rec.get("yahoo_sector", ""),
                "country": db_rec.get("yahoo_country", ""),
                "fiscal_year": fy,
                "field": db_field,
                "db_value": db_val,
                "yf_value": yf_val,
                "pct_diff": round(diff, 2) if diff is not None else None,
                "sign_match": sign_ok,
                "status": status,
                "currency": db_rec.get("original_currency", ""),
            })

    if not results:
        return [{"yahoo_code": yahoo_code, "status": "NO_MATCHING_YEARS"}]

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Análise de outliers (z-score)
# ══════════════════════════════════════════════════════════════════════════════

def analyze_outliers(all_results: list[dict], z_threshold: float = 3.0) -> pd.DataFrame:
    """Identifica outliers usando z-score por campo."""
    df = pd.DataFrame(all_results)
    df = df[df["pct_diff"].notna()].copy()

    if df.empty:
        return pd.DataFrame()

    # z-score por campo
    outliers = []
    for field, group in df.groupby("field"):
        if len(group) < 3:
            continue
        mean = group["pct_diff"].mean()
        std = group["pct_diff"].std()
        if std == 0:
            continue
        group = group.copy()
        group["z_score"] = (group["pct_diff"] - mean) / std
        field_outliers = group[group["z_score"].abs() > z_threshold]
        outliers.append(field_outliers)

    if not outliers:
        return pd.DataFrame()

    return pd.concat(outliers).sort_values("z_score", key=abs, ascending=False)


# ══════════════════════════════════════════════════════════════════════════════
# Relatório
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(all_results: list[dict], outlier_df: pd.DataFrame):
    """Imprime resumo do teste no console."""
    df = pd.DataFrame(all_results)

    print("\n" + "=" * 80)
    print("  RELATÓRIO DE QUALIDADE DE DADOS — EstudoAnloc")
    print(f"  Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Tickers testados
    tickers = df["yahoo_code"].unique()
    print(f"\n📊 Tickers testados: {len(tickers)}")

    # Status geral
    errors = df[df["status"].isin(["NO_DB_DATA", "YF_FETCH_ERROR", "NO_MATCHING_YEARS"])]
    if not errors.empty:
        print(f"\n⚠ Tickers com problemas:")
        for _, row in errors.drop_duplicates("yahoo_code").iterrows():
            print(f"  {row['yahoo_code']}: {row['status']}")

    # Apenas comparações válidas
    valid = df[df["pct_diff"].notna()]
    if valid.empty:
        print("\nNenhuma comparação válida encontrada!")
        return

    print(f"\n📈 Comparações válidas: {len(valid)}")

    # Distribuição de status
    print("\n─── Status das Comparações ───")
    status_counts = df["status"].value_counts()
    for status, count in status_counts.items():
        pct = count / len(df) * 100
        icon = {"MATCH": "✅", "CLOSE": "🟢", "DEVIATION": "🟡",
                "LARGE_DEVIATION": "🔴", "SIGN_MISMATCH": "⛔",
                "MISSING_DB": "📭", "MISSING_YF": "📬",
                "BOTH_NULL": "⬜"}.get(status, "❓")
        print(f"  {icon} {status}: {count} ({pct:.1f}%)")

    # Precisão por campo
    print("\n─── Precisão por Campo (mediana |%diff|) ───")
    field_stats = valid.groupby("field")["pct_diff"].agg(
        ["median", "mean", "std", "count"]
    ).sort_values("median", key=abs)

    for field, row in field_stats.iterrows():
        med = abs(row["median"])
        icon = "✅" if med < 1 else ("🟡" if med < 5 else "🔴")
        print(f"  {icon} {field:30s}  mediana={row['median']:+.2f}%  "
              f"média={row['mean']:+.2f}%  σ={row['std']:.2f}%  n={int(row['count'])}")

    # Precisão por país
    print("\n─── Precisão por País (mediana |%diff|) ───")
    country_stats = valid.groupby("country")["pct_diff"].agg(
        ["median", "mean", "count"]
    ).sort_values("median", key=abs)
    for country, row in country_stats.iterrows():
        med = abs(row["median"])
        icon = "✅" if med < 1 else ("🟡" if med < 5 else "🔴")
        print(f"  {icon} {country:25s}  mediana={row['median']:+.2f}%  n={int(row['count'])}")

    # Precisão por setor
    print("\n─── Precisão por Setor (mediana |%diff|) ───")
    sector_stats = valid.groupby("sector")["pct_diff"].agg(
        ["median", "mean", "count"]
    ).sort_values("median", key=abs)
    for sector, row in sector_stats.iterrows():
        med = abs(row["median"])
        icon = "✅" if med < 1 else ("🟡" if med < 5 else "🔴")
        print(f"  {icon} {sector:30s}  mediana={row['median']:+.2f}%  n={int(row['count'])}")

    # Outliers
    if not outlier_df.empty:
        print(f"\n─── TOP OUTLIERS (|z| > 3σ): {len(outlier_df)} encontrados ───")
        top = outlier_df.head(30)
        for _, row in top.iterrows():
            print(f"  🔴 {row['yahoo_code']:15s} {row['field']:25s} "
                  f"FY{int(row['fiscal_year'])}  DB={row['db_value']:.0f}  "
                  f"YF={row['yf_value']:.0f}  diff={row['pct_diff']:+.1f}%  "
                  f"z={row['z_score']:+.1f}")
    else:
        print("\n✅ Nenhum outlier estatístico (>3σ) encontrado!")

    # Sign mismatches
    sign_issues = valid[valid["sign_match"] == False]
    if not sign_issues.empty:
        print(f"\n─── SINAIS INVERTIDOS: {len(sign_issues)} ───")
        for _, row in sign_issues.head(20).iterrows():
            print(f"  ⛔ {row['yahoo_code']:15s} {row['field']:25s} "
                  f"FY{row['fiscal_year']}  DB={row['db_value']:.0f}  YF={row['yf_value']:.0f}")

    # Dados ausentes
    missing_db = df[df["status"] == "MISSING_DB"]
    if not missing_db.empty:
        print(f"\n─── CAMPOS AUSENTES NO DB (presentes no YF): {len(missing_db)} ───")
        missing_summary = missing_db.groupby("field").size().sort_values(ascending=False)
        for field, count in missing_summary.head(10).items():
            print(f"  📭 {field}: {count} ocorrências")

    print("\n" + "=" * 80)


# ══════════════════════════════════════════════════════════════════════════════
# Testes de consistência interna
# ══════════════════════════════════════════════════════════════════════════════

def test_internal_consistency() -> list[dict]:
    """Testa consistência interna do DB: múltiplos, sinais, completude."""
    conn = sqlite3.connect(str(DB_PATH))
    issues = []

    # 1. EV/EBITDA negativo ou absurdo
    rows = conn.execute("""
        SELECT yahoo_code, fiscal_year, enterprise_value_estimated, ebitda, ev_ebitda
        FROM company_financials_historical
        WHERE period_type = 'annual' AND ev_ebitda IS NOT NULL
          AND (ev_ebitda < 0 OR ev_ebitda > 200)
        LIMIT 50
    """).fetchall()
    for r in rows:
        issues.append({
            "type": "EXTREME_EV_EBITDA",
            "yahoo_code": r[0],
            "fiscal_year": r[1],
            "detail": f"EV/EBITDA={r[4]:.1f} (EV={r[2]:.0f}, EBITDA={r[3]:.0f})"
        })

    # 2. Revenue negativa
    rows = conn.execute("""
        SELECT yahoo_code, fiscal_year, total_revenue
        FROM company_financials_historical
        WHERE period_type = 'annual' AND total_revenue < 0
        LIMIT 30
    """).fetchall()
    for r in rows:
        issues.append({
            "type": "NEGATIVE_REVENUE",
            "yahoo_code": r[0],
            "fiscal_year": r[1],
            "detail": f"Revenue={r[2]:.0f}"
        })

    # 3. Market Cap estimado negativo
    rows = conn.execute("""
        SELECT yahoo_code, fiscal_year, market_cap_estimated
        FROM company_financials_historical
        WHERE period_type = 'annual' AND market_cap_estimated < 0
        LIMIT 30
    """).fetchall()
    for r in rows:
        issues.append({
            "type": "NEGATIVE_MCAP",
            "yahoo_code": r[0],
            "fiscal_year": r[1],
            "detail": f"MCap={r[2]:.0f}"
        })

    # 4. Margin > 100%
    rows = conn.execute("""
        SELECT yahoo_code, fiscal_year, ebitda_margin, net_margin
        FROM company_financials_historical
        WHERE period_type = 'annual'
          AND (ABS(ebitda_margin) > 1.5 OR ABS(net_margin) > 2.0)
        LIMIT 50
    """).fetchall()
    for r in rows:
        issues.append({
            "type": "EXTREME_MARGIN",
            "yahoo_code": r[0],
            "fiscal_year": r[1],
            "detail": f"EBITDA_margin={r[2]}, Net_margin={r[3]}"
        })

    # 5. FX rate = 1.0 para moeda diferente de USD
    rows = conn.execute("""
        SELECT yahoo_code, fiscal_year, original_currency, fx_rate_to_usd
        FROM company_financials_historical
        WHERE period_type = 'annual'
          AND original_currency IS NOT NULL AND original_currency != 'USD'
          AND fx_rate_to_usd = 1.0
        LIMIT 30
    """).fetchall()
    for r in rows:
        issues.append({
            "type": "FX_RATE_SUSPECT",
            "yahoo_code": r[0],
            "fiscal_year": r[1],
            "detail": f"Currency={r[2]}, FX=1.0 (suspeito)"
        })

    conn.close()
    return issues


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Teste de qualidade de dados EstudoAnloc")
    parser.add_argument("--quick", action="store_true", help="Amostra reduzida (2 por grupo)")
    parser.add_argument("--ticker", type=str, help="Testar ticker específico")
    parser.add_argument("--output", type=str, help="Nome base para CSV de saída")
    parser.add_argument("--z-threshold", type=float, default=3.0, help="Z-score threshold para outliers")
    parser.add_argument("--skip-yf", action="store_true", help="Pular comparação com Yahoo (só consistência interna)")
    args = parser.parse_args()

    all_results = []

    # ── Fase 1: Consistência interna ──
    print("\n" + "=" * 80)
    print("  FASE 1: CONSISTÊNCIA INTERNA DO DB")
    print("=" * 80)
    internal_issues = test_internal_consistency()
    if internal_issues:
        print(f"\n⚠ {len(internal_issues)} problemas de consistência encontrados:")
        by_type = {}
        for issue in internal_issues:
            by_type.setdefault(issue["type"], []).append(issue)
        for itype, items in sorted(by_type.items()):
            print(f"\n  [{itype}] ({len(items)} ocorrências)")
            for item in items[:5]:
                print(f"    {item['yahoo_code']} FY{item['fiscal_year']}: {item['detail']}")
            if len(items) > 5:
                print(f"    ... e mais {len(items) - 5}")
    else:
        print("\n✅ Nenhum problema de consistência interna encontrado!")

    if args.skip_yf:
        return

    # ── Fase 2: Comparação com Yahoo Finance ──
    print("\n" + "=" * 80)
    print("  FASE 2: COMPARAÇÃO DB vs YAHOO FINANCE (ao vivo)")
    print("=" * 80)

    # Montar lista de tickers
    if args.ticker:
        tickers_to_test = [args.ticker]
    else:
        tickers_to_test = []
        for group_key, group_tickers in SAMPLE_TICKERS.items():
            n = 2 if args.quick else len(group_tickers)
            tickers_to_test.extend(group_tickers[:n])

    total = len(tickers_to_test)
    print(f"\nTickers para testar: {total}")
    start = time.time()

    for i, ticker in enumerate(tickers_to_test, 1):
        elapsed = time.time() - start
        rps = i / elapsed if elapsed > 0 else 0
        eta = (total - i) / rps / 60 if rps > 0 else 0
        print(f"\n[{i}/{total}] {ticker} ({rps:.1f} t/s, ETA {eta:.0f}min)...")

        try:
            results = compare_ticker(ticker)
            all_results.extend(results)

            # Mini-resumo inline
            valid = [r for r in results if r.get("pct_diff") is not None]
            if valid:
                diffs = [abs(r["pct_diff"]) for r in valid]
                med = np.median(diffs)
                icon = "✅" if med < 1 else ("🟡" if med < 5 else "🔴")
                n_match = sum(1 for r in valid if abs(r["pct_diff"]) < 1)
                print(f"  {icon} {len(valid)} comparações, mediana |diff|={med:.1f}%, "
                      f"match(<1%)={n_match}/{len(valid)}")
            else:
                status = results[0].get("status", "?") if results else "?"
                print(f"  ⚠ {status}")
        except Exception as e:
            print(f"  💥 ERRO: {e}")
            all_results.append({"yahoo_code": ticker, "status": f"EXCEPTION: {e}"})

        # Rate limit: ~1 req/s
        time.sleep(1.0)

    # ── Fase 3: Análise de outliers ──
    print("\n" + "=" * 80)
    print(f"  FASE 3: ANÁLISE DE OUTLIERS (z > {args.z_threshold}σ)")
    print("=" * 80)

    outlier_df = analyze_outliers(all_results, z_threshold=args.z_threshold)

    # ── Fase 4: Relatório ──
    print_summary(all_results, outlier_df)

    # ── Salvar CSV ──
    if args.output:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = args.output

        # CSV principal
        df = pd.DataFrame(all_results)
        csv_path = CACHE_DIR / f"{base}_{ts}.csv"
        df.to_csv(str(csv_path), index=False, encoding="utf-8-sig")
        print(f"\n💾 Resultados salvos em: {csv_path}")

        # CSV outliers
        if not outlier_df.empty:
            outlier_path = CACHE_DIR / f"{base}_outliers_{ts}.csv"
            outlier_df.to_csv(str(outlier_path), index=False, encoding="utf-8-sig")
            print(f"💾 Outliers salvos em: {outlier_path}")

        # CSV consistência
        if internal_issues:
            issues_df = pd.DataFrame(internal_issues)
            issues_path = CACHE_DIR / f"{base}_consistency_{ts}.csv"
            issues_df.to_csv(str(issues_path), index=False, encoding="utf-8-sig")
            print(f"💾 Consistência salva em: {issues_path}")


if __name__ == "__main__":
    main()
