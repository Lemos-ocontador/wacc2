"""
fetch_historical_financials.py
==============================
Busca dados financeiros históricos via yfinance e armazena no SQLite.

Dados buscados:
  - Income Statement (anual + trimestral): Revenue, EBIT, EBITDA, Net Income, etc.
  - Cash Flow: FCF, Operating Cash Flow, Capex
  - Balance Sheet: Total Debt, Equity, Assets, Cash
  - Statistics correntes: EV, EV/Revenue, EV/EBITDA
  - Preço histórico para calcular Market Cap histórico
  - EV histórico estimado: Market Cap + Total Debt - Cash

Uso:
  python scripts/fetch_historical_financials.py --sector "Technology" --limit 100
  python scripts/fetch_historical_financials.py --industry "Biotechnology" --limit 50
  python scripts/fetch_historical_financials.py --country "Brazil" --workers 4
  python scripts/fetch_historical_financials.py --company AAPL
  python scripts/fetch_historical_financials.py --sector "Technology" --quarterly
  python scripts/fetch_historical_financials.py --force  # re-busca mesmo se existir
"""

import argparse
import os
import sys
import time
import random
import sqlite3
import threading
import logging
import warnings
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Garante import do yfinance com SSL ok
os.environ.setdefault("CURL_CA_BUNDLE", r"C:\cacerts\cacert.pem")

import yfinance as yf
from yfinance.exceptions import YFRateLimitError
from yfinance.data import YfData
import numpy as np
import pandas as pd

# Silenciar warnings de depreciação do Pandas (Timestamp.utcnow) no yfinance
warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")
warnings.filterwarnings("ignore", message=".*Timestamp.utcnow.*")

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("hist_fin")

# --------------------------------------------------------------------------
# Rate Limiter
# --------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, max_per_second: float):
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last = 0.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            # Jitter aleatório: 0.5x a 2x do intervalo mínimo
            jitter = self._min_interval * random.uniform(0.5, 2.0)
            wait = jitter - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


_rate_limiter: RateLimiter | None = None
_rate_limited = threading.Event()
_RATE_LIMIT_PAUSE = 60  # 1 minuto (com cookies limpos + curl_cffi é suficiente)
_reset_lock = threading.Lock()
_request_counter = 0
_counter_lock = threading.Lock()
_PROACTIVE_RESET_EVERY = 400  # resetar sessão a cada N requests (antes do rate limit)


def _reset_yf_session():
    with _reset_lock:
        if YfData in YfData._instances:
            del YfData._instances[YfData]


# --------------------------------------------------------------------------
# Conversão de moeda (cache com série histórica)
# --------------------------------------------------------------------------
_fx_cache: dict[str, pd.DataFrame] = {}
_fx_lock = threading.Lock()


def _get_fx_series(currency: str) -> pd.DataFrame:
    """Retorna série histórica FX para USD. Usa cache thread-safe."""
    if not currency or currency == "USD":
        return pd.DataFrame()
    with _fx_lock:
        if currency in _fx_cache:
            return _fx_cache[currency]
    # Busca via yfinance
    try:
        pair = f"{currency}USD=X"
        ticker = yf.Ticker(pair)
        hist = ticker.history(period="10y", interval="1d")
        if hist.empty:
            log.warning(f"Sem cotação histórica para {pair}")
            hist = pd.DataFrame()
    except Exception as e:
        log.warning(f"Erro buscando FX {currency}: {e}")
        hist = pd.DataFrame()
    with _fx_lock:
        _fx_cache[currency] = hist
    return hist


def _get_historical_fx_rates(currency: str, period_dates: list[str]) -> dict[str, float]:
    """Retorna taxa FX para USD mais próxima de cada data de período."""
    if not currency or currency == "USD":
        return {d: 1.0 for d in period_dates}
    fx_hist = _get_fx_series(currency)
    if fx_hist.empty:
        return {d: 1.0 for d in period_dates}
    tz = fx_hist.index.tz
    result = {}
    for date_str in period_dates:
        pd_date = pd.Timestamp(date_str)
        if tz is not None:
            pd_date = pd_date.tz_localize(tz)
        closest_idx = fx_hist.index.get_indexer([pd_date], method="nearest")[0]
        if 0 <= closest_idx < len(fx_hist):
            result[date_str] = float(fx_hist.iloc[closest_idx]["Close"])
        else:
            result[date_str] = 1.0
    return result


# --------------------------------------------------------------------------
# DB Setup
# --------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "damodaran_data_new.db"


def ensure_table(db_path: Path):
    """Cria tabela company_financials_historical se não existir."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS company_financials_historical (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_basic_data_id INTEGER NOT NULL,
        yahoo_code TEXT NOT NULL,
        company_name TEXT,
        period_type TEXT NOT NULL CHECK(period_type IN ('annual','quarterly')),
        period_date TEXT NOT NULL,
        fiscal_year INTEGER,
        fiscal_quarter INTEGER,

        -- Income Statement
        total_revenue REAL,
        cost_of_revenue REAL,
        gross_profit REAL,
        operating_income REAL,
        operating_expense REAL,
        ebit REAL,
        ebitda REAL,
        normalized_ebitda REAL,
        net_income REAL,
        interest_expense REAL,
        tax_provision REAL,
        research_and_development REAL,
        sga REAL,
        diluted_average_shares REAL,

        -- Cash Flow
        free_cash_flow REAL,
        operating_cash_flow REAL,
        capital_expenditure REAL,

        -- Balance Sheet
        total_assets REAL,
        total_debt REAL,
        stockholders_equity REAL,
        total_liabilities REAL,
        cash_and_equivalents REAL,
        short_term_debt REAL,
        long_term_debt REAL,
        short_term_investments REAL,
        current_assets REAL,
        current_liabilities REAL,
        ordinary_shares_number REAL,
        preferred_stock REAL,
        minority_interest REAL,

        -- Market / EV
        close_price REAL,
        market_cap_estimated REAL,
        enterprise_value_estimated REAL,

        -- Moeda e conversão
        original_currency TEXT,
        fx_rate_to_usd REAL,

        -- Valores convertidos em USD
        total_revenue_usd REAL,
        ebit_usd REAL,
        ebitda_usd REAL,
        net_income_usd REAL,
        free_cash_flow_usd REAL,
        enterprise_value_usd REAL,

        -- Indicadores calculados
        ebit_margin REAL,
        ebitda_margin REAL,
        gross_margin REAL,
        net_margin REAL,
        fcf_revenue_ratio REAL,
        fcf_ebitda_ratio REAL,
        debt_equity REAL,
        debt_ebitda REAL,
        capex_revenue REAL,
        ev_revenue REAL,
        ev_ebitda REAL,
        ev_ebit REAL,

        -- Metadata
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_quality TEXT DEFAULT 'ok',

        UNIQUE(company_basic_data_id, period_type, period_date)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cfh_yahoo ON company_financials_historical(yahoo_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cfh_period ON company_financials_historical(period_type, fiscal_year)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cfh_company ON company_financials_historical(company_basic_data_id)")
    # Migration: adicionar colunas novas se tabela já existia
    for col_name in ('ordinary_shares_number', 'preferred_stock', 'minority_interest',
                      'diluted_average_shares', 'short_term_debt', 'long_term_debt',
                      'close_price',
                      'total_revenue_ttm', 'ebitda_ttm', 'ebit_ttm',
                      'free_cash_flow_ttm', 'net_income_ttm'):
        try:
            conn.execute(f"ALTER TABLE company_financials_historical ADD COLUMN {col_name} REAL")
            log.info(f"Coluna '{col_name}' adicionada.")
        except sqlite3.OperationalError:
            pass  # coluna já existe
    conn.commit()
    conn.close()
    log.info("Tabela company_financials_historical verificada/criada.")


# --------------------------------------------------------------------------
# Busca de empresas alvo
# --------------------------------------------------------------------------
def get_target_companies(db_path: Path, args) -> list[dict]:
    """Retorna lista de empresas para buscar, com base nos filtros."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    base_query = """
        SELECT cbd.id, cbd.yahoo_code, cbd.company_name, cbd.yahoo_sector,
               cbd.yahoo_industry, cbd.yahoo_country, cbd.currency
        FROM company_basic_data cbd
        WHERE cbd.yahoo_code IS NOT NULL AND cbd.yahoo_code != ''
    """
    params = []

    if args.company:
        base_query += " AND (cbd.yahoo_code = ? OR cbd.ticker LIKE ?)"
        params.extend([args.company, f"%{args.company}%"])
    else:
        if args.sector:
            base_query += " AND cbd.yahoo_sector = ?"
            params.append(args.sector)
        if args.industry:
            base_query += " AND cbd.yahoo_industry = ?"
            params.append(args.industry)
        if args.country:
            base_query += " AND cbd.yahoo_country = ?"
            params.append(args.country)

    if not args.force:
        period_type = "quarterly" if args.quarterly else "annual"
        base_query += f"""
            AND cbd.id NOT IN (
                SELECT DISTINCT company_basic_data_id
                FROM company_financials_historical
                WHERE period_type = ?
            )
        """
        params.append(period_type)

    base_query += " ORDER BY cbd.id"
    if args.limit:
        base_query += " LIMIT ?"
        params.append(args.limit)

    rows = conn.execute(base_query, params).fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result


# --------------------------------------------------------------------------
# Fetch financials de uma empresa
# --------------------------------------------------------------------------
def _safe_value(series, field_name):
    """Extrai valor de um DataFrame row de forma segura."""
    if field_name not in series.index:
        return None
    val = series[field_name]
    if pd.isna(val):
        return None
    return float(val)


def _clamp_ratio(numerator, denominator, limit=100):
    """Calcula ratio com clamp: retorna None se numerador é None ou |resultado| > limit."""
    if numerator is None:
        return None
    ratio = numerator / denominator
    if abs(ratio) > limit:
        return None
    return ratio


def _get_historical_prices(ticker_obj, periods: list[str]) -> dict[str, float | None]:
    """Busca preço de fechamento nas datas dos períodos para calcular market cap."""
    result = {}
    try:
        hist = ticker_obj.history(period="10y", interval="1d")
        if hist.empty:
            return result
        # Resolver timezone: hist.index é tz-aware, periods são naive
        tz = hist.index.tz
        for period_date_str in periods:
            pd_date = pd.Timestamp(period_date_str)
            if tz is not None:
                pd_date = pd_date.tz_localize(tz)
            closest_idx = hist.index.get_indexer([pd_date], method="nearest")[0]
            if 0 <= closest_idx < len(hist):
                result[period_date_str] = float(hist.iloc[closest_idx]["Close"])
    except Exception:
        pass
    return result


def fetch_company_financials(company: dict, quarterly: bool = False) -> dict | str | None:
    """Busca dados financeiros históricos de uma empresa via yfinance."""
    if _rate_limited.is_set():
        _rate_limited.wait()
    if _rate_limiter:
        _rate_limiter.acquire()

    # Reset proativo de sessão a cada N requests
    global _request_counter
    with _counter_lock:
        _request_counter += 1
        if _request_counter % _PROACTIVE_RESET_EVERY == 0:
            _reset_yf_session()
            _clear_yf_cookies()
            log.info(f"Reset proativo de sessão ({_request_counter} requests)")

    yahoo_code = company["yahoo_code"]
    try:
        ticker = yf.Ticker(yahoo_code)
    except Exception as e:
        log.debug(f"Erro criando ticker {yahoo_code}: {e}")
        return None

    # Buscar info para shares outstanding e moeda
    try:
        info = ticker.get_info()
    except YFRateLimitError:
        return "RATE_LIMITED"
    except Exception:
        info = {}

    qt = info.get("quoteType", "")
    if qt in ("NONE", ""):
        return None

    shares_outstanding_current = info.get("sharesOutstanding")  # fallback se balance_sheet não tiver
    financial_currency = info.get("financialCurrency") or info.get("currency")
    trading_currency = info.get("currency")  # moeda de negociação (preço)

    # Buscar financial statements
    try:
        if quarterly:
            income_df = ticker.quarterly_income_stmt
            cashflow_df = ticker.quarterly_cash_flow
            balance_df = ticker.quarterly_balance_sheet
        else:
            income_df = ticker.income_stmt
            cashflow_df = ticker.cash_flow
            balance_df = ticker.balance_sheet
    except YFRateLimitError:
        return "RATE_LIMITED"
    except Exception as e:
        log.debug(f"Erro buscando statements {yahoo_code}: {e}")
        return None

    if income_df is None or income_df.empty:
        return None

    # Preços históricos para market cap (diário para precisão)
    period_dates = [str(c.date()) for c in income_df.columns]
    hist_prices = _get_historical_prices(ticker, period_dates)

    # Taxas de câmbio históricas por período
    fx_rates = _get_historical_fx_rates(financial_currency, period_dates) if financial_currency else {d: 1.0 for d in period_dates}

    # Taxas de conversão preço→financialCurrency (quando moedas diferem)
    # Ex: PGAS.JK negocia em IDR mas reporta em USD → precisa converter MCap de IDR→USD
    price_to_fin_rates = {}
    if trading_currency and financial_currency and trading_currency != financial_currency:
        # Converter de trading_currency para financial_currency
        # Primeiro obter trading_currency→USD
        trading_fx = _get_historical_fx_rates(trading_currency, period_dates)
        # Depois obter financial_currency→USD (já temos)
        for d in period_dates:
            trad_to_usd = trading_fx.get(d, 1.0)
            fin_to_usd = fx_rates.get(d, 1.0)
            # trading→USD / financial→USD = trading→financial
            if fin_to_usd and fin_to_usd != 0:
                price_to_fin_rates[d] = trad_to_usd / fin_to_usd
            else:
                price_to_fin_rates[d] = 1.0
        log.debug(f"{yahoo_code}: trading={trading_currency} financial={financial_currency}, convertendo MCap")

    periods = []
    for col in income_df.columns:
        period_date = str(col.date())
        fiscal_year = col.year
        fiscal_quarter = (col.month - 1) // 3 + 1 if quarterly else None

        fx_rate = fx_rates.get(period_date, 1.0)
        rec = {
            "period_date": period_date,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "original_currency": financial_currency,
            "fx_rate_to_usd": fx_rate,
        }

        # Income Statement
        income_col = income_df[col] if col in income_df.columns else pd.Series()
        rec["total_revenue"] = _safe_value(income_col, "Total Revenue")
        rec["cost_of_revenue"] = _safe_value(income_col, "Cost Of Revenue")
        rec["gross_profit"] = _safe_value(income_col, "Gross Profit")
        rec["operating_income"] = _safe_value(income_col, "Operating Income")
        rec["operating_expense"] = _safe_value(income_col, "Operating Expense")
        rec["ebit"] = _safe_value(income_col, "EBIT")
        rec["ebitda"] = _safe_value(income_col, "EBITDA")
        rec["normalized_ebitda"] = _safe_value(income_col, "Normalized EBITDA")
        rec["net_income"] = _safe_value(income_col, "Net Income")
        rec["interest_expense"] = _safe_value(income_col, "Interest Expense")
        rec["tax_provision"] = _safe_value(income_col, "Tax Provision")
        rec["research_and_development"] = _safe_value(income_col, "Research And Development")
        rec["sga"] = _safe_value(income_col, "Selling General And Administration")
        rec["diluted_average_shares"] = _safe_value(income_col, "Diluted Average Shares")

        # Cash Flow
        if cashflow_df is not None and not cashflow_df.empty and col in cashflow_df.columns:
            cf_col = cashflow_df[col]
            rec["free_cash_flow"] = _safe_value(cf_col, "Free Cash Flow")
            rec["operating_cash_flow"] = _safe_value(cf_col, "Operating Cash Flow")
            rec["capital_expenditure"] = _safe_value(cf_col, "Capital Expenditure")

        # Balance Sheet
        if balance_df is not None and not balance_df.empty and col in balance_df.columns:
            bs_col = balance_df[col]
            rec["total_assets"] = _safe_value(bs_col, "Total Assets")
            rec["total_debt"] = _safe_value(bs_col, "Total Debt")
            rec["stockholders_equity"] = _safe_value(bs_col, "Stockholders Equity")
            rec["total_liabilities"] = _safe_value(bs_col, "Total Liabilities Net Minority Interest")
            rec["cash_and_equivalents"] = _safe_value(bs_col, "Cash And Cash Equivalents")
            rec["short_term_debt"] = _safe_value(bs_col, "Current Debt")
            rec["long_term_debt"] = _safe_value(bs_col, "Long Term Debt")
            rec["short_term_investments"] = _safe_value(bs_col, "Other Short Term Investments")
            rec["current_assets"] = _safe_value(bs_col, "Current Assets")
            rec["current_liabilities"] = _safe_value(bs_col, "Current Liabilities")
            rec["ordinary_shares_number"] = _safe_value(bs_col, "Ordinary Shares Number")
            rec["preferred_stock"] = _safe_value(bs_col, "Preferred Stock")
            rec["minority_interest"] = _safe_value(bs_col, "Minority Interest")

        # Market Cap: usar shares histórico do balance_sheet, fallback para atual
        price = hist_prices.get(period_date)
        rec["close_price"] = price
        shares = rec.get("ordinary_shares_number") or shares_outstanding_current
        if price and shares:
            mcap_raw = price * shares
            # Converter MCap da moeda de negociação para financialCurrency se diferem
            if price_to_fin_rates:
                conv_rate = price_to_fin_rates.get(period_date, 1.0)
                mcap_raw = mcap_raw * conv_rate
            # Guarda: se MCap/Receita > 50.000x com MCap > 1B, provável shares inflados
            rev_check = abs(rec.get("total_revenue") or 0)
            if rev_check > 0 and mcap_raw > 1e9 and (mcap_raw / rev_check) > 50000:
                rec["market_cap_estimated"] = None
            else:
                rec["market_cap_estimated"] = mcap_raw
        else:
            rec["market_cap_estimated"] = None

        # EV = Market Cap + Total Debt + Preferred Stock + Minority Interest - Cash
        mcap = rec.get("market_cap_estimated")
        debt = rec.get("total_debt") or 0
        pref = rec.get("preferred_stock") or 0
        mi = rec.get("minority_interest") or 0
        cash = rec.get("cash_and_equivalents") or 0
        if mcap:
            rec["enterprise_value_estimated"] = mcap + debt + pref + mi - cash
        else:
            rec["enterprise_value_estimated"] = None

        # Conversão USD
        rec["total_revenue_usd"] = rec["total_revenue"] * fx_rate if rec.get("total_revenue") else None
        rec["ebit_usd"] = rec["ebit"] * fx_rate if rec.get("ebit") else None
        rec["ebitda_usd"] = rec["ebitda"] * fx_rate if rec.get("ebitda") else None
        rec["net_income_usd"] = rec["net_income"] * fx_rate if rec.get("net_income") else None
        rec["free_cash_flow_usd"] = rec["free_cash_flow"] * fx_rate if rec.get("free_cash_flow") else None
        rec["enterprise_value_usd"] = rec["enterprise_value_estimated"] * fx_rate if rec.get("enterprise_value_estimated") else None

        # Indicadores calculados — com guardas para denominadores desprezíveis
        # Threshold: receita < $1.000 USD (ou equivalente) = receita desprezível
        rev = rec.get("total_revenue")
        rev_usd = abs(rev * fx_rate) if rev and fx_rate else 0
        rev_ok = rev and rev != 0 and rev_usd >= 1000
        # Threshold para múltiplos EV: receita mínima $100K USD para evitar
        # distorções em empresas pré-receita (IMSR ~$18K, ECNR.TA ~$2.6K, etc)
        rev_material = rev_ok and rev_usd >= 100_000

        if rev_ok:
            rec["ebit_margin"] = _clamp_ratio(rec.get("ebit"), rev)
            rec["ebitda_margin"] = _clamp_ratio(rec.get("ebitda"), rev)
            rec["gross_margin"] = _clamp_ratio(rec.get("gross_profit"), rev)
            rec["net_margin"] = _clamp_ratio(rec.get("net_income"), rev)
            rec["fcf_revenue_ratio"] = _clamp_ratio(rec.get("free_cash_flow"), rev)
            rec["capex_revenue"] = abs(rec["capital_expenditure"]) / rev if rec.get("capital_expenditure") is not None else None
        else:
            rec["ebit_margin"] = rec["ebitda_margin"] = rec["gross_margin"] = None
            rec["net_margin"] = rec["fcf_revenue_ratio"] = rec["capex_revenue"] = None

        ebitda_val = rec.get("ebitda")
        ebitda_usd = abs(ebitda_val * fx_rate) if ebitda_val and fx_rate else 0
        ebitda_ok = ebitda_val and ebitda_val != 0 and ebitda_usd >= 100

        if ebitda_ok:
            rec["fcf_ebitda_ratio"] = _clamp_ratio(rec.get("free_cash_flow"), ebitda_val)
            rec["debt_ebitda"] = _clamp_ratio(rec.get("total_debt"), ebitda_val)
        else:
            rec["fcf_ebitda_ratio"] = rec["debt_ebitda"] = None

        equity = rec.get("stockholders_equity")
        equity_usd = abs(equity * fx_rate) if equity and fx_rate else 0
        if equity and equity != 0 and equity_usd >= 100:
            rec["debt_equity"] = _clamp_ratio(rec.get("total_debt"), equity)
        else:
            rec["debt_equity"] = None

        ev = rec.get("enterprise_value_estimated")
        if ev and rev_material:
            rec["ev_revenue"] = _clamp_ratio(ev, rev, limit=500)
        else:
            rec["ev_revenue"] = None
        if ev and ebitda_ok:
            rec["ev_ebitda"] = _clamp_ratio(ev, ebitda_val, limit=500)
        else:
            rec["ev_ebitda"] = None

        ebit_val = rec.get("ebit")
        if ev and ebit_val and ebit_val > 0:
            rec["ev_ebit"] = _clamp_ratio(ev, ebit_val, limit=500)
        else:
            rec["ev_ebit"] = None

        periods.append(rec)

    return {"periods": periods, "currency": financial_currency}


# --------------------------------------------------------------------------
# Salvar no banco
# --------------------------------------------------------------------------
_db_lock = threading.Lock()

FIELDS = [
    "period_date", "fiscal_year", "fiscal_quarter",
    "total_revenue", "cost_of_revenue", "gross_profit", "operating_income",
    "operating_expense", "ebit", "ebitda", "normalized_ebitda", "net_income",
    "interest_expense", "tax_provision", "research_and_development", "sga",
    "diluted_average_shares",
    "free_cash_flow", "operating_cash_flow", "capital_expenditure",
    "total_assets", "total_debt", "stockholders_equity", "total_liabilities",
    "cash_and_equivalents", "short_term_debt", "long_term_debt",
    "short_term_investments", "current_assets", "current_liabilities",
    "ordinary_shares_number", "preferred_stock", "minority_interest",
    "close_price", "market_cap_estimated", "enterprise_value_estimated",
    "original_currency", "fx_rate_to_usd",
    "total_revenue_usd", "ebit_usd", "ebitda_usd", "net_income_usd",
    "free_cash_flow_usd", "enterprise_value_usd",
    "ebit_margin", "ebitda_margin", "gross_margin", "net_margin",
    "fcf_revenue_ratio", "fcf_ebitda_ratio", "debt_equity", "debt_ebitda",
    "capex_revenue", "ev_revenue", "ev_ebitda", "ev_ebit",
]


def save_financials(db_path: Path, company: dict, data: dict, period_type: str):
    """Salva dados históricos no banco. Thread-safe."""
    with _db_lock:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        for rec in data["periods"]:
            values = [
                company["id"],
                company["yahoo_code"],
                company.get("company_name"),
                period_type,
            ] + [rec.get(f) for f in FIELDS]

            placeholders = ", ".join(["?"] * (4 + len(FIELDS)))
            field_names = "company_basic_data_id, yahoo_code, company_name, period_type, " + ", ".join(FIELDS)

            cursor.execute(f"""
                INSERT OR REPLACE INTO company_financials_historical
                ({field_names})
                VALUES ({placeholders})
            """, values)
        conn.commit()
        conn.close()


# --------------------------------------------------------------------------
# Worker
# --------------------------------------------------------------------------
def process_company(company: dict, quarterly: bool) -> tuple[int, str, str]:
    """Processa uma empresa. Retorna (id, status, yahoo_code)."""
    yahoo_code = company["yahoo_code"]
    try:
        result = fetch_company_financials(company, quarterly=quarterly)
        if result == "RATE_LIMITED":
            return company["id"], "rate_limited", yahoo_code
        if result is None:
            return company["id"], "no_data", yahoo_code
        if not result.get("periods"):
            return company["id"], "empty", yahoo_code

        period_type = "quarterly" if quarterly else "annual"
        save_financials(DB_PATH, company, result, period_type)
        n = len(result["periods"])
        return company["id"], f"ok:{n}", yahoo_code
    except Exception as e:
        log.debug(f"Erro {yahoo_code}: {e}")
        return company["id"], "error", yahoo_code


# --------------------------------------------------------------------------
# Limpeza de cache de cookies do yfinance (resolve rate-limit persistente)
# --------------------------------------------------------------------------
def _clear_yf_cookies():
    """Deleta cookies.db do yfinance para resetar sessão limpa."""
    try:
        import platformdirs
        cache_dir = Path(platformdirs.user_cache_dir("py-yfinance"))
    except Exception:
        cache_dir = Path.home() / "AppData" / "Local" / "py-yfinance"
    for f in ("cookies.db", "cookies.db-shm", "cookies.db-wal"):
        p = cache_dir / f
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass
    log.info("Cache de cookies do yfinance limpo.")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    global _rate_limiter

    parser = argparse.ArgumentParser(description="Busca dados financeiros históricos via Yahoo Finance")
    parser.add_argument("--sector", type=str, help="Filtrar por Yahoo sector")
    parser.add_argument("--industry", type=str, help="Filtrar por Yahoo industry")
    parser.add_argument("--country", type=str, help="Filtrar por country")
    parser.add_argument("--company", type=str, help="Yahoo code ou ticker específico")
    parser.add_argument("--quarterly", action="store_true", help="Buscar dados trimestrais (default: anual)")
    parser.add_argument("--limit", type=int, default=None, help="Limite de empresas")
    parser.add_argument("--workers", type=int, default=3, help="Threads paralelas (default: 3)")
    parser.add_argument("--max-rps", type=float, default=2.0, help="Requests/segundo (default: 2)")
    parser.add_argument("--force", action="store_true", help="Re-busca mesmo se já existir")
    parser.add_argument("--db", type=str, default=None, help="Caminho do banco (opcional)")
    args = parser.parse_args()

    global DB_PATH
    if args.db:
        DB_PATH = Path(args.db)

    log.info(f"DB: {DB_PATH}")
    log.info(f"Filtros: sector={args.sector}, industry={args.industry}, country={args.country}, company={args.company}")
    log.info(f"Tipo: {'trimestral' if args.quarterly else 'anual'} | Workers: {args.workers} | Max RPS: {args.max_rps}")

    # Limpar cookies antes de iniciar
    _clear_yf_cookies()

    # Setup
    ensure_table(DB_PATH)
    _rate_limiter = RateLimiter(args.max_rps)

    # Buscar empresas alvo
    companies = get_target_companies(DB_PATH, args)
    if not companies:
        log.info("Nenhuma empresa encontrada com os filtros especificados (ou todas já têm dados).")
        return

    total = len(companies)
    log.info(f"Empresas para processar: {total}")

    # Estatísticas
    stats = {"ok": 0, "no_data": 0, "empty": 0, "error": 0, "rate_limited": 0, "periods_total": 0}
    start_time = time.time()
    rate_pauses = 0

    # Batch processing — processa em lotes pequenos, re-enfileira rate-limited
    batch_size = args.workers * 5
    idx = 0
    while idx < total:
        batch = companies[idx : idx + batch_size]
        retry_companies = []
        hit_rate_limit = False

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(process_company, comp, args.quarterly): comp
                for comp in batch
            }

            for future in as_completed(futures):
                comp = futures[future]
                try:
                    cid, status, ycode = future.result()
                except Exception as e:
                    log.error(f"Exceção não tratada para {comp['yahoo_code']}: {e}")
                    stats["error"] += 1
                    continue

                if status == "rate_limited":
                    stats["rate_limited"] += 1
                    hit_rate_limit = True
                    retry_companies.append(comp)
                    continue

                if status.startswith("ok:"):
                    n_periods = int(status.split(":")[1])
                    stats["ok"] += 1
                    stats["periods_total"] += n_periods
                elif status == "no_data":
                    stats["no_data"] += 1
                elif status == "empty":
                    stats["empty"] += 1
                else:
                    stats["error"] += 1

        if hit_rate_limit:
            rate_pauses += 1
            pause = _RATE_LIMIT_PAUSE * min(rate_pauses, 3)  # max 15min
            log.warning(f"Rate limit! Pausa de {pause//60}min (pausa #{rate_pauses})...")
            _clear_yf_cookies()
            _reset_yf_session()
            time.sleep(pause)
            log.info("Sessão resetada, retomando...")
            # Re-inserir rate-limited no início da fila restante
            remaining = retry_companies + companies[idx + batch_size:]
            companies = companies[:idx + batch_size] + remaining[len(retry_companies):]
            # Re-inserir retry no próximo batch (não avançar idx por completo)
            companies[idx + batch_size - len(retry_companies):idx + batch_size] = retry_companies
            total = len(companies)
            # Avançar apenas pelas empresas que NÃO falharam
            idx += batch_size - len(retry_companies)
            continue
        else:
            if rate_pauses > 0:
                rate_pauses = 0

        idx += len(batch)

        # Progress
        elapsed = time.time() - start_time
        processed = stats["ok"] + stats["no_data"] + stats["empty"] + stats["error"]
        rps = processed / elapsed if elapsed > 0 else 0
        remaining_time = (total - idx) / rps if rps > 0 else 0
        pct = idx / total * 100
        log.info(
            f"[{idx}/{total}] {pct:.0f}% | "
            f"OK:{stats['ok']} Sem dados:{stats['no_data']} Erros:{stats['error']} "
            f"Periodos:{stats['periods_total']} | {rps:.1f} emp/s | "
            f"ETA {remaining_time/60:.0f}min"
        )

    # Resumo final
    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info(f"CONCLUÍDO em {elapsed:.0f}s ({elapsed/60:.1f} min)")
    log.info(f"  OK (com dados): {stats['ok']}")
    log.info(f"  Sem dados financeiros: {stats['no_data']}")
    log.info(f"  Vazios: {stats['empty']}")
    log.info(f"  Erros: {stats['error']}")
    log.info(f"  Rate limits: {stats['rate_limited']}")
    log.info(f"  Total períodos gravados: {stats['periods_total']}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
