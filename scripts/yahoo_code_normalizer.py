from __future__ import annotations

import re


EXCHANGE_SUFFIX_MAP: dict[str, str] = {
    "NASDAQGS": "",
    "NASDAQCM": "",
    "NASDAQGM": "",
    "NYSE": "",
    "NYSEARCA": "",
    "NYSEAMERICAN": "",
    "BATS": "",
    "OTC": "",
    "SASE": ".SR",
    "TWSE": ".TW",
    "TWO": ".TWO",
    "SEHK": ".HK",
    "ASX": ".AX",
    "NSE": ".NS",
    "NSEI": ".NS",
    "BSE": ".BO",
    "SHSE": ".SS",
    "SSE": ".SS",
    "SZSE": ".SZ",
    "BOVESPA": ".SA",
    "B3": ".SA",
    "BMV": ".MX",
    "KOSE": ".KS",
    "TSX": ".TO",
    "TPEX": ".TWO",
    "TSXV": ".V",
    "LSE": ".L",
    "KLSE": ".KL",
    "SET": ".BK",
    "IDX": ".JK",
    "BVB": ".RO",
    "XTRA": ".DE",
    "ENXTPA": ".PA",
    "FWB": ".DE",
    "XETR": ".DE",
    "EPA": ".PA",
    "BIT": ".MI",
    "BME": ".MC",
    "AMS": ".AS",
    "SWX": ".SW",
    "SIX": ".SW",
    "STO": ".ST",
    "CPH": ".CO",
    "HEL": ".HE",
    "OSL": ".OL",
    "WSE": ".WA",
    "PRA": ".PR",
    "KRX": ".KS",
    "KOSPI": ".KS",
    "KOSDAQ": ".KQ",
    "TASE": ".TA",
    "TA": ".TA",
    "TYO": ".T",
    "TSE": ".T",
    "SGX": ".SI",
    "JSE": ".JO",
}


def _split_exchange_ticker(raw_ticker: str | None) -> tuple[str | None, str | None]:
    if not raw_ticker:
        return None, None
    ticker = str(raw_ticker).strip()
    if not ticker:
        return None, None

    if ":" in ticker:
        exchange, code = ticker.split(":", 1)
        return exchange.strip().upper(), code.strip()

    return None, ticker


def _normalize_token(token: str | None) -> str | None:
    if token is None:
        return None
    out = str(token).strip().upper()
    if not out:
        return None
    out = re.sub(r"\s+", "", out)
    return out


def _pad_numeric_code(code: str, width: int = 4) -> str:
    return code.zfill(width) if code.isdigit() else code


def _strip_market_prefix(exchange: str | None, code: str) -> str:
    if not code:
        return code

    if exchange in {"KOSDAQ", "KOSE", "KOSPI", "KRX"}:
        if code.startswith("A") and code[1:].isdigit():
            return code[1:]

    return code


def _market_specific_adjustments(exchange: str | None, code: str) -> str:
    code = _strip_market_prefix(exchange, code)

    if exchange in {"SEHK", "SASE", "TWSE", "TWO", "TPEX"}:
        return _pad_numeric_code(code, width=4)

    if exchange in {"SHSE", "SSE", "SZSE"}:
        return _pad_numeric_code(code, width=6)

    if exchange in {"BSE"}:
        return _pad_numeric_code(code, width=6)

    return code


def normalize_yahoo_code(yahoo_code: str | None, ticker: str | None = None) -> str | None:
    exchange, ticker_code = _split_exchange_ticker(ticker)
    yc = _normalize_token(yahoo_code)

    base = yc
    if not base and ticker_code:
        base = _normalize_token(ticker_code)

    if not base:
        return None

    if "." in base:
        return base

    base = _market_specific_adjustments(exchange, base)

    suffix = EXCHANGE_SUFFIX_MAP.get(exchange or "", "")

    if exchange in {"SEHK", "SASE", "TWSE", "TWO"}:
        base = _pad_numeric_code(base, width=4)

    return f"{base}{suffix}"


def generate_yahoo_code_candidates(yahoo_code: str | None, ticker: str | None = None) -> list[str]:
    candidates: list[str] = []

    normalized = normalize_yahoo_code(yahoo_code, ticker)
    if normalized:
        candidates.append(normalized)

    raw_yc = _normalize_token(yahoo_code)
    if raw_yc and raw_yc not in candidates:
        candidates.append(raw_yc)

    exchange, ticker_code = _split_exchange_ticker(ticker)
    ticker_token = _normalize_token(ticker_code)
    if ticker_token and ticker_token not in candidates:
        candidates.append(ticker_token)

    if ticker_token:
        adjusted = _market_specific_adjustments(exchange, ticker_token)
        if adjusted not in candidates:
            candidates.append(adjusted)

    if exchange == "SEHK" and ticker_token:
        padded = _pad_numeric_code(ticker_token, 4)
        hk = f"{padded}.HK"
        if hk not in candidates:
            candidates.append(hk)

    if exchange in {"SHSE", "SSE"} and ticker_token:
        ss = f"{_pad_numeric_code(_market_specific_adjustments(exchange, ticker_token), 6)}.SS"
        if ss not in candidates:
            candidates.append(ss)

    if exchange == "SZSE" and ticker_token:
        sz = f"{_pad_numeric_code(_market_specific_adjustments(exchange, ticker_token), 6)}.SZ"
        if sz not in candidates:
            candidates.append(sz)

    if exchange == "BSE" and ticker_token:
        bo = f"{_pad_numeric_code(_market_specific_adjustments(exchange, ticker_token), 6)}.BO"
        if bo not in candidates:
            candidates.append(bo)

    if exchange == "NSEI" and ticker_token:
        ns = f"{_market_specific_adjustments(exchange, ticker_token)}.NS"
        if ns not in candidates:
            candidates.append(ns)

    if exchange in {"KOSDAQ", "KOSE", "KOSPI", "KRX"} and ticker_token:
        k_code = _market_specific_adjustments(exchange, ticker_token)
        for suffix in [".KQ", ".KS"]:
            c = f"{k_code}{suffix}"
            if c not in candidates:
                candidates.append(c)

    return candidates
