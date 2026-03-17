#!/usr/bin/env python3
"""
Provedores de holdings de ETFs direto dos emissores.

Cada provedor busca dados de holdings de uma fonte específica
(iShares/BlackRock, Vanguard, SPDR/State Street, etc.) e retorna
no formato padronizado esperado por ETFExtractor.save_holdings().

Formato de saída:
    [{
        "holding_ticker": str | None,
        "holding_name": str,
        "weight": float | None,
        "shares": int | None,
        "market_value": float | None,
        "sector": str | None,
        "asset_class": str | None,
        "country": str | None,
        "cusip": str | None,
        "isin": str | None,
    }]
"""

import csv
import io
import logging
import re
import time
from typing import Dict, List, Optional, Tuple

import requests

log = logging.getLogger("holdings_providers")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}

# ────────────────────────────────────────────────────────────────
# iShares / BlackRock  CSV
# ────────────────────────────────────────────────────────────────

# Product ID mapeado manualmente – iShares exige isso na URL
ISHARES_PRODUCTS: Dict[str, int] = {
    "AGG":  239458,
    "EMB":  239572,
    "FLOT": 239536,
    "GOVT": 239468,
    "HYG":  239565,
    "IAGG": 279626,
    "IAU":  239561,
    "IBIT": 333011,
    "IEF":  239456,
    "IGIB": 239463,
    "IGSB": 239451,
    "LQD":  239566,
    "MBB":  239465,
    "MUB":  239766,
    "SHV":  239466,
    "SHY":  239452,
    "SLV":  239855,
    "TIP":  239467,
    "TLT":  239454,
}

def _parse_float(val: str) -> Optional[float]:
    """Converte string numérica tolerando vírgulas e '-'."""
    if not val or val.strip() in ("-", "N/A", "--", ""):
        return None
    try:
        return float(val.strip().replace(",", ""))
    except (ValueError, TypeError):
        return None

def _parse_int(val: str) -> Optional[int]:
    f = _parse_float(val)
    return int(f) if f is not None else None


def fetch_ishares(ticker: str, timeout: int = 20) -> List[Dict]:
    """Baixa holdings de um ETF iShares via CSV do site da BlackRock."""
    product_id = ISHARES_PRODUCTS.get(ticker.upper())
    if not product_id:
        return []

    url = (
        f"https://www.ishares.com/us/products/{product_id}/"
        f"ishares-etf/1467271812596.ajax?fileType=csv"
        f"&fileName={ticker.upper()}_holdings&dataType=fund"
    )

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        if resp.status_code != 200 or len(resp.text) < 100:
            return []
    except requests.RequestException:
        return []

    text = resp.text

    # iShares CSV tem linhas de cabeçalho antes do header real
    lines = text.strip().split("\n")
    header_idx = None
    for i, line in enumerate(lines):
        if "Name," in line and ("Weight" in line or "Market Value" in line):
            header_idx = i
            break
    if header_idx is None:
        return []

    csv_text = "\n".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(csv_text))

    holdings = []
    for row in reader:
        name = (row.get("Name") or "").strip()
        if not name or name.startswith("---"):
            continue

        ticker_val = (row.get("Ticker") or row.get("ticker") or "").strip() or None
        weight = _parse_float(row.get("Weight (%)") or row.get("Weight") or "")
        market_value = _parse_float(row.get("Market Value") or "")
        shares = _parse_int(row.get("Shares") or row.get("Par Value") or "")
        sector = (row.get("Sector") or "").strip() or None
        asset_class = (row.get("Asset Class") or "").strip() or None
        country = (row.get("Location") or "").strip() or None
        cusip_val = (row.get("CUSIP") or "").strip() or None
        isin_val = (row.get("ISIN") or "").strip() or None

        holdings.append({
            "holding_ticker": ticker_val,
            "holding_name": name,
            "weight": weight,
            "shares": shares,
            "market_value": market_value,
            "sector": sector,
            "asset_class": asset_class,
            "country": country,
            "cusip": cusip_val,
            "isin": isin_val,
        })

    log.info(f"iShares CSV: {ticker} → {len(holdings)} holdings")
    return holdings


# ────────────────────────────────────────────────────────────────
# Vanguard  API (JSON)
# ────────────────────────────────────────────────────────────────

def fetch_vanguard(ticker: str, timeout: int = 20) -> List[Dict]:
    """Busca holdings de um ETF Vanguard via API pública de portfólio."""
    # Tenta endpoint de bonds primeiro, depois stocks
    for holding_type in ("bond", "stock"):
        url = (
            f"https://investor.vanguard.com/investment-products/etfs/profile/api/"
            f"{ticker.upper()}/portfolio-holding/{holding_type}"
            f"?offset=0&limit=500&sortField=marketValue&sortOrder=desc"
        )
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=timeout)
            if resp.status_code != 200:
                continue
            data = resp.json()
        except (requests.RequestException, ValueError):
            continue

        entities = data.get("fund", {}).get("entity", [])
        if not entities:
            continue

        holdings = []
        for ent in entities:
            name = ent.get("shortName", ent.get("longName", "")).strip()
            if not name:
                continue

            ticker_val = (ent.get("ticker") or "").strip() or None
            weight = _parse_float(str(ent.get("percentWeight", "")))
            market_value = _parse_float(str(ent.get("marketValue", "")))
            shares = _parse_int(str(ent.get("faceAmount") or ent.get("sharesHeld") or ""))
            sector = (ent.get("sectorName") or "").strip() or None
            country = (ent.get("country") or "").strip() or None
            cusip_val = (ent.get("cusip") or "").strip() or None
            isin_val = (ent.get("isin") or "").strip() or None
            coupon = ent.get("couponRate")
            maturity = ent.get("maturityDate")

            # Para bonds, inclui informações no nome se disponíveis
            if holding_type == "bond" and coupon is not None and maturity:
                name = f"{name} {coupon}% {maturity}"

            holdings.append({
                "holding_ticker": ticker_val,
                "holding_name": name,
                "weight": weight,
                "shares": shares,
                "market_value": market_value,
                "sector": sector,
                "asset_class": "Bond" if holding_type == "bond" else "Equity",
                "country": country,
                "cusip": cusip_val,
                "isin": isin_val,
            })

        if holdings:
            log.info(f"Vanguard API: {ticker} → {len(holdings)} holdings ({holding_type})")
            return holdings

    return []


# ────────────────────────────────────────────────────────────────
# SPDR / State Street  XLSX
# ────────────────────────────────────────────────────────────────

def fetch_spdr(ticker: str, timeout: int = 20) -> List[Dict]:
    """Baixa holdings de um ETF SPDR/State Street via XLSX."""
    if "." in ticker:  # Ignora tickers com sufixo (.SA, .L, etc.)
        return []
    try:
        import openpyxl
    except ImportError:
        log.warning("openpyxl não instalado; SPDR XLSX não disponível")
        return []

    url = (
        f"https://www.ssga.com/us/en/intermediary/etfs/library-content/"
        f"products/fund-data/etfs/us/"
        f"holdings-daily-us-en-{ticker.lower()}.xlsx"
    )

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        if resp.status_code != 200 or len(resp.content) < 1000:
            return []
    except requests.RequestException:
        return []

    try:
        wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
    except Exception as e:
        log.warning(f"SPDR XLSX parse error for {ticker}: {e}")
        return []

    # Encontra o header (linha com "Name" e "Weight")
    header_idx = None
    for i, row in enumerate(rows):
        str_row = [str(c).strip() if c else "" for c in row]
        if "Name" in str_row and ("Weight" in str_row or "Shares Held" in str_row):
            header_idx = i
            break
    if header_idx is None:
        return []

    header = [str(c).strip() if c else f"col_{j}" for j, c in enumerate(rows[header_idx])]
    col_map = {h: j for j, h in enumerate(header)}

    holdings = []
    for row in rows[header_idx + 1:]:
        vals = list(row)
        name_idx = col_map.get("Name")
        if name_idx is None or name_idx >= len(vals):
            continue
        name = str(vals[name_idx]).strip() if vals[name_idx] else ""
        if not name or name == "None":
            continue

        def _get(col_name):
            idx = col_map.get(col_name)
            if idx is None or idx >= len(vals):
                return None
            v = vals[idx]
            return v

        ticker_val = str(_get("Ticker") or "").strip() or None
        weight = _parse_float(str(_get("Weight") or ""))
        market_value = _parse_float(str(_get("Market Value") or _get("Notional Value") or ""))
        shares = _parse_int(str(_get("Shares Held") or _get("Shares") or ""))
        sector = str(_get("Sector") or "").strip() or None
        country = str(_get("Country") or _get("Location") or "").strip() or None
        cusip_val = str(_get("CUSIP") or "").strip() or None
        isin_val = str(_get("ISIN") or "").strip() or None
        asset_class = str(_get("Asset Class") or "").strip() or None

        holdings.append({
            "holding_ticker": ticker_val,
            "holding_name": name,
            "weight": weight,
            "shares": shares,
            "market_value": market_value,
            "sector": sector,
            "asset_class": asset_class,
            "country": country,
            "cusip": cusip_val,
            "isin": isin_val,
        })

    log.info(f"SPDR XLSX: {ticker} → {len(holdings)} holdings")
    return holdings


# ────────────────────────────────────────────────────────────────
# Commodity / Crypto – holding sintético
# ────────────────────────────────────────────────────────────────

COMMODITY_MAP: Dict[str, Dict] = {
    "GLD":   {"name": "Gold Bullion",             "asset_class": "Commodity"},
    "GLDM":  {"name": "Gold Bullion",             "asset_class": "Commodity"},
    "IAU":   {"name": "Gold Bullion",             "asset_class": "Commodity"},
    "BAR":   {"name": "Gold Bullion",             "asset_class": "Commodity"},
    "SLV":   {"name": "Silver Bullion",           "asset_class": "Commodity"},
    "PPLT":  {"name": "Platinum Bullion",         "asset_class": "Commodity"},
    "PALL":  {"name": "Palladium Bullion",        "asset_class": "Commodity"},
    "CPER":  {"name": "Copper Futures Contracts", "asset_class": "Commodity"},
    "USO":   {"name": "Crude Oil Futures (WTI)",  "asset_class": "Commodity"},
    "PDBC":  {"name": "Diversified Commodities Futures", "asset_class": "Commodity"},
    "GBTC":  {"name": "Bitcoin (BTC)",            "asset_class": "Cryptocurrency"},
    "ETHE":  {"name": "Ethereum (ETH)",           "asset_class": "Cryptocurrency"},
    "IBIT":  {"name": "Bitcoin (BTC)",            "asset_class": "Cryptocurrency"},
    # BR crypto/commodity
    "BITH11.SA": {"name": "Bitcoin (BTC)",        "asset_class": "Cryptocurrency"},
    "ETHE11.SA": {"name": "Ethereum (ETH)",       "asset_class": "Cryptocurrency"},
    "HASH11.SA": {"name": "Crypto Index (NCI)",   "asset_class": "Cryptocurrency"},
    "GOLD11.SA": {"name": "Gold Bullion",         "asset_class": "Commodity"},
}


def fetch_commodity(ticker: str) -> List[Dict]:
    """Retorna holding sintético para ETFs de commodity/crypto."""
    info = COMMODITY_MAP.get(ticker.upper())
    if info is None:
        # Tenta .SA
        info = COMMODITY_MAP.get(ticker)
    if info is None:
        return []

    return [{
        "holding_ticker": None,
        "holding_name": info["name"],
        "weight": 100.0,
        "shares": None,
        "market_value": None,
        "sector": None,
        "asset_class": info["asset_class"],
        "country": None,
        "cusip": None,
        "isin": None,
    }]


# ────────────────────────────────────────────────────────────────
# Dispatcher – tenta todas as fontes de emissores
# ────────────────────────────────────────────────────────────────

def fetch_from_issuer(ticker: str) -> Tuple[List[Dict], str]:
    """Tenta obter holdings direto do emissor.

    Returns:
        (holdings_list, source_name)  –  lista vazia + "none" se nada encontrado.
    """
    upper = ticker.upper()

    # 1. iShares
    if upper in ISHARES_PRODUCTS:
        data = fetch_ishares(upper)
        if data:
            return data, "ishares_csv"

    # 2. Vanguard
    data = fetch_vanguard(upper)
    if data:
        return data, "vanguard_api"

    # 3. SPDR / State Street
    data = fetch_spdr(upper)
    if data:
        return data, "spdr_xlsx"

    # 4. Commodity / Crypto (sintético)
    data = fetch_commodity(ticker)  # preserva case para .SA
    if data:
        return data, "commodity_synthetic"

    return [], "none"
