#!/usr/bin/env python3
"""
Extrator de dados de ETFs com múltiplas fontes:
  - yfinance  (metadados + top holdings)
  - SEC EDGAR (holdings completos via N-PORT para ETFs dos EUA)
  - CVM       (holdings de ETFs brasileiros via dados abertos)

Fallback chain: SEC EDGAR → yfinance (EUA) | CVM → yfinance (Brasil)
"""

import logging
import time
import threading
import random
import sqlite3
import os
import csv
import io
import json
import zipfile
import warnings
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)  # 10 MB – CVM gera campos grandes

_cacert = Path(r"C:\cacerts\cacert.pem")
if _cacert.exists():
    os.environ.setdefault("CURL_CA_BUNDLE", str(_cacert))

import requests
import yfinance as yf
from yfinance.exceptions import YFRateLimitError

try:
    from lxml import etree
    HAS_LXML = True
except ImportError:
    HAS_LXML = False

warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")
warnings.filterwarnings("ignore", message=".*Timestamp.utcnow.*")

from .base_extractor import BaseExtractor

log = logging.getLogger("etf_extractor")

# ────────────────────────────────────────────────────────────────────
# Lista expandida de ETFs (~350 tickers)
# ────────────────────────────────────────────────────────────────────
ETF_LIST_US = [
    # Broad Market
    "SPY", "VOO", "IVV", "VTI", "QQQ", "DIA", "RSP", "ITOT", "SPLG", "SPTM",
    "SCHB", "SCHX", "IWB", "VV", "MGC", "OEF",
    # Mid Cap
    "MDY", "IJH", "VO", "IWR", "IVOO", "SCHM", "SPMD",
    # Small Cap
    "IWM", "VB", "SCHA", "IJR", "VXF", "SPSM", "VIOO",
    # Setoriais – Select Sector SPDRs
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLC", "XLU", "XLB", "XLP", "XLY", "XLRE",
    # Setoriais – Vanguard
    "VGT", "VFH", "VHT", "VDE", "VIS", "VOX", "VNQ", "VAW", "VCR", "VDC", "VPU",
    # Setoriais – iShares
    "IYW", "IYF", "IYH", "IYE", "IYJ", "IYZ", "IYR", "IYM", "IYC", "IYK",
    # Setoriais – Fidelity
    "FCOM", "FDIS", "FENY", "FHLC", "FIDU", "FMAT", "FNCL", "FREL", "FSTA", "FTEC", "FUTY",
    # Internacional – Desenvolvidos
    "EFA", "VEA", "IEFA", "SPDW", "VGK", "EWJ", "EWG", "EWU", "EWQ", "EWC", "EWA",
    "EWL", "EWP", "EWN", "EWI", "EWD", "EWK", "NORW", "EFNL",
    "HEWJ", "HEDJ", "HEFA", "DXJ",
    # Internacional – Emergentes
    "VWO", "EEM", "IEMG", "EWZ", "FXI", "INDA", "EWY", "EWT", "EWW", "TUR",
    "EZA", "THD", "EPHE", "ECH", "EIDO", "EPU", "GXG", "ARGT",
    "MCHI", "KWEB", "ASHR", "CNYA",
    # Internacional – Regiões
    "AAXJ", "AIA", "GMF", "PAF", "FEZ", "EZU", "VPL",
    # Renda Fixa – EUA
    "BND", "AGG", "TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "TIP", "VCIT",
    "VCSH", "MUB", "GOVT", "SCHZ", "IGSB", "IGIB", "FLOT", "SHV", "BIL",
    "VGSH", "VGIT", "VGLT", "VMBS", "MBB",
    # Renda Fixa – Internacional
    "EMB", "BNDX", "IAGG", "BWX", "PCY", "EMLC",
    # Commodities
    "GLD", "SLV", "IAU", "USO", "DBC", "PDBC", "GDX", "GDXJ",
    "GLDM", "BAR", "PPLT", "PALL", "CPER", "WEAT", "CORN", "SOYB",
    # Dividendos
    "VYM", "DVY", "SCHD", "HDV", "SDY", "NOBL", "DGRO", "SPYD",
    "VIG", "DGRW", "FDL", "RDVY",
    # Crescimento / Valor
    "VUG", "IWF", "VONG", "VTV", "IWD", "VONV", "IUSV", "IUSG",
    "IWO", "IWN", "RPV", "RPG", "SPYV", "SPYG", "SCHG", "SCHV",
    # Temáticos – Tecnologia & Inovação
    "ARKK", "ARKG", "ARKW", "ARKF", "ARKQ", "SOXX", "SMH", "XBI", "IBB",
    "SKYY", "CIBR", "HACK", "BOTZ", "ROBO", "IRBO",
    "WCLD", "CLOU", "BUG", "AIQ", "DRIV",
    # Temáticos – Energia & Clima
    "ICLN", "TAN", "QCLN", "FAN", "PBW", "ACES",
    "LIT", "REMX", "URA",
    # Multi-ativo / Balanced
    "AOM", "AOR", "AOK", "AOA",
    # Volatilidade / Hedge
    "VIXY", "SVXY",
    # Alavancados populares
    "TQQQ", "SQQQ", "SPXL", "SPXS", "QLD", "SSO", "UPRO", "SDS",
    "TNA", "TZA", "LABU", "LABD",
    # REITs & Infraestrutura
    "SCHH", "RWR", "ICF", "IGF", "USRT", "REET", "REM", "MORT",
    # ESG
    "ESGU", "ESGV", "SUSA", "DSI", "KRMA", "VEGN",
    # Crypto-related
    "BITO", "GBTC", "ETHE",
    # Factor / Smart Beta
    "MTUM", "QUAL", "VLUE", "SIZE", "USMV", "EFAV",
    "MOAT", "PKW", "COWZ", "DSTL",
]

ETF_LIST_BR = [
    # Ibovespa
    "BOVA11.SA", "BOVV11.SA", "BBOV11.SA",
    # S&P 500
    "IVVB11.SA", "SPXI11.SA",
    # Small Cap
    "SMAL11.SA", "SMAC11.SA",
    # Renda Fixa
    "IMAB11.SA", "IRFM11.SA", "FIXA11.SA", "B5P211.SA",
    # Dividendos
    "DIVO11.SA", "BBSD11.SA",
    # Sustentabilidade
    "ISUS11.SA",
    # Ouro
    "GOLD11.SA",
    # Nasdaq / EUA
    "NASD11.SA", "QQQM11.SA",
    # Cripto
    "HASH11.SA", "ETHE11.SA", "BITH11.SA",
    # Outros
    "MATB11.SA", "FIND11.SA", "ECOO11.SA", "TECK11.SA", "XINA11.SA",
    "EURP11.SA", "ACWI11.SA",
    # Adicionais B3
    "BRAX11.SA", "PIBB11.SA", "BOVB11.SA",
    "5MBA11.SA", "HTEK11.SA",
]

ETF_LIST_GLOBAL = [
    # Europa / LSE
    "VWRL.L", "IWDA.L", "CSPX.L", "VUSA.L", "ISF.L",
    "VWRA.L", "EIMI.L", "SWDA.L",
    # Euro Stoxx
    "FEZ", "EZU",
    # Canadá
    "XIU.TO", "XIC.TO", "ZSP.TO", "VFV.TO", "XEI.TO", "VCN.TO",
    # Austrália
    "VAS.AX", "IOZ.AX", "STW.AX",
    # Hong Kong
    "2800.HK", "3067.HK",
]

ALL_ETFS = ETF_LIST_US + ETF_LIST_BR + ETF_LIST_GLOBAL


# ────────────────────────────────────────────────────────────────────
# Mapeamento Ticker B3 → CNPJ CVM (ETFs brasileiros conhecidos)
# ────────────────────────────────────────────────────────────────────
_BR_TICKER_TO_CNPJ = {
    "BOVA11.SA":  "10.406.511/0001-61",
    "BOVV11.SA":  "21.407.758/0001-19",
    "BBOV11.SA":  "32.203.211/0001-18",
    "IVVB11.SA":  "19.909.560/0001-91",
    "SPXI11.SA":  "24.739.507/0001-70",
    "SMAL11.SA":  "10.406.600/0001-08",
    "SMAC11.SA":  "34.803.814/0001-86",
    "IMAB11.SA":  "15.149.118/0001-22",
    "IRFM11.SA":  "13.523.890/0001-62",
    "FIXA11.SA":  "33.880.513/0001-49",
    "B5P211.SA":  "38.354.864/0001-84",
    "DIVO11.SA":  "12.984.444/0001-03",
    "ISUS11.SA":  "11.184.136/0001-15",
    "GOLD11.SA":  "32.839.591/0001-39",
    "HASH11.SA":  "40.420.323/0001-94",
    "FIND11.SA":  "14.161.230/0001-14",
    "ECOO11.SA":  "15.562.377/0001-01",
    "MATB11.SA":  "13.416.228/0001-09",
    "BRAX11.SA":  "11.455.378/0001-04",
    "PIBB11.SA":  "06.323.688/0001-89",
    "NASD11.SA":  "42.249.267/0001-42",
    "TECK11.SA":  "39.710.378/0001-47",
    "XINA11.SA":  "39.272.552/0001-35",
    "EURP11.SA":  "39.272.561/0001-26",
    "ACWI11.SA":  "41.204.671/0001-51",
}


# ────────────────────────────────────────────────────────────────────
# Rate Limiter (thread-safe)
# ────────────────────────────────────────────────────────────────────
class _RateLimiter:
    def __init__(self, max_per_second: float = 1.5):
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last = 0.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            jitter = self._min_interval * random.uniform(0.5, 2.0)
            wait = jitter - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


# ────────────────────────────────────────────────────────────────────
# SQL DDL
# ────────────────────────────────────────────────────────────────────
_DDL_ETFS = """
CREATE TABLE IF NOT EXISTS etfs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT    UNIQUE NOT NULL,
    name        TEXT,
    exchange    TEXT,
    currency    TEXT,
    category    TEXT,
    subcategory TEXT,
    region      TEXT,
    country     TEXT,
    index_tracked TEXT,
    issuer      TEXT,
    inception_date TEXT,
    expense_ratio  REAL,
    aum            REAL,
    avg_volume     INTEGER,
    total_holdings INTEGER,
    last_updated   TEXT,
    data_source    TEXT DEFAULT 'yfinance'
);
"""

_DDL_HOLDINGS = """
CREATE TABLE IF NOT EXISTS etf_holdings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    etf_ticker      TEXT    NOT NULL,
    holding_ticker  TEXT,
    holding_name    TEXT,
    weight          REAL,
    shares          INTEGER,
    market_value    REAL,
    sector          TEXT,
    asset_class     TEXT,
    country         TEXT,
    cusip           TEXT,
    isin            TEXT,
    report_date     TEXT,
    last_updated    TEXT,
    FOREIGN KEY (etf_ticker) REFERENCES etfs(ticker)
);
"""

_DDL_LOG = """
CREATE TABLE IF NOT EXISTS etf_update_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    etf_ticker     TEXT,
    update_type    TEXT,
    source         TEXT DEFAULT 'yfinance',
    status         TEXT,
    records_count  INTEGER DEFAULT 0,
    timestamp      TEXT,
    error_message  TEXT
);
"""

_DDL_TAGS = """
CREATE TABLE IF NOT EXISTS etf_tags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    etf_ticker  TEXT    NOT NULL,
    tag_type    TEXT    NOT NULL,
    tag_value   TEXT    NOT NULL,
    confidence  REAL    DEFAULT 1.0,
    source      TEXT    DEFAULT 'auto',
    created_at  TEXT,
    FOREIGN KEY (etf_ticker) REFERENCES etfs(ticker),
    UNIQUE(etf_ticker, tag_type, tag_value)
);
"""

_IDX_TAGS = [
    "CREATE INDEX IF NOT EXISTS idx_etf_tags_ticker ON etf_tags(etf_ticker);",
    "CREATE INDEX IF NOT EXISTS idx_etf_tags_type   ON etf_tags(tag_type);",
    "CREATE INDEX IF NOT EXISTS idx_etf_tags_value  ON etf_tags(tag_value);",
]

_IDX_HOLDINGS = [
    "CREATE INDEX IF NOT EXISTS idx_etf_holdings_etf     ON etf_holdings(etf_ticker);",
    "CREATE INDEX IF NOT EXISTS idx_etf_holdings_holding ON etf_holdings(holding_ticker);",
    "CREATE INDEX IF NOT EXISTS idx_etfs_ticker          ON etfs(ticker);",
]


# ────────────────────────────────────────────────────────────────────
# SEC EDGAR – constantes
# ────────────────────────────────────────────────────────────────────
_SEC_HEADERS = {
    "User-Agent": "WACC-Automation-System/2.0 (Educational Purpose; wacc@educational.dev)",
    "Accept": "application/json",
}
_SEC_BASE = "https://data.sec.gov"
_SEC_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
_NPORT_NS = {"n": "http://www.sec.gov/edgar/nport"}

# Cache de CIK → ticker (carregado sob demanda)
_cik_cache: Dict[str, int] = {}
_name_to_ticker: Dict[str, str] = {}  # nome_normalizado → ticker (para resolver holdings)
_cusip_to_ticker: Dict[str, str] = {}  # CUSIP → ticker (cache persistente)
_cik_lock = threading.Lock()

import re as _re

def _normalize_company_name(name: str) -> str:
    """Normaliza nome de empresa para matching (remove sufixos, pontuação, etc.)."""
    n = name.upper().strip()
    # Remover sufixos estruturados: /THE, /DE, /OH, /NY, /MD, /DE/, etc.
    n = _re.sub(r'/[A-Z]{2,3}/?$', '', n).strip()
    n = _re.sub(r'^THE\s+', '', n)
    # Remover formato SEC de estado: " /DE/" ou " /MD/" no meio/fim
    n = _re.sub(r'\s*/[A-Z]{2,3}/\s*', ' ', n)
    # Padronizar & → AND
    n = n.replace('&', ' AND ')
    # Remover apóstrofos possessivos
    n = n.replace("'S ", "S ").replace("'S", "S")
    # Hífens e pontos → espaços (para manter tokens separados)
    n = n.replace('-', ' ').replace('.', ' ')
    # Remover restante da pontuação
    n = _re.sub(r'[^A-Z0-9\s]', '', n)
    n = _re.sub(r'\s+', ' ', n).strip()
    # Remover sufixos corporativos (mais longo primeiro, multi-pass)
    for suffix in [' COMPANIES INC', ' COS INC', ' CO INC', ' INC',
                   ' CORP', ' CO', ' LTD', ' LLC', ' PLC', ' SA', ' AG',
                   ' NV', ' SE', ' LP', ' GROUP', ' HOLDINGS', ' HOLDING',
                   ' COS', ' COMPANIES', ' COMPANY']:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
            break
    # Comprimir iniciais isoladas: "W R" → "WR", "A O" → "AO"
    n = _re.sub(r'\b([A-Z])\s+(?=[A-Z]\b)', r'\1', n)
    return n


# ────────────────────────────────────────────────────────────────────
# ETFExtractor
# ────────────────────────────────────────────────────────────────────
class ETFExtractor(BaseExtractor):
    """Extrator de metadados e composição de ETFs.

    Fontes (fallback chain):
      Holdings EUA:    SEC EDGAR N-PORT → yfinance
      Holdings Brasil: CVM dados abertos → yfinance
      Metadados:       yfinance (sempre)
    """

    DB_PATH = Path(__file__).resolve().parent.parent / "data" / "damodaran_data_new.db"

    def __init__(self, db_path: Optional[str] = None, rate_limit: float = 1.5,
                 use_sec: bool = True, use_cvm: bool = True):
        super().__init__(
            name="ETF",
            base_url="https://finance.yahoo.com",
            cache_duration=86400,  # 24h
        )
        self.db_path = str(db_path or self.DB_PATH)
        self._rate = _RateLimiter(rate_limit)
        self._sec_rate = _RateLimiter(8.0)  # SEC permite 10 req/s
        self.use_sec = use_sec and HAS_LXML
        self.use_cvm = use_cvm
        self._cvm_cache: Optional[Dict[str, List[Dict]]] = None  # lazy load
        self._cusip_cache_path = str(Path(self.db_path).parent / "cusip_ticker_cache.json")
        self._trust_map_path = str(Path(self.db_path).parent / "ticker_trust_map.json")
        self._load_cusip_cache()
        self._load_trust_map()
        self._ensure_tables()

    def _load_cusip_cache(self):
        """Carrega cache CUSIP→ticker de arquivo JSON."""
        global _cusip_to_ticker
        try:
            with open(self._cusip_cache_path, "r") as f:
                _cusip_to_ticker = json.load(f)
            log.info(f"Cache CUSIP→ticker carregado: {len(_cusip_to_ticker)} entradas")
        except (FileNotFoundError, json.JSONDecodeError):
            _cusip_to_ticker = {}

    def _save_cusip_cache(self):
        """Salva cache CUSIP→ticker em arquivo JSON."""
        try:
            with open(self._cusip_cache_path, "w") as f:
                json.dump(_cusip_to_ticker, f, indent=2)
        except Exception as e:
            log.warning(f"Erro ao salvar cache CUSIP: {e}")

    def _load_trust_map(self):
        """Carrega mapeamento ticker→trust info de arquivo JSON."""
        self._trust_map = {}
        try:
            with open(self._trust_map_path, "r") as f:
                self._trust_map = json.load(f)
            if self._trust_map:
                log.info(f"Trust map carregado: {len(self._trust_map)} ETFs")
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    # ── Tabelas ──────────────────────────────────────────────────
    def _ensure_tables(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_DDL_ETFS)
            conn.execute(_DDL_HOLDINGS)
            conn.execute(_DDL_LOG)
            conn.execute(_DDL_TAGS)
            for idx in _IDX_HOLDINGS:
                conn.execute(idx)
            for idx in _IDX_TAGS:
                conn.execute(idx)
            # Migração: adicionar colunas novas se não existem
            for col, ctype in [("country", "TEXT"), ("cusip", "TEXT"), ("isin", "TEXT")]:
                try:
                    conn.execute(f"ALTER TABLE etf_holdings ADD COLUMN {col} {ctype}")
                except Exception:
                    pass  # coluna já existe
            conn.commit()
        log.info("Tabelas de ETF verificadas/criadas.")

    # ── Extração de metadados ────────────────────────────────────
    def get_etf_metadata(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Extrai metadados de um ETF via yfinance."""
        self._rate.acquire()
        try:
            t = yf.Ticker(ticker)
            info = t.info or {}

            if not info or info.get("quoteType") not in ("ETF", "MUTUALFUND", None):
                # Alguns ETFs retornam quoteType=None no yfinance
                if not info.get("shortName"):
                    log.warning(f"{ticker}: sem dados ou não é ETF (quoteType={info.get('quoteType')})")
                    return None

            meta = {
                "ticker": ticker,
                "name": info.get("shortName") or info.get("longName"),
                "exchange": info.get("exchange"),
                "currency": info.get("currency"),
                "category": info.get("category"),
                "subcategory": info.get("fundFamily"),
                "region": None,
                "country": info.get("country"),
                "index_tracked": None,
                "issuer": info.get("fundFamily"),
                "inception_date": info.get("fundInceptionDate"),
                "expense_ratio": _pct(info.get("annualReportExpenseRatio")),
                "aum": info.get("totalAssets"),
                "avg_volume": info.get("averageDailyVolume10Day") or info.get("averageVolume"),
                "total_holdings": None,
                "last_updated": datetime.now().isoformat(),
            }
            log.info(f"{ticker}: metadados OK – {meta['name']}")
            return meta

        except YFRateLimitError:
            log.warning(f"{ticker}: rate limit – aguardando 60s")
            time.sleep(60)
            return self.get_etf_metadata(ticker)
        except Exception as e:
            log.error(f"{ticker}: erro ao buscar metadados – {e}")
            return None

    # ── Extração de holdings ─────────────────────────────────────
    def get_etf_holdings(self, ticker: str) -> List[Dict[str, Any]]:
        """Extrai os top holdings de um ETF via yfinance."""
        self._rate.acquire()
        try:
            t = yf.Ticker(ticker)
            holdings_list = []
            sector_weights = {}
            asset_classes = {}

            # funds_data.top_holdings → DataFrame com Symbol como index
            try:
                fd = t.funds_data
                if fd is not None:
                    # Sector weightings (para enriquecer holdings)
                    try:
                        sw = fd.sector_weightings
                        if sw:
                            sector_weights = sw
                    except Exception:
                        pass

                    # Asset classes
                    try:
                        ac = fd.asset_classes
                        if ac:
                            asset_classes = ac
                    except Exception:
                        pass

                    # Top holdings
                    try:
                        df = fd.top_holdings
                        if df is not None and not df.empty:
                            for symbol, row in df.iterrows():
                                name_col = "Name" if "Name" in df.columns else df.columns[0]
                                weight_col = "Holding Percent" if "Holding Percent" in df.columns else df.columns[-1]
                                raw_weight = float(row[weight_col])
                                weight_pct = round(raw_weight * 100, 4) if raw_weight <= 1 else round(raw_weight, 4)
                                holdings_list.append({
                                    "holding_ticker": str(symbol),
                                    "holding_name": str(row[name_col]) if name_col in df.columns else str(symbol),
                                    "weight": weight_pct,
                                })
                    except Exception:
                        pass
            except Exception:
                pass

            if holdings_list:
                log.info(f"{ticker}: {len(holdings_list)} holdings encontrados (yfinance)")
            else:
                log.warning(f"{ticker}: nenhum holding encontrado (yfinance)")

            return holdings_list

        except YFRateLimitError:
            log.warning(f"{ticker}: rate limit – aguardando 60s")
            time.sleep(60)
            return self.get_etf_holdings(ticker)
        except Exception as e:
            log.error(f"{ticker}: erro ao buscar holdings yfinance – {e}")
            return []

    # ── SEC EDGAR – Holdings completos (EUA) ─────────────────────
    def _load_cik_map(self):
        """Carrega mapeamento ticker→CIK e nome→ticker da SEC (lazy, thread-safe)."""
        global _cik_cache, _name_to_ticker
        with _cik_lock:
            if _cik_cache:
                return
            try:
                r = requests.get(
                    "https://www.sec.gov/files/company_tickers.json",
                    headers=_SEC_HEADERS, timeout=30,
                )
                r.raise_for_status()
                for v in r.json().values():
                    tk = v["ticker"].upper()
                    _cik_cache[tk] = v["cik_str"]
                    # Mapa nome→ticker (exato e normalizado) para resolver holdings SEC
                    title = v.get("title", "").strip()
                    if title:
                        _name_to_ticker[title.upper()] = tk
                        norm = _normalize_company_name(title)
                        if norm and norm not in _name_to_ticker:
                            _name_to_ticker[norm] = tk
                log.info(f"CIK map carregado: {len(_cik_cache)} tickers, {len(_name_to_ticker)} nomes")
            except Exception as e:
                log.error(f"Erro ao carregar mapa CIK: {e}")

    def _get_cik(self, ticker: str) -> Optional[str]:
        """Retorna CIK formatado (10 dígitos) para um ticker."""
        clean = ticker.replace(".SA", "").replace(".L", "").replace(".TO", "").replace(".AX", "").replace(".HK", "").upper()
        if not _cik_cache:
            self._load_cik_map()
        cik = _cik_cache.get(clean)
        if cik is None:
            return None
        return str(cik).zfill(10)

    def get_holdings_sec(self, ticker: str) -> List[Dict[str, Any]]:
        """Extrai holdings completos via SEC EDGAR N-PORT filing."""
        if not HAS_LXML:
            log.warning("lxml não disponível – SEC EDGAR desabilitado")
            return []

        cik = self._get_cik(ticker)
        trust_info = None
        if not cik:
            # Fallback: buscar via Trust CIK map
            trust_info = self._trust_map.get(ticker.upper())
            if not trust_info:
                log.debug(f"{ticker}: CIK não encontrado na SEC")
                return []
            log.info(f"{ticker}: usando Trust CIK ({trust_info.get('trust_name', '?')})")

        self._sec_rate.acquire()
        try:
            if trust_info:
                # Via Trust: usar accession direto do mapa
                acc = trust_info["accession"].replace("-", "")
                filing_date = trust_info["date"]
                trust_cik = trust_info["trust_cik"].lstrip("0")
                log.info(f"{ticker}: N-PORT via trust ({filing_date}), baixando XML...")
                xml_url = f"{_SEC_ARCHIVES}/{trust_cik}/{acc}/primary_doc.xml"
            else:
                # Via CIK direto: buscar filing mais recente
                url = f"{_SEC_BASE}/submissions/CIK{cik}.json"
                r = requests.get(url, headers=_SEC_HEADERS, timeout=30)
                r.raise_for_status()
                data = r.json()

                recent = data.get("filings", {}).get("recent", {})
                forms = recent.get("form", [])
                accessions = recent.get("accessionNumber", [])
                dates = recent.get("filingDate", [])

                nport_idx = None
                for i, f in enumerate(forms):
                    if "NPORT" in f:
                        nport_idx = i
                        break

                if nport_idx is None:
                    log.debug(f"{ticker}: sem filing N-PORT na SEC")
                    return []

                acc = accessions[nport_idx].replace("-", "")
                filing_date = dates[nport_idx]
                log.info(f"{ticker}: N-PORT encontrado ({filing_date}), baixando XML...")
                self._sec_rate.acquire()
                xml_url = f"{_SEC_ARCHIVES}/{cik.lstrip('0')}/{acc}/primary_doc.xml"
            r2 = requests.get(xml_url, headers=_SEC_HEADERS, timeout=60)
            r2.raise_for_status()

            # 4. Parse XML
            root = etree.fromstring(r2.content)
            securities = root.findall(".//n:invstOrSec", _NPORT_NS)

            if not securities:
                log.warning(f"{ticker}: N-PORT sem securities")
                return []

            holdings = []
            for sec in securities:
                name_el = sec.find("n:name", _NPORT_NS)
                cusip_el = sec.find("n:cusip", _NPORT_NS)
                balance_el = sec.find("n:balance", _NPORT_NS)
                val_el = sec.find("n:valUSD", _NPORT_NS)
                pct_el = sec.find("n:pctVal", _NPORT_NS)
                asset_el = sec.find("n:assetCat", _NPORT_NS)
                country_el = sec.find("n:invCountry", _NPORT_NS)
                isin_el = sec.find(".//n:isin", _NPORT_NS)
                cur_el = sec.find("n:curCd", _NPORT_NS)

                # Extrair ticker, ISIN e outros identifiers
                isin_val = None
                ticker_val = None
                ids_el = sec.find("n:identifiers", _NPORT_NS)
                if ids_el is not None:
                    for id_child in ids_el:
                        tag = id_child.tag.split("}")[-1] if "}" in id_child.tag else id_child.tag
                        if tag == "isin":
                            isin_val = id_child.get("value") or id_child.text
                        elif tag == "ticker":
                            ticker_val = id_child.get("value") or id_child.text

                name = name_el.text if name_el is not None else None
                cusip = cusip_el.text if cusip_el is not None else None
                shares = None
                try:
                    shares = int(float(balance_el.text)) if balance_el is not None else None
                except (ValueError, TypeError):
                    pass
                market_value = None
                try:
                    market_value = float(val_el.text) if val_el is not None else None
                except (ValueError, TypeError):
                    pass
                weight = None
                try:
                    weight = round(float(pct_el.text), 6) if pct_el is not None else None
                except (ValueError, TypeError):
                    pass

                asset_map = {"EC": "Equity", "DBT": "Debt", "ABS": "ABS",
                             "MBS": "MBS", "OTHER": "Other"}
                asset_cat = asset_el.text if asset_el is not None else None
                asset_class = asset_map.get(asset_cat, asset_cat)

                # Resolver ticker: 1) cache CUSIP, 2) identifiers/ticker,
                #   3) nome exato, 4) nome normalizado, 5) CUSIP como fallback
                resolved_ticker = None
                if cusip and cusip != '000000000':
                    resolved_ticker = _cusip_to_ticker.get(cusip)
                if not resolved_ticker:
                    resolved_ticker = ticker_val
                if not resolved_ticker and name:
                    name_upper = name.upper().strip()
                    resolved_ticker = _name_to_ticker.get(name_upper)
                    if not resolved_ticker:
                        resolved_ticker = _name_to_ticker.get(_normalize_company_name(name))
                if not resolved_ticker:
                    resolved_ticker = cusip
                # Atualizar cache CUSIP com resolução encontrada
                if cusip and cusip != '000000000' and resolved_ticker != cusip:
                    _cusip_to_ticker[cusip] = resolved_ticker

                holdings.append({
                    "holding_ticker": resolved_ticker,
                    "holding_name": name,
                    "weight": weight,
                    "shares": shares,
                    "market_value": market_value,
                    "sector": None,  # Setor preenchido posteriormente
                    "asset_class": asset_class,
                    "country": (country_el.text if country_el is not None else None),
                    "cusip": cusip,
                    "isin": isin_val,
                })

            log.info(f"{ticker}: {len(holdings)} holdings via SEC EDGAR (N-PORT {filing_date})")
            # Salvar cache CUSIP→ticker atualizado
            self._save_cusip_cache()
            return holdings

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log.debug(f"{ticker}: filing N-PORT não encontrado na SEC")
            else:
                log.warning(f"{ticker}: erro HTTP SEC – {e}")
            return []
        except Exception as e:
            log.warning(f"{ticker}: erro SEC EDGAR – {e}")
            return []

    # ── CVM – Holdings de ETFs brasileiros ───────────────────────
    def _load_cvm_data(self, year_month: Optional[str] = None) -> Dict[str, List[Dict]]:
        """Carrega composição de carteira da CVM (lazy, cached).

        Args:
            year_month: formato YYYYMM (padrão: mês anterior)

        Returns:
            Dict[cnpj] → lista de holdings
        """
        if self._cvm_cache is not None:
            return self._cvm_cache

        if year_month is None:
            # Mês anterior (CVM publica com atraso)
            now = datetime.now()
            if now.month == 1:
                year_month = f"{now.year - 1}12"
            else:
                year_month = f"{now.year}{now.month - 1:02d}"

        self._cvm_cache = {}
        url = f"http://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{year_month}.zip"
        log.info(f"Baixando composição CVM: {url}")

        try:
            r = requests.get(url, headers={"User-Agent": "WACC-Research"}, timeout=120)
            r.raise_for_status()

            z = zipfile.ZipFile(io.BytesIO(r.content))

            # 1. Processar arquivos BLC (blocos de ativos – fundos regulares)
            for fname in z.namelist():
                if "BLC" not in fname:
                    continue
                with z.open(fname) as f:
                    content = f.read().decode("latin-1")
                    reader = csv.DictReader(io.StringIO(content), delimiter=";")
                    for row in reader:
                        cnpj = row.get("CNPJ_FUNDO_CLASSE") or row.get("CNPJ_FUNDO", "")
                        if not cnpj:
                            continue
                        if cnpj not in self._cvm_cache:
                            self._cvm_cache[cnpj] = []

                        # Determinar asset class pelo bloco
                        blc = "Equity" if "BLC_1" in fname else \
                              "Fixed Income" if "BLC_2" in fname else \
                              "Swap" if "BLC_3" in fname else \
                              "FX" if "BLC_5" in fname else \
                              "Fund" if "BLC_7" in fname else "Other"

                        isin = row.get("CD_ISIN", "")
                        qty_str = row.get("QT_POS_FINAL", "0")
                        val_str = row.get("VL_MERC_POS_FINAL", "0")

                        try:
                            qty = float(qty_str.replace(",", ".")) if qty_str else 0
                        except ValueError:
                            qty = 0
                        try:
                            val = float(val_str.replace(",", ".")) if val_str else 0
                        except ValueError:
                            val = 0

                        self._cvm_cache[cnpj].append({
                            "holding_ticker": isin or None,
                            "holding_name": row.get("DENOM_SOCIAL", row.get("TP_ATIVO", "")),
                            "weight": None,
                            "shares": int(qty) if qty else None,
                            "market_value": val if val else None,
                            "sector": None,
                            "asset_class": blc,
                        })

            # 2. Processar cda_fie (Fundos Estruturados – inclui ETFs/FIIM)
            for fname in z.namelist():
                if "cda_fie" not in fname or "CONFID" in fname:
                    continue
                with z.open(fname) as f:
                    content = f.read().decode("latin-1")
                    reader = csv.DictReader(io.StringIO(content), delimiter=";")
                    for row in reader:
                        cnpj = row.get("CNPJ_FUNDO_CLASSE") or ""
                        if not cnpj:
                            continue
                        if cnpj not in self._cvm_cache:
                            self._cvm_cache[cnpj] = []

                        tp_ativo = (row.get("TP_ATIVO") or "").upper()
                        asset_class = "Equity" if tp_ativo in ("Ações", "AÇÕES", "ACOES") else \
                                      "Fixed Income" if "RENDA FIXA" in tp_ativo else \
                                      "Fund" if "COTAS" in tp_ativo or "FUNDO" in tp_ativo else \
                                      tp_ativo or "Other"

                        cd_ativo = row.get("CD_ATIVO", "")
                        ds_ativo = row.get("DS_ATIVO", "")
                        qty_str = row.get("QT_POS_FINAL", "0")
                        val_str = row.get("VL_MERC_POS_FINAL", "0")

                        try:
                            qty = float(qty_str.replace(",", ".")) if qty_str else 0
                        except ValueError:
                            qty = 0
                        try:
                            val = float(val_str.replace(",", ".")) if val_str else 0
                        except ValueError:
                            val = 0

                        self._cvm_cache[cnpj].append({
                            "holding_ticker": cd_ativo or None,
                            "holding_name": ds_ativo or row.get("EMISSOR", ""),
                            "weight": None,
                            "shares": int(qty) if qty else None,
                            "market_value": val if val else None,
                            "sector": row.get("CD_PAIS"),
                            "asset_class": asset_class,
                        })

            log.info(f"CVM carregada: {len(self._cvm_cache)} fundos, mês {year_month}")

        except requests.exceptions.HTTPError:
            # Talvez o mês não esteja disponível ainda, tentar mês anterior
            log.warning(f"CVM: dados de {year_month} não disponíveis")
            if year_month and len(year_month) == 6:
                y, m = int(year_month[:4]), int(year_month[4:])
                if m > 1:
                    prev = f"{y}{m - 1:02d}"
                else:
                    prev = f"{y - 1}12"
                log.info(f"Tentando mês anterior: {prev}")
                self._cvm_cache = None
                return self._load_cvm_data(prev)
        except Exception as e:
            log.error(f"Erro ao carregar CVM: {e}")

        return self._cvm_cache or {}

    def get_holdings_cvm(self, ticker: str) -> List[Dict[str, Any]]:
        """Extrai holdings de um ETF brasileiro via CVM."""
        cnpj = _BR_TICKER_TO_CNPJ.get(ticker)
        if not cnpj:
            log.debug(f"{ticker}: CNPJ não mapeado para CVM")
            return []

        cvm_data = self._load_cvm_data()
        holdings = cvm_data.get(cnpj, [])

        if holdings:
            # Calcular pesos baseado no valor de mercado
            total_value = sum(h.get("market_value", 0) or 0 for h in holdings)
            if total_value > 0:
                for h in holdings:
                    mv = h.get("market_value", 0) or 0
                    h["weight"] = round((mv / total_value) * 100, 4) if mv else 0
            log.info(f"{ticker}: {len(holdings)} holdings via CVM (CNPJ {cnpj})")
        else:
            log.debug(f"{ticker}: sem dados na CVM para CNPJ {cnpj}")

        return holdings

    # ── Fallback chain ───────────────────────────────────────────
    def get_holdings_with_fallback(self, ticker: str) -> tuple:
        """Obtém holdings usando fallback chain.

        Ordem:
          BR  → CVM → issuer (commodity/crypto) → yfinance
          EUA → SEC EDGAR → issuer (iShares/Vanguard/SPDR/commodity) → yfinance

        Returns:
            (holdings_list, source_name)
        """
        from .holdings_providers import fetch_from_issuer

        is_br = ticker.endswith(".SA")
        is_us = not any(ticker.endswith(s) for s in (".SA", ".L", ".TO", ".AX", ".HK"))

        # ETFs brasileiros: CVM → issuer → yfinance
        if is_br and self.use_cvm:
            holdings = self.get_holdings_cvm(ticker)
            if holdings:
                return holdings, "cvm"

        # ETFs dos EUA: SEC EDGAR → issuer → yfinance
        if is_us and self.use_sec:
            holdings = self.get_holdings_sec(ticker)
            if holdings:
                return holdings, "sec_edgar"

        # Issuer direto (iShares CSV, Vanguard API, SPDR XLSX, commodity)
        holdings, source = fetch_from_issuer(ticker)
        if holdings:
            return holdings, source

        # Fallback final: yfinance (todos)
        holdings = self.get_etf_holdings(ticker)
        if holdings:
            return holdings, "yfinance"

        return [], "none"

    # ── Persistência ─────────────────────────────────────────────
    def save_etf(self, meta: Dict[str, Any]) -> bool:
        """Salva/atualiza metadados de um ETF no banco."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO etfs (ticker, name, exchange, currency, category,
                                      subcategory, region, country, index_tracked,
                                      issuer, inception_date, expense_ratio, aum,
                                      avg_volume, total_holdings, last_updated, data_source)
                    VALUES (:ticker, :name, :exchange, :currency, :category,
                            :subcategory, :region, :country, :index_tracked,
                            :issuer, :inception_date, :expense_ratio, :aum,
                            :avg_volume, :total_holdings, :last_updated, 'yfinance')
                    ON CONFLICT(ticker) DO UPDATE SET
                        name=excluded.name, exchange=excluded.exchange,
                        currency=excluded.currency, category=excluded.category,
                        subcategory=excluded.subcategory, region=excluded.region,
                        country=excluded.country, index_tracked=excluded.index_tracked,
                        issuer=excluded.issuer, inception_date=excluded.inception_date,
                        expense_ratio=excluded.expense_ratio, aum=excluded.aum,
                        avg_volume=excluded.avg_volume, total_holdings=excluded.total_holdings,
                        last_updated=excluded.last_updated
                """, meta)
                conn.commit()
            return True
        except Exception as e:
            log.error(f"Erro ao salvar ETF {meta.get('ticker')}: {e}")
            return False

    def save_holdings(self, etf_ticker: str, holdings: List[Dict[str, Any]],
                      source: str = "yfinance") -> int:
        """Salva holdings de um ETF (substitui anteriores)."""
        now = datetime.now().isoformat()
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Remove holdings antigos do mesmo ETF
                conn.execute("DELETE FROM etf_holdings WHERE etf_ticker = ?", (etf_ticker,))
                count = 0
                for h in holdings:
                    conn.execute("""
                        INSERT INTO etf_holdings
                          (etf_ticker, holding_ticker, holding_name, weight,
                           shares, market_value, sector, asset_class,
                           country, cusip, isin,
                           report_date, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        etf_ticker,
                        h.get("holding_ticker"),
                        h.get("holding_name"),
                        h.get("weight"),
                        h.get("shares"),
                        h.get("market_value"),
                        h.get("sector"),
                        h.get("asset_class"),
                        h.get("country"),
                        h.get("cusip"),
                        h.get("isin"),
                        today,
                        now,
                    ))
                    count += 1
                # Atualiza total_holdings e data_source no ETF
                conn.execute(
                    "UPDATE etfs SET total_holdings = ?, data_source = ? WHERE ticker = ?",
                    (count, source, etf_ticker),
                )
                conn.commit()
            return count
        except Exception as e:
            log.error(f"Erro ao salvar holdings de {etf_ticker}: {e}")
            return 0

    def _log_update(self, ticker: str, update_type: str, status: str,
                    records: int = 0, error: str = None, source: str = "yfinance"):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO etf_update_log
                      (etf_ticker, update_type, source, status, records_count, timestamp, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (ticker, update_type, source, status, records, datetime.now().isoformat(), error))
                conn.commit()
        except Exception:
            pass

    # ── Workflow principal ───────────────────────────────────────
    def process_etf(self, ticker: str) -> Dict[str, Any]:
        """Processa um ETF: extrai metadados + holdings (com fallback) e salva."""
        result = {"ticker": ticker, "metadata": False, "holdings": 0, "source": "none"}

        meta = self.get_etf_metadata(ticker)
        if meta:
            ok = self.save_etf(meta)
            result["metadata"] = ok
            self._log_update(ticker, "metadata", "success" if ok else "error")
        else:
            self._log_update(ticker, "metadata", "error", error="sem dados")
            return result

        # Usa fallback chain para obter holdings
        holdings, source = self.get_holdings_with_fallback(ticker)
        result["source"] = source
        if holdings:
            count = self.save_holdings(ticker, holdings, source=source)
            result["holdings"] = count
            self._log_update(ticker, "holdings", "success", count, source=source)
        else:
            self._log_update(ticker, "holdings", "warning", 0, "sem holdings", source="none")

        return result

    def bulk_process(self, tickers: Optional[List[str]] = None,
                     batch_size: int = 20, pause_between_batches: float = 5.0,
                     callback=None) -> Dict[str, Any]:
        """Processa vários ETFs em lotes sequenciais.

        Args:
            tickers: lista de tickers (padrão: ALL_ETFS)
            batch_size: tamanho do lote
            pause_between_batches: pausa entre lotes (segundos)
            callback: função chamada após cada ETF com (result_dict,)

        Returns:
            Resumo {success, failed, total_holdings, errors}
        """
        tickers = tickers or ALL_ETFS
        total = len(tickers)
        success = 0
        failed = 0
        total_holdings = 0
        errors = []

        log.info(f"Iniciando processamento de {total} ETFs em lotes de {batch_size}")

        for i in range(0, total, batch_size):
            batch = tickers[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            log.info(f"── Lote {batch_num}/{total_batches} ({len(batch)} ETFs) ──")

            for ticker in batch:
                try:
                    r = self.process_etf(ticker)
                    if r["metadata"]:
                        success += 1
                        total_holdings += r["holdings"]
                    else:
                        failed += 1
                        errors.append(ticker)
                    if callback:
                        callback(r)
                except Exception as e:
                    failed += 1
                    errors.append(ticker)
                    log.error(f"{ticker}: exceção – {e}")

            if i + batch_size < total:
                log.info(f"Pausa de {pause_between_batches}s entre lotes...")
                time.sleep(pause_between_batches)

        summary = {
            "total": total,
            "success": success,
            "failed": failed,
            "total_holdings": total_holdings,
            "errors": errors,
        }
        log.info(f"Concluído: {success}/{total} ETFs, {total_holdings} holdings, {failed} falhas")
        return summary

    # ── Consultas ────────────────────────────────────────────────
    def get_all_etfs(self) -> List[Dict[str, Any]]:
        """Retorna todos os ETFs cadastrados."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM etfs ORDER BY ticker").fetchall()
            return [dict(r) for r in rows]

    def get_holdings_for(self, etf_ticker: str) -> List[Dict[str, Any]]:
        """Retorna holdings de um ETF."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM etf_holdings WHERE etf_ticker = ? ORDER BY weight DESC",
                (etf_ticker,),
            ).fetchall()
            return [dict(r) for r in rows]

    def find_etfs_containing(self, holding_ticker: str) -> List[Dict[str, Any]]:
        """Busca reversa: em quais ETFs um ticker aparece?"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT h.etf_ticker, e.name as etf_name, h.weight, h.holding_name
                FROM etf_holdings h
                JOIN etfs e ON e.ticker = h.etf_ticker
                WHERE h.holding_ticker = ?
                ORDER BY h.weight DESC
            """, (holding_ticker,)).fetchall()
            return [dict(r) for r in rows]

    def get_stats(self) -> Dict[str, Any]:
        """Estatísticas da base de ETFs."""
        with sqlite3.connect(self.db_path) as conn:
            etf_count = conn.execute("SELECT COUNT(*) FROM etfs").fetchone()[0]
            holding_count = conn.execute("SELECT COUNT(*) FROM etf_holdings").fetchone()[0]
            unique_holdings = conn.execute(
                "SELECT COUNT(DISTINCT holding_ticker) FROM etf_holdings WHERE holding_ticker IS NOT NULL"
            ).fetchone()[0]
            last_update = conn.execute(
                "SELECT MAX(last_updated) FROM etfs"
            ).fetchone()[0]

            # Stats por fonte
            source_stats = {}
            rows = conn.execute(
                "SELECT data_source, COUNT(*) as cnt FROM etfs GROUP BY data_source"
            ).fetchall()
            for row in rows:
                source_stats[row[0] or "unknown"] = row[1]

            # Stats por região
            region_stats = {}
            for r in conn.execute("""
                SELECT
                    CASE
                        WHEN ticker LIKE '%.SA' THEN 'Brasil'
                        WHEN ticker LIKE '%.L' THEN 'UK'
                        WHEN ticker LIKE '%.TO' THEN 'Canadá'
                        WHEN ticker LIKE '%.AX' THEN 'Austrália'
                        WHEN ticker LIKE '%.HK' THEN 'Hong Kong'
                        ELSE 'EUA'
                    END as reg,
                    COUNT(*) as cnt
                FROM etfs GROUP BY 1
            """).fetchall():
                region_stats[r[0]] = r[1]

            return {
                "etfs": etf_count,
                "holdings_total": holding_count,
                "unique_holdings": unique_holdings,
                "last_update": last_update,
                "by_source": source_stats,
                "by_region": region_stats,
            }

    def get_etfs_needing_update(self, days_old: int = 7) -> List[str]:
        """Retorna tickers de ETFs que precisam de atualização."""
        cutoff = datetime.now().isoformat()[:10]
        with sqlite3.connect(self.db_path) as conn:
            # ETFs não atualizados nos últimos N dias
            rows = conn.execute("""
                SELECT ticker FROM etfs
                WHERE last_updated IS NULL
                   OR date(last_updated) < date(?, '-' || ? || ' days')
                ORDER BY last_updated ASC
            """, (cutoff, days_old)).fetchall()
            return [r[0] for r in rows]

    def update_stale(self, days_old: int = 7, **kwargs) -> Dict[str, Any]:
        """Atualiza ETFs que estão desatualizados há mais de N dias."""
        stale = self.get_etfs_needing_update(days_old)
        if not stale:
            log.info("Nenhum ETF precisa de atualização")
            return {"total": 0, "success": 0, "failed": 0, "total_holdings": 0, "errors": []}
        log.info(f"{len(stale)} ETFs precisam de atualização (>{days_old} dias)")
        return self.bulk_process(tickers=stale, **kwargs)

    def get_overlap(self, ticker1: str, ticker2: str) -> Dict[str, Any]:
        """Calcula sobreposição entre dois ETFs."""
        h1 = self.get_holdings_for(ticker1)
        h2 = self.get_holdings_for(ticker2)

        tickers1 = {h["holding_ticker"] for h in h1 if h.get("holding_ticker")}
        tickers2 = {h["holding_ticker"] for h in h2 if h.get("holding_ticker")}

        common = tickers1 & tickers2
        union = tickers1 | tickers2

        overlap_pct = (len(common) / len(union) * 100) if union else 0

        return {
            "etf1": ticker1,
            "etf2": ticker2,
            "holdings_etf1": len(tickers1),
            "holdings_etf2": len(tickers2),
            "common": len(common),
            "overlap_pct": round(overlap_pct, 2),
            "common_tickers": sorted(common),
        }

    # ── Auto-tagging ───────────────────────────────────────────
    # Tag types: asset_class, geography, cap_size, style, sector, strategy, theme, index

    _TAG_RULES = {
        # Regras baseadas em palavras-chave no nome do ETF
        "asset_class": {
            "Equity":     [r"\b(stock|equity|s&p|nasdaq|russell|dow jones|msci|ftse)\b"],
            "Fixed Income": [r"\b(bond|treasury|aggregate|debt|fixed income|municipal|corporate bond|high yield bond|tips|gilt)\b"],
            "Commodity":  [r"\b(gold|silver|oil|commodity|palladium|platinum|copper|wheat|corn|soybean|agriculture|metal)\b"],
            "Currency":   [r"\b(currency|dollar|euro|yen|forex)\b"],
            "Real Estate": [r"\b(real estate|reit|mortgage)\b"],
            "Crypto":     [r"\b(bitcoin|ethereum|crypto|blockchain)\b"],
            "Multi-Asset": [r"\b(balanced|multi.?asset|allocation|target.?date|income)\b"],
        },
        "geography": {
            "US":        [r"\b(u\.?s\.?|united states|america|s&p|nasdaq|russell|dow jones)\b"],
            "Global":    [r"\b(global|world|acwi|all.?country)\b"],
            "International": [r"\b(international|foreign|ex.?us|eafe|developed market)\b"],
            "Emerging":  [r"\b(emerging|em|frontier)\b"],
            "Europe":    [r"\b(europe|euro|stoxx|ftse 100|dax|cac)\b"],
            "Asia":      [r"\b(asia|pacific|apac|japan|nikkei)\b"],
            "China":     [r"\b(china|chinese|csi|hang seng)\b"],
            "Brazil":    [r"\b(brazil|brasil|ibov|bovespa|b3)\b"],
            "Japan":     [r"\b(japan|nikkei|topix|日本)\b"],
            "India":     [r"\b(india|nifty|sensex)\b"],
            "Korea":     [r"\b(korea|kospi)\b"],
            "Latin America": [r"\b(latin america|latam)\b"],
        },
        "cap_size": {
            "Large-Cap": [r"\b(large.?cap|mega.?cap|s&p 500|top 20|top 50|large blend|large growth|large value)\b"],
            "Mid-Cap":   [r"\b(mid.?cap|s&p 400)\b"],
            "Small-Cap": [r"\b(small.?cap|s&p 600|russell 2000)\b"],
            "Micro-Cap": [r"\b(micro.?cap)\b"],
            "All-Cap":   [r"\b(total|all.?cap|total market|broad market|russell 3000)\b"],
        },
        "style": {
            "Growth":    [r"\b(growth)\b"],
            "Value":     [r"\b(value)\b"],
            "Blend":     [r"\b(blend|core)\b"],
            "Dividend":  [r"\b(dividend|yield|income|payout)\b"],
            "Momentum":  [r"\b(momentum)\b"],
            "Quality":   [r"\b(quality|wide moat)\b"],
            "Min Vol":   [r"\b(min.?vol|low.?vol|minimum volatility)\b"],
        },
        "sector": {
            "Technology":   [r"\b(tech|semiconductor|software|cloud|cyber|internet|ai|artificial)\b"],
            "Healthcare":   [r"\b(health|biotech|pharma|genomic|medical)\b"],
            "Financials":   [r"\b(financ|bank|insurance|fintech)\b"],
            "Energy":       [r"\b(energy|oil|gas|clean energy|solar|wind|uranium|nuclear)\b"],
            "Materials":    [r"\b(material|mining|metal|steel|chemical)\b"],
            "Industrials":  [r"\b(industrial|aerospace|defense|transport|infrastructure)\b"],
            "Consumer Disc.": [r"\b(consumer discretionary|retail|luxury|e.?commerce)\b"],
            "Consumer Staples": [r"\b(consumer staples|food|beverage)\b"],
            "Utilities":    [r"\b(utilit)\b"],
            "Real Estate":  [r"\b(real estate|reit|property)\b"],
            "Communications": [r"\b(communication|media|telecom)\b"],
        },
        "strategy": {
            "Passive":   [r"\b(index|track|s&p|nasdaq|russell|msci|ftse)\b"],
            "Active":    [r"\b(active|ark|managed)\b"],
            "Leveraged": [r"\b(leverag|2x|3x|ultra|bull)\b"],
            "Inverse":   [r"\b(inverse|short|bear|-1x|-2x|-3x)\b"],
            "Factor":    [r"\b(factor|smart beta|multifactor|equal.?weight)\b"],
            "Thematic":  [r"\b(thematic|innovation|disrupt|robot|autonomous|space|cyber|cannabis|esport|gaming|metaverse)\b"],
            "ESG":       [r"\b(esg|sri|sustain|green|clean|responsib|carbon|climate)\b"],
        },
        "index": {
            "S&P 500":     [r"\bs&?p\s*500\b"],
            "Nasdaq 100":  [r"\bnasdaq.?100\b"],
            "Russell 2000": [r"\brussell\s*2000\b"],
            "Russell 1000": [r"\brussell\s*1000\b"],
            "Russell 3000": [r"\brussell\s*3000\b"],
            "Dow Jones":   [r"\bdow\s*jones\b"],
            "MSCI EAFE":   [r"\bmsci\s*eafe\b"],
            "MSCI EM":     [r"\bmsci\s*(em|emerging)\b"],
            "MSCI World":  [r"\bmsci\s*world\b"],
            "FTSE":        [r"\bftse\b"],
            "Ibovespa":    [r"\bib?ovespa\b"],
        },
    }

    # Regras baseadas na categoria do yfinance
    _CATEGORY_TAGS = {
        "Large Blend":   {"cap_size": "Large-Cap", "style": "Blend"},
        "Large Growth":  {"cap_size": "Large-Cap", "style": "Growth"},
        "Large Value":   {"cap_size": "Large-Cap", "style": "Value"},
        "Mid-Cap Blend": {"cap_size": "Mid-Cap", "style": "Blend"},
        "Mid-Cap Growth": {"cap_size": "Mid-Cap", "style": "Growth"},
        "Mid-Cap Value": {"cap_size": "Mid-Cap", "style": "Value"},
        "Small Blend":   {"cap_size": "Small-Cap", "style": "Blend"},
        "Small Growth":  {"cap_size": "Small-Cap", "style": "Growth"},
        "Small Value":   {"cap_size": "Small-Cap", "style": "Value"},
        "Technology":    {"sector": "Technology", "asset_class": "Equity"},
        "Health":        {"sector": "Healthcare", "asset_class": "Equity"},
        "Financial":     {"sector": "Financials", "asset_class": "Equity"},
        "Natural Resources": {"sector": "Energy", "asset_class": "Equity"},
        "Real Estate":   {"sector": "Real Estate", "asset_class": "Equity"},
        "Industrials":   {"sector": "Industrials", "asset_class": "Equity"},
        "Communications": {"sector": "Communications", "asset_class": "Equity"},
        "Utilities":     {"sector": "Utilities", "asset_class": "Equity"},
        "Consumer Cyclical": {"sector": "Consumer Disc.", "asset_class": "Equity"},
        "Consumer Defensive": {"sector": "Consumer Staples", "asset_class": "Equity"},
        "Commodities Focused": {"asset_class": "Commodity"},
        "Trading--Leveraged Equity": {"strategy": "Leveraged", "asset_class": "Equity"},
        "Trading--Inverse Equity":  {"strategy": "Inverse", "asset_class": "Equity"},
        "Foreign Large Blend": {"geography": "International", "cap_size": "Large-Cap", "style": "Blend"},
        "Foreign Large Growth": {"geography": "International", "cap_size": "Large-Cap", "style": "Growth"},
        "Foreign Large Value":  {"geography": "International", "cap_size": "Large-Cap", "style": "Value"},
        "Diversified Emerging Mkts": {"geography": "Emerging", "asset_class": "Equity"},
        "China Region":  {"geography": "China", "asset_class": "Equity"},
        "Europe Stock":  {"geography": "Europe", "asset_class": "Equity"},
        "India Equity":  {"geography": "India", "asset_class": "Equity"},
        "Japan Stock":   {"geography": "Japan", "asset_class": "Equity"},
        "Latin America Stock": {"geography": "Latin America", "asset_class": "Equity"},
        "Miscellaneous Region": {"geography": "International"},
        "Miscellaneous Sector": {},
    }

    def auto_tag_etf(self, ticker: str) -> List[Dict[str, str]]:
        """Gera tags automáticas para um ETF baseado em nome, categoria e holdings."""
        tags = []
        now = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT name, category, subcategory, issuer, data_source FROM etfs WHERE ticker = ?",
                (ticker,),
            ).fetchone()

        if not row:
            return tags

        name, category, subcategory, issuer, data_source = row
        name = name or ""
        category = category or ""

        # 1) Tags baseadas na categoria do yfinance
        if category in self._CATEGORY_TAGS:
            for tag_type, tag_value in self._CATEGORY_TAGS[category].items():
                tags.append({"tag_type": tag_type, "tag_value": tag_value,
                             "confidence": 0.9, "source": "yfinance_category"})

        # 2) Tags baseadas em keywords no nome
        name_lower = name.lower()
        for tag_type, rules in self._TAG_RULES.items():
            for tag_value, patterns in rules.items():
                for pattern in patterns:
                    if _re.search(pattern, name_lower):
                        tags.append({"tag_type": tag_type, "tag_value": tag_value,
                                     "confidence": 0.8, "source": "name_keyword"})
                        break

        # 3) Tag de issuer
        if issuer:
            tags.append({"tag_type": "issuer", "tag_value": issuer,
                         "confidence": 1.0, "source": "yfinance"})

        # 4) Tag de ticker suffix para geografia
        if ticker.endswith(".SA"):
            tags.append({"tag_type": "geography", "tag_value": "Brazil",
                         "confidence": 1.0, "source": "ticker_suffix"})
        elif ticker.endswith(".L"):
            tags.append({"tag_type": "geography", "tag_value": "UK",
                         "confidence": 1.0, "source": "ticker_suffix"})

        # Deduplica (mesmo tag_type + tag_value)
        seen = set()
        unique_tags = []
        for t in tags:
            key = (t["tag_type"], t["tag_value"])
            if key not in seen:
                seen.add(key)
                t["created_at"] = now
                unique_tags.append(t)

        return unique_tags

    def save_tags(self, ticker: str, tags: List[Dict[str, str]]) -> int:
        """Salva tags de um ETF (merge com existentes)."""
        count = 0
        with sqlite3.connect(self.db_path) as conn:
            for t in tags:
                try:
                    conn.execute("""
                        INSERT INTO etf_tags (etf_ticker, tag_type, tag_value,
                                              confidence, source, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(etf_ticker, tag_type, tag_value) DO UPDATE SET
                            confidence=excluded.confidence,
                            source=excluded.source
                    """, (ticker, t["tag_type"], t["tag_value"],
                          t.get("confidence", 1.0), t.get("source", "auto"),
                          t.get("created_at", datetime.now().isoformat())))
                    count += 1
                except Exception:
                    pass
            conn.commit()
        return count

    def auto_tag_all(self) -> Dict[str, Any]:
        """Aplica auto-tagging a todos os ETFs no banco."""
        with sqlite3.connect(self.db_path) as conn:
            tickers = [r[0] for r in conn.execute("SELECT ticker FROM etfs").fetchall()]

        total_tags = 0
        for ticker in tickers:
            tags = self.auto_tag_etf(ticker)
            if tags:
                total_tags += self.save_tags(ticker, tags)

        log.info(f"Auto-tagging concluído: {total_tags} tags para {len(tickers)} ETFs")
        return {"etfs": len(tickers), "total_tags": total_tags}

    def get_tags(self, ticker: str) -> List[Dict[str, Any]]:
        """Retorna tags de um ETF."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT tag_type, tag_value, confidence, source FROM etf_tags WHERE etf_ticker = ? ORDER BY tag_type",
                (ticker,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_tag_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas das tags."""
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM etf_tags").fetchone()[0]
            by_type = conn.execute(
                "SELECT tag_type, COUNT(DISTINCT tag_value), COUNT(*) FROM etf_tags GROUP BY tag_type ORDER BY COUNT(*) DESC"
            ).fetchall()
            etfs_tagged = conn.execute("SELECT COUNT(DISTINCT etf_ticker) FROM etf_tags").fetchone()[0]
        return {
            "total_tags": total,
            "etfs_tagged": etfs_tagged,
            "by_type": [{"type": t, "unique_values": uv, "total": c} for t, uv, c in by_type],
        }

    # ── Interface BaseExtractor ──────────────────────────────────
    def extract_data(self, **kwargs) -> Dict[str, Any]:
        ticker = kwargs.get("ticker")
        if ticker:
            return self.process_etf(ticker)
        return self.bulk_process()

    def get_latest_data(self, data_type: str) -> Dict[str, Any]:
        return self.format_data(self.get_stats(), data_type)


# ────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────
def _pct(val):
    """Converte ratio (0.0012) para percentual (0.12) se necessário."""
    if val is None:
        return None
    return round(float(val) * 100, 4) if abs(float(val)) < 1 else round(float(val), 4)


def _is_nan(v):
    try:
        import math
        return v is None or math.isnan(float(v))
    except (TypeError, ValueError):
        return True
