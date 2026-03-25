#!/usr/bin/env python3
"""
discover_new_tickers.py — Descobre tickers que existem no mundo mas ainda não estão na base.

Fontes de descoberta:
  1. Damodaran Excel (anual) — diff contra base atual
  2. ETF Holdings (mensal) — tickers em holdings que não estão na CBD
  3. yfinance Screener (sob demanda) — consulta por market/setor

Uso:
  python scripts/discover_new_tickers.py --source damodaran  # Compara Excel vs DB
  python scripts/discover_new_tickers.py --source etf         # Tickers em ETFs fora da DB
  python scripts/discover_new_tickers.py --source all          # Todas as fontes
  python scripts/discover_new_tickers.py --source damodaran --import  # Importa os novos
  python scripts/discover_new_tickers.py --report              # Relatório de cobertura
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

DB_PATH = Path("data/damodaran_data_new.db")
DAMODARAN_BASE_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/"
CACHE_DIR = Path("cache")


# ─── Fonte 1: Damodaran Excel ─────────────────────────────────────────────────

def discover_from_damodaran(db_path: Path, year: int | None = None) -> dict:
    """Compara Excel do Damodaran com a base atual. Retorna novos tickers."""
    conn = sqlite3.connect(str(db_path))

    # Determinar ano do arquivo
    if year is None:
        year = datetime.now().year
    
    excel_path = Path(f"data/damodaran_data/globalcompfirms{year}.xlsx")
    
    if not excel_path.exists():
        print(f"Baixando globalcompfirms{year}.xlsx ...")
        url = f"{DAMODARAN_BASE_URL}globalcompfirms{year}.xlsx"
        resp = requests.get(url, timeout=120, stream=True)
        if resp.status_code != 200:
            print(f"  Erro {resp.status_code} ao baixar. Tentando ano anterior...")
            year -= 1
            url = f"{DAMODARAN_BASE_URL}globalcompfirms{year}.xlsx"
            resp = requests.get(url, timeout=120, stream=True)
            if resp.status_code != 200:
                return {"error": f"Excel não encontrado para {year} nem {year+1}"}
        excel_path = Path(f"data/damodaran_data/globalcompfirms{year}.xlsx")
        excel_path.parent.mkdir(parents=True, exist_ok=True)
        with open(excel_path, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        print(f"  Salvo: {excel_path}")
    
    print(f"Lendo {excel_path.name}...")
    df = pd.read_excel(excel_path, sheet_name=0, engine="openpyxl")
    
    # Detectar coluna de company_name/ticker
    name_col = None
    for c in df.columns:
        if "company" in str(c).lower() and "name" in str(c).lower():
            name_col = c
            break
    if name_col is None:
        name_col = df.columns[0]  # primeira coluna geralmente é company_name
    
    # Tickers no Excel
    excel_tickers = set()
    excel_companies = {}
    for _, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name or name == "nan":
            continue
        # Extrair ticker do nome (formato "Company Name (EXCHANGE:TICKER)")
        match = re.search(r'\(([^)]+):([^)]+)\)', name)
        if match:
            exchange = match.group(1)
            ticker = match.group(2).strip()
            full_ticker = f"{exchange}:{ticker}"
            excel_tickers.add(full_ticker)
            excel_companies[full_ticker] = name
    
    # Tickers na base
    db_tickers = set()
    rows = conn.execute("SELECT ticker FROM company_basic_data WHERE ticker IS NOT NULL").fetchall()
    for r in rows:
        db_tickers.add(r[0])
    
    # Também por damodaran_global
    dg_tickers = set()
    rows = conn.execute("SELECT ticker FROM damodaran_global WHERE ticker IS NOT NULL").fetchall()
    for r in rows:
        dg_tickers.add(r[0])
    
    new_tickers = excel_tickers - db_tickers - dg_tickers
    
    result = {
        "source": "damodaran",
        "excel_year": year,
        "excel_total": len(excel_tickers),
        "db_total": len(db_tickers),
        "new_count": len(new_tickers),
        "new_tickers": sorted(new_tickers)[:200],  # limitar output
        "new_companies": {t: excel_companies[t] for t in sorted(new_tickers)[:50]},
    }
    
    conn.close()
    return result


# ─── Fonte 2: ETF Holdings ────────────────────────────────────────────────────

def discover_from_etf_holdings(db_path: Path) -> dict:
    """Descobre tickers que aparecem em holdings de ETFs mas não estão na base."""
    conn = sqlite3.connect(str(db_path))
    
    # Verificar se tabela de holdings existe
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    
    holdings_table = None
    for t in tables:
        if "holding" in t.lower():
            holdings_table = t
            break
    
    if not holdings_table:
        # Buscar em ETF holdings cache files
        return _discover_from_etf_cache(conn, db_path)
    
    # Tickers em holdings
    holding_tickers = set()
    rows = conn.execute(f"SELECT DISTINCT ticker FROM {holdings_table} WHERE ticker IS NOT NULL").fetchall()
    for r in rows:
        holding_tickers.add(r[0])
    
    # Tickers na base
    db_yahoo_codes = set()
    rows = conn.execute("SELECT yahoo_code FROM company_basic_data WHERE yahoo_code IS NOT NULL").fetchall()
    for r in rows:
        db_yahoo_codes.add(r[0])
    
    db_tickers = set()
    rows = conn.execute("SELECT ticker FROM company_basic_data WHERE ticker IS NOT NULL").fetchall()
    for r in rows:
        db_tickers.add(r[0])
    
    new_from_holdings = holding_tickers - db_yahoo_codes - db_tickers
    
    conn.close()
    return {
        "source": "etf_holdings",
        "holdings_total": len(holding_tickers),
        "new_count": len(new_from_holdings),
        "new_tickers": sorted(new_from_holdings)[:200],
    }


def _discover_from_etf_cache(conn: sqlite3.Connection, db_path: Path) -> dict:
    """Fallback: busca em arquivos JSON de cache de ETF holdings."""
    # Tickers na base
    db_yahoo_codes = set(r[0] for r in conn.execute(
        "SELECT yahoo_code FROM company_basic_data WHERE yahoo_code IS NOT NULL"
    ).fetchall())
    
    # Buscar em cache JSON
    holding_tickers = set()
    cache_files = list(CACHE_DIR.glob("*holdings*.json")) + list(CACHE_DIR.glob("*etf*.json"))
    
    for cf in cache_files:
        try:
            data = json.loads(cf.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "ticker" in item:
                        holding_tickers.add(item["ticker"])
            elif isinstance(data, dict):
                for key, val in data.items():
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict) and "ticker" in item:
                                holding_tickers.add(item["ticker"])
        except Exception:
            continue
    
    new_tickers = holding_tickers - db_yahoo_codes
    return {
        "source": "etf_cache",
        "holdings_total": len(holding_tickers),
        "new_count": len(new_tickers),
        "new_tickers": sorted(new_tickers)[:200],
        "cache_files_scanned": len(cache_files),
    }


# ─── Fonte 3: yfinance Screener ───────────────────────────────────────────────

def discover_from_screener(db_path: Path, market: str = "us_market") -> dict:
    """Usa yfinance screener para descobrir tickers por mercado."""
    try:
        import yfinance as yf
    except ImportError:
        return {"error": "yfinance não instalado"}
    
    conn = sqlite3.connect(str(db_path))
    db_yahoo_codes = set(r[0] for r in conn.execute(
        "SELECT yahoo_code FROM company_basic_data WHERE yahoo_code IS NOT NULL"
    ).fetchall())
    
    screened = set()
    try:
        # yfinance Screener (disponível em versões recentes)
        screener = yf.Screener()
        screener.set_default_body({"offset": 0, "size": 250, "query": market})
        result = screener.response
        if result and "quotes" in result:
            for q in result["quotes"]:
                sym = q.get("symbol")
                if sym:
                    screened.add(sym)
    except Exception as e:
        # Fallback: usar predefined screeners
        try:
            sc = yf.Screener()
            sc.set_predefined_body(market)
            resp = sc.response
            if resp and isinstance(resp, dict):
                for q in resp.get("quotes", []):
                    sym = q.get("symbol")
                    if sym:
                        screened.add(sym)
        except Exception as e2:
            conn.close()
            return {
                "source": "screener",
                "error": f"Screener não disponível: {e2}",
                "tip": "yfinance screener requer versão >= 0.2.31",
            }
    
    new_tickers = screened - db_yahoo_codes
    conn.close()
    return {
        "source": "screener",
        "market": market,
        "screened_total": len(screened),
        "new_count": len(new_tickers),
        "new_tickers": sorted(new_tickers)[:200],
    }


# ─── Relatório de Cobertura ───────────────────────────────────────────────────

def coverage_report(db_path: Path) -> None:
    """Gera relatório de cobertura atual da base."""
    conn = sqlite3.connect(str(db_path))
    
    total_cbd = conn.execute("SELECT COUNT(*) FROM company_basic_data").fetchone()[0]
    with_yahoo = conn.execute("SELECT COUNT(*) FROM company_basic_data WHERE yahoo_code IS NOT NULL AND yahoo_code != ''").fetchone()[0]
    with_about = conn.execute("SELECT COUNT(*) FROM company_basic_data WHERE about IS NOT NULL").fetchone()[0]
    with_hist = conn.execute("SELECT COUNT(DISTINCT company_basic_data_id) FROM company_financials_historical WHERE period_type='annual'").fetchone()[0]
    total_hist = conn.execute("SELECT COUNT(*) FROM company_financials_historical").fetchone()[0]
    
    # Por país — cobertura
    print("=" * 70)
    print("RELATÓRIO DE COBERTURA DA BASE")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)
    print(f"\n{'Métrica':<40s} {'Valor':>10s} {'%':>8s}")
    print("-" * 60)
    print(f"{'Total empresas':<40s} {total_cbd:>10,}")
    print(f"{'Com yahoo_code':<40s} {with_yahoo:>10,} {100*with_yahoo/total_cbd:>7.1f}%")
    print(f"{'Com about (Yahoo reconhece)':<40s} {with_about:>10,} {100*with_about/total_cbd:>7.1f}%")
    print(f"{'Com dados históricos annual':<40s} {with_hist:>10,} {100*with_hist/total_cbd:>7.1f}%")
    print(f"{'Total registros históricos':<40s} {total_hist:>10,}")
    
    # Top 15 países — cobertura histórica
    print(f"\n{'País':<30s} {'Total':>7s} {'C/Hist':>7s} {'%':>7s}")
    print("-" * 55)
    rows = conn.execute("""
        SELECT cbd.country, COUNT(*) as total,
               SUM(CASE WHEN cfh.cbid IS NOT NULL THEN 1 ELSE 0 END) as with_h
        FROM company_basic_data cbd
        LEFT JOIN (SELECT DISTINCT company_basic_data_id as cbid FROM company_financials_historical) cfh
            ON cfh.cbid = cbd.id
        WHERE cbd.country IS NOT NULL
        GROUP BY cbd.country
        ORDER BY total DESC
        LIMIT 15
    """).fetchall()
    for country, total, with_h in rows:
        pct = 100 * with_h / total if total else 0
        print(f"  {str(country):<28s} {total:>7,} {with_h:>7,} {pct:>6.1f}%")
    
    # Exchanges sem cobertura (>80% missing)
    print(f"\n{'Bolsa sem cobertura (>80% missing)':<35s} {'Total':>7s} {'Sem':>7s} {'%':>7s}")
    print("-" * 60)
    rows = conn.execute("""
        SELECT dg.exchange, COUNT(*) as total,
               SUM(CASE WHEN cfh.cbid IS NULL THEN 1 ELSE 0 END) as without_h
        FROM damodaran_global dg
        JOIN company_basic_data cbd ON cbd.damodaran_company_id = dg.id
        LEFT JOIN (SELECT DISTINCT company_basic_data_id as cbid FROM company_financials_historical) cfh
            ON cfh.cbid = cbd.id
        WHERE dg.exchange IS NOT NULL
        GROUP BY dg.exchange
        HAVING total >= 10 AND (without_h * 100.0 / total) > 80
        ORDER BY without_h DESC
        LIMIT 20
    """).fetchall()
    for ex, total, without_h in rows:
        pct = 100 * without_h / total if total else 0
        print(f"  {str(ex):<33s} {total:>7,} {without_h:>7,} {pct:>6.1f}%")
    
    # Última atualização
    last_update = conn.execute("""
        SELECT MAX(updated_at) FROM company_basic_data
    """).fetchone()[0]
    last_hist = conn.execute("""
        SELECT MAX(fetched_at) FROM company_financials_historical
    """).fetchone()[0]
    print(f"\nÚltima atualização CBD: {last_update}")
    print(f"Último registro histórico: {last_hist}")
    
    conn.close()


# ─── Importação de novos tickers ──────────────────────────────────────────────

def import_new_from_damodaran(db_path: Path, year: int | None = None) -> int:
    """Importa novos tickers do Damodaran para damodaran_global e company_basic_data."""
    result = discover_from_damodaran(db_path, year)
    if "error" in result:
        print(f"Erro: {result['error']}")
        return 0
    
    if result["new_count"] == 0:
        print("Nenhum ticker novo encontrado no Damodaran.")
        return 0
    
    print(f"\n{result['new_count']} tickers novos encontrados. Importando...")
    print("Para importar, execute:")
    print(f"  python scripts/extract_global_damodaran.py")
    print(f"  python scripts/sync_company_basic_data.py")
    print(f"  python scripts/normalize_company_yahoo_codes.py")
    print(f"  python scripts/update_company_data_from_yahoo_fast.py --workers 4 --max-rps 3")
    print(f"  python scripts/fetch_historical_financials.py --workers 3 --max-rps 2")
    
    return result["new_count"]


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Descobre tickers que existem no mundo mas não estão na base.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python scripts/discover_new_tickers.py --source damodaran
  python scripts/discover_new_tickers.py --source etf
  python scripts/discover_new_tickers.py --source all
  python scripts/discover_new_tickers.py --report
        """,
    )
    parser.add_argument(
        "--source",
        choices=["damodaran", "etf", "screener", "all"],
        default="all",
        help="Fonte de descoberta (default: all)",
    )
    parser.add_argument("--report", action="store_true", help="Gera relatório de cobertura")
    parser.add_argument("--year", type=int, help="Ano do Excel Damodaran")
    parser.add_argument("--market", default="most_actives", help="Mercado para screener (default: most_actives)")
    parser.add_argument("--import-new", action="store_true", dest="do_import", help="Importa novos tickers encontrados")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Caminho do banco SQLite")
    parser.add_argument("--save", help="Salva resultado em arquivo JSON")
    args = parser.parse_args()
    
    db_path = Path(args.db_path)
    
    if args.report:
        coverage_report(db_path)
        return
    
    results = {}
    sources = [args.source] if args.source != "all" else ["damodaran", "etf", "screener"]
    
    for source in sources:
        print(f"\n{'='*60}")
        print(f"FONTE: {source.upper()}")
        print(f"{'='*60}")
        
        if source == "damodaran":
            r = discover_from_damodaran(db_path, args.year)
        elif source == "etf":
            r = discover_from_etf_holdings(db_path)
        elif source == "screener":
            r = discover_from_screener(db_path, args.market)
        else:
            continue
        
        results[source] = r
        
        if "error" in r:
            print(f"  ERRO: {r['error']}")
        else:
            print(f"  Novos tickers encontrados: {r['new_count']:,}")
            if r.get("new_companies"):
                print(f"  Exemplos:")
                for t, name in list(r["new_companies"].items())[:10]:
                    print(f"    {t:25s} | {name[:60]}")
            elif r.get("new_tickers"):
                print(f"  Primeiros 20: {r['new_tickers'][:20]}")
    
    # Resumo
    total_new = sum(r.get("new_count", 0) for r in results.values() if "error" not in r)
    print(f"\n{'='*60}")
    print(f"TOTAL DE NOVOS TICKERS DESCOBERTOS: {total_new:,}")
    print(f"{'='*60}")
    
    if args.save:
        # Serializar para JSON
        save_data = {}
        for k, v in results.items():
            save_data[k] = {kk: vv for kk, vv in v.items()}
        Path(args.save).write_text(json.dumps(save_data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Resultado salvo em: {args.save}")
    
    if args.do_import and "damodaran" in results:
        import_new_from_damodaran(db_path, args.year)


if __name__ == "__main__":
    main()
