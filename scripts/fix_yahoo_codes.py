"""
Corrige yahoo_code das empresas usando yf.Search pelo nome da empresa.

Para exchanges onde o yahoo_code atual não funciona (BSE com scrip codes numéricos,
KLSE com códigos invertidos, IBSE/OM/CNSX etc. sem sufixo), busca o símbolo correto
do Yahoo Finance pelo nome da empresa.

Uso:
    python scripts/fix_yahoo_codes.py --limit 500
    python scripts/fix_yahoo_codes.py --exchanges BSE,KLSE --limit 1000
    python scripts/fix_yahoo_codes.py --dry-run --limit 50
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Fix SSL para caminhos com espaços (OneDrive)
_cacert = Path(r"C:\cacerts\cacert.pem")
if _cacert.exists() and "CURL_CA_BUNDLE" not in os.environ:
    os.environ["CURL_CA_BUNDLE"] = str(_cacert)

import yfinance as yf

# Mapeamento de exchange do banco → sufixo preferido no Yahoo
EXCHANGE_PREFERRED_SUFFIX: dict[str, str] = {
    "BSE": ".BO",
    "NSEI": ".NS",
    "NSE": ".NS",
    "KLSE": ".KL",
    "IDX": ".JK",
    "OM": ".ST",
    "WSE": ".WA",
    "ENXTPA": ".PA",
    "TSX": ".TO",
    "TSXV": ".V",
    "CNSX": ".CN",
    "IBSE": ".IS",
    "AIM": ".L",
    "LSE": ".L",
    "TASE": ".TA",
    "HOSE": ".VN",
    "HNX": ".VN",
    "KOSDAQ": ".KQ",
    "KOSE": ".KS",
    "KOSPI": ".KS",
    "KRX": ".KS",
    "XKON": ".KQ",
    "BIT": ".MI",
    "BME": ".MC",
    "XTRA": ".DE",
    "ENXTBR": ".BR",
    "ENXTAM": ".AS",
    "ENXTLS": ".LS",
    "SWX": ".SW",
    "SIX": ".SW",
    "HLSE": ".HE",
    "SGX": ".SI",
    "Catalist": ".SI",
    "SET": ".BK",
    "JSE": ".JO",
    "SASE": ".SR",
    "OB": ".OL",
    "DB": ".DE",
    "BVB": ".RO",
    "PSE": ".PS",
    "DSE": ".BD",
    "COSE": ".CM",
    "SNSE": ".SN",
    "ASE": ".AT",
    "BMV": ".MX",
    "BOVESPA": ".SA",
    "B3": ".SA",
    "SEHK": ".HK",
    "ASX": ".AX",
    "TWSE": ".TW",
    "TPEX": ".TWO",
    "TWO": ".TWO",
    "SHSE": ".SS",
    "SSE": ".SS",
    "SZSE": ".SZ",
    "NZSE": ".NZ",
    "NGSE": ".LG",
    "BVL": ".LS",
    "WBAG": ".VI",
    "NGM": ".ST",
    "NYSEAM": "",
    "NasdaqGS": "",
    "NasdaqCM": "",
    "NasdaqGM": "",
    "NYSE": "",
    "OTCPK": "",
}


def _extract_exchange(ticker: str | None) -> str | None:
    if not ticker or ":" not in ticker:
        return None
    return ticker.split(":", 1)[0].strip()


def _clean_company_name(name: str | None) -> str | None:
    if not name:
        return None
    clean = name.strip()
    # Remover parte entre parênteses no final (geralmente o ticker)
    paren = clean.rfind("(")
    if paren > 0:
        clean = clean[:paren].strip()
    return clean if clean else None


def _test_yahoo_code(yahoo_code: str) -> tuple[bool, dict]:
    """Testa se um yahoo_code funciona e retorna info."""
    try:
        ticker = yf.Ticker(yahoo_code)
        info = ticker.get_info()
        qt = info.get("quoteType", "")
        # quoteType NONE ou vazio significa que não reconhece
        if qt and qt not in ("NONE", "MUTUALFUND"):
            return True, info
        # MUTUALFUND pode ter dados, verificar
        if qt == "MUTUALFUND" and info.get("longBusinessSummary"):
            return True, info
        return False, info
    except Exception:
        return False, {}


def search_correct_yahoo_code(
    company_name: str | None,
    exchange: str | None,
    max_results: int = 5,
) -> str | None:
    """Busca o símbolo correto via yf.Search pelo nome da empresa."""
    clean = _clean_company_name(company_name)
    if not clean:
        return None

    try:
        search = yf.Search(clean, max_results=max_results)
        quotes = getattr(search, "quotes", []) or []
    except Exception:
        return None

    if not quotes:
        return None

    preferred_suffix = EXCHANGE_PREFERRED_SUFFIX.get(exchange or "", "")

    # Prioridade 1: símbolo com o sufixo preferido da exchange
    for item in quotes:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol", "")
        if preferred_suffix and symbol.endswith(preferred_suffix):
            return symbol

    # Prioridade 2: qualquer símbolo que pareça válido (não PNK/OTC se exchange não é OTC)
    for item in quotes:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol", "")
        exch = item.get("exchange", "")
        # Evitar mercados OTC como fallback para empresas listadas em bolsa
        if exch in ("PNK", "OQB") and exchange not in ("OTCPK", "OTC"):
            continue
        if symbol:
            return symbol

    # Prioridade 3: qualquer resultado
    for item in quotes:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol", "")
        if symbol:
            return symbol

    return None


def fetch_candidates(
    conn: sqlite3.Connection,
    limit: int | None,
    exchanges: list[str] | None,
    only_broken: bool,
) -> list[tuple[int, str | None, str | None, str | None]]:
    """Busca registros que precisam de correção do yahoo_code."""
    cursor = conn.cursor()

    where = "WHERE 1=1"
    params: list[str] = []

    if only_broken:
        # Apenas empresas sem about (proxy para yahoo_code quebrado)
        where += " AND (about IS NULL OR TRIM(about) = '')"

    if exchanges:
        normalized = [ex.strip() for ex in exchanges if ex.strip()]
        if normalized:
            conditions = []
            for ex in normalized:
                conditions.append("ticker LIKE ?")
                params.append(f"{ex}:%")
            where += f" AND ({' OR '.join(conditions)})"

    sql = f"""
        SELECT id, yahoo_code, ticker, company_name
        FROM company_basic_data
        {where}
        ORDER BY id
    """
    if limit and limit > 0:
        sql += f" LIMIT {int(limit)}"

    return cursor.execute(sql, params).fetchall()


def fix_codes(
    conn: sqlite3.Connection,
    rows: list[tuple[int, str | None, str | None, str | None]],
    sleep_seconds: float,
    dry_run: bool,
) -> tuple[int, int, int]:
    """Corrige yahoo_codes via Search. Retorna (corrigidos, já_ok, falha)."""
    cursor = conn.cursor()
    fixed = 0
    already_ok = 0
    failed = 0

    total = len(rows)
    for i, (row_id, yahoo_code, ticker, company_name) in enumerate(rows, 1):
        exchange = _extract_exchange(ticker)

        if i % 100 == 0 or i == 1:
            print(f"  Progresso: {i}/{total} (corrigidos={fixed}, ok={already_ok}, falha={failed})")

        # Primeiro testar se o yahoo_code atual funciona
        if yahoo_code and yahoo_code.strip():
            works, _ = _test_yahoo_code(yahoo_code)
            if works:
                already_ok += 1
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds * 0.5)  # sleep menor para os que já funcionam
                continue

        # Yahoo_code não funciona, buscar correto via search
        new_code = search_correct_yahoo_code(company_name, exchange)

        if new_code:
            if not dry_run:
                cursor.execute(
                    """UPDATE company_basic_data
                       SET yahoo_code = ?, updated_at = CURRENT_TIMESTAMP
                       WHERE id = ?""",
                    (new_code, row_id),
                )
            clean = _clean_company_name(company_name) or ""
            print(f"    FIX: [{row_id}] {yahoo_code} -> {new_code} ({clean[:40]})")
            fixed += 1
        else:
            failed += 1

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        # Commit intermediário a cada 50 registros para não perder progresso
        if not dry_run and i % 50 == 0:
            conn.commit()

    return fixed, already_ok, failed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corrige yahoo_code de empresas usando yf.Search pelo nome."
    )
    parser.add_argument("--db-path", default="data/damodaran_data_new.db")
    parser.add_argument("--limit", type=int, default=500,
                        help="Máximo de registros a processar por execução (padrão: 500)")
    parser.add_argument("--sleep", type=float, default=0.3,
                        help="Pausa entre requisições em segundos (padrão: 0.3)")
    parser.add_argument("--exchanges", type=str, default="",
                        help="Lista de exchanges separadas por vírgula (vazio=todas)")
    parser.add_argument("--all", action="store_true",
                        help="Processa todos, inclusive os que já possuem about")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sem gravar no banco")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        exchanges = [x.strip() for x in args.exchanges.split(",") if x.strip()] if args.exchanges else None

        print(f"Buscando candidatos...")
        rows = fetch_candidates(conn, args.limit, exchanges, only_broken=not args.all)
        if not rows:
            print("Nenhum registro elegível.")
            return

        print(f"Registros a processar: {len(rows)}")
        if exchanges:
            print(f"Exchanges: {', '.join(exchanges)}")
        if args.dry_run:
            print("*** MODO DRY-RUN — nenhuma alteração será gravada ***")
        print()

        fixed, ok, failed = fix_codes(conn, rows, args.sleep, args.dry_run)

        if not args.dry_run:
            conn.commit()

        print()
        print(f"Corrigidos: {fixed}")
        print(f"Já funcionavam: {ok}")
        print(f"Sem resultado no Yahoo: {failed}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
