"""
Migração segura: expansão da base Damodaran para incluir TODAS as empresas
(financeiras + não-financeiras) usando globalcompfirms2026.xlsx.

Antes: globalcompnonfinfirms2026.xlsx (~42k empresas, exclui financeiras)
Depois: globalcompfirms2026.xlsx (~48k empresas, base completa)

Etapas:
1. Lê o Excel completo (globalcompfirms2026.xlsx)
2. Normaliza para schema da tabela damodaran_global
3. Cria staging table
4. Valida dados (contagem, completude, cobertura)
5. Identifica empresas novas (financeiras)
6. Executa swap atômico com backup

Uso:
  # Dry-run (padrão) - sem alterações no banco
  python scripts/migrate_full_globalcomp.py

  # Executar de fato
  python scripts/migrate_full_globalcomp.py --execute

  # Forçar mesmo com validações falhando
  python scripts/migrate_full_globalcomp.py --execute --force
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass
class ValidationResult:
    name: str
    passed: bool
    details: str


def normalize_column_name(column_name: str) -> str:
    text = str(column_name).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def pick_column(columns: list[str], patterns: list[str]) -> str | None:
    normalized = {col: normalize_column_name(col) for col in columns}
    for pattern in patterns:
        regex = re.compile(pattern)
        for original, norm in normalized.items():
            if regex.search(norm):
                return original
    return None


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def split_exchange_ticker(value: str | None) -> tuple[str | None, str | None]:
    if value is None or pd.isna(value):
        return None, None
    text = str(value).strip()
    if not text:
        return None, None
    if ":" in text:
        exchange, ticker = text.split(":", 1)
        return exchange.strip(), ticker.strip()
    return None, text


def build_dataframe_for_db(df_raw: pd.DataFrame, year: int) -> pd.DataFrame:
    columns = list(df_raw.columns)

    column_candidates = {
        "company_name": [r"^company_name$"],
        "exchange_ticker": [r"^exchange_ticker$"],
        "industry_group": [r"^industry_group$"],
        "primary_sector": [r"^primary_sector$"],
        "sic_code": [r"^sic_code$"],
        "country": [r"^country$"],
        "broad_group": [r"^broad_group$"],
        "sub_group": [r"^sub_group$"],
        "bottom_up_beta_sector": [r"^bottom_up_beta_for_sector$"],
        "erp_for_country": [r"^erp_for_country$"],
        "market_cap": [r"^market_cap_in_us$", r"^market_cap$"],
        "enterprise_value": [r"^enterprise_value_in_us$", r"^enterprise_value$"],
        "revenue": [r"^revenues$", r"^trailing_revenues$"],
        "net_income": [r"^net_income$", r"^trailing_net_income$"],
        "ebitda": [r"^ebitda$", r"^trailing_ebitda_adj_for_leases$"],
        "pe_ratio": [r"^current_pe$", r"^trailing_pe$", r"^pe_ratio$"],
        "beta": [r"^beta$"],
        "debt_equity": [r"^market_debt_to_equity_ratio$", r"^book_debt_to_equity_ratio$"],
        "roe": [r"^roe_adj_for_r_d$", r"^roe$", r"^return_on_equity$"],
        "roa": [r"^roa$"],
        "dividend_yield": [r"^dividend_yield$"],
        "revenue_growth": [r"^revenue_growth_last_5_yeers$", r"^historical_growth_in_revenues_last_5_years$"],
        "operating_margin": [r"^pre_tax_operating_margin$", r"^operating_margin_in_ltm_2022$"],
    }

    selected = {key: pick_column(columns, patterns) for key, patterns in column_candidates.items()}

    result = pd.DataFrame(index=df_raw.index)
    result["year"] = year

    company_col = selected["company_name"]
    ticker_col = selected["exchange_ticker"]

    result["company_name"] = df_raw[company_col] if company_col else None

    exchange_series = []
    ticker_series = []
    if ticker_col:
        for value in df_raw[ticker_col].tolist():
            raw_ticker = None
            if value is not None and not pd.isna(value):
                raw_ticker = str(value).strip() or None
            exchange, ticker = split_exchange_ticker(value)
            exchange_series.append(exchange)
            ticker_series.append(raw_ticker or ticker)
    else:
        exchange_series = [None] * len(df_raw)
        ticker_series = [None] * len(df_raw)

    result["ticker"] = ticker_series
    result["exchange"] = exchange_series

    for col in ["industry_group", "primary_sector", "country", "broad_group", "sub_group", "sic_code"]:
        source = selected[col]
        result[col] = df_raw[source] if source else None

    result["industry"] = result["industry_group"]

    numeric_map = {
        "bottom_up_beta_sector": "bottom_up_beta_sector",
        "erp_for_country": "erp_for_country",
        "market_cap": "market_cap",
        "enterprise_value": "enterprise_value",
        "revenue": "revenue",
        "net_income": "net_income",
        "ebitda": "ebitda",
        "pe_ratio": "pe_ratio",
        "beta": "beta",
        "debt_equity": "debt_equity",
        "roe": "roe",
        "roa": "roa",
        "dividend_yield": "dividend_yield",
        "revenue_growth": "revenue_growth",
        "operating_margin": "operating_margin",
    }

    for source_key, target_key in numeric_map.items():
        source_col = selected[source_key]
        if source_col:
            result[target_key] = to_numeric(df_raw[source_col])
        else:
            result[target_key] = None

    result["raw_data"] = result[["company_name", "ticker", "country", "industry_group"]].to_dict("records")
    result["raw_data"] = result["raw_data"].apply(lambda x: json.dumps(x, ensure_ascii=False))

    return result


def create_staging_table(conn: sqlite3.Connection, staging_table: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
    conn.execute(
        f"""
        CREATE TABLE {staging_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            company_name TEXT,
            ticker TEXT,
            exchange TEXT,
            industry TEXT,
            country TEXT,
            broad_group TEXT,
            erp_for_country REAL,
            industry_group TEXT,
            primary_sector TEXT,
            sub_group TEXT,
            sic_code TEXT,
            bottom_up_beta_sector REAL,
            market_cap REAL,
            enterprise_value REAL,
            revenue REAL,
            net_income REAL,
            ebitda REAL,
            pe_ratio REAL,
            beta REAL,
            debt_equity REAL,
            roe REAL,
            roa REAL,
            dividend_yield REAL,
            revenue_growth REAL,
            operating_margin REAL,
            raw_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def insert_staging(conn: sqlite3.Connection, df: pd.DataFrame, staging_table: str) -> int:
    insert_cols = [
        "year", "company_name", "ticker", "exchange", "industry", "country",
        "broad_group", "erp_for_country", "industry_group", "primary_sector",
        "sub_group", "sic_code", "bottom_up_beta_sector", "market_cap",
        "enterprise_value", "revenue", "net_income", "ebitda", "pe_ratio",
        "beta", "debt_equity", "roe", "roa", "dividend_yield",
        "revenue_growth", "operating_margin", "raw_data",
    ]
    insert_df = df[insert_cols].copy()
    insert_df.to_sql(staging_table, conn, if_exists="append", index=False)
    count = conn.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()[0]
    return int(count)


def run_validations(
    conn: sqlite3.Connection,
    df_new: pd.DataFrame,
    prod_count: int,
) -> list[ValidationResult]:
    results: list[ValidationResult] = []
    new_count = len(df_new)

    # Deve ter pelo menos 40k empresas (base completa)
    results.append(
        ValidationResult(
            name="row_count_min",
            passed=new_count >= 40000,
            details=f"novas_linhas={new_count}, mínimo_esperado=40000",
        )
    )

    company_ratio = float(df_new["company_name"].notna().mean()) if new_count else 0
    results.append(
        ValidationResult(
            name="company_name_completude",
            passed=company_ratio >= 0.98,
            details=f"completude={company_ratio:.4f}, mínimo=0.98",
        )
    )

    ticker_ratio = float(df_new["ticker"].notna().mean()) if new_count else 0
    results.append(
        ValidationResult(
            name="ticker_completude",
            passed=ticker_ratio >= 0.90,
            details=f"completude={ticker_ratio:.4f}, mínimo=0.90",
        )
    )

    # Delta vs produção: base completa (~48k) vs nonfin (~42k) = ratio ~1.12
    # Aceitável: 0.95 a 1.50 (pode crescer bastante com financeiras)
    if prod_count > 0:
        ratio_vs_prod = new_count / prod_count
        results.append(
            ValidationResult(
                name="delta_vs_producao",
                passed=0.95 <= ratio_vs_prod <= 1.50,
                details=f"novas/prod={ratio_vs_prod:.4f}, faixa_aceitável=[0.95, 1.50]",
            )
        )

    duplicate_ticker = int(df_new["ticker"].dropna().duplicated().sum())
    results.append(
        ValidationResult(
            name="duplicidade_ticker_controlada",
            passed=duplicate_ticker <= int(max(1000, new_count * 0.1)),
            details=f"duplicados={duplicate_ticker}",
        )
    )

    countries = int(df_new["country"].dropna().nunique())
    sectors = int(df_new["primary_sector"].dropna().nunique())
    results.append(
        ValidationResult(
            name="cobertura_geografia_setor",
            passed=(countries >= 20 and sectors >= 5),
            details=f"países={countries}, setores={sectors}",
        )
    )

    # Verificar que setores financeiros estão presentes
    financial_keywords = ["bank", "insurance", "financial", "brokerage", "investment"]
    industries = df_new["industry_group"].dropna().str.lower().unique()
    fin_sectors_found = [
        kw for kw in financial_keywords
        if any(kw in ind for ind in industries)
    ]
    results.append(
        ValidationResult(
            name="presenca_setores_financeiros",
            passed=len(fin_sectors_found) >= 3,
            details=f"setores_fin_encontrados={fin_sectors_found}",
        )
    )

    return results


def backup_database(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"damodaran_data_new_backup_{ts}.db"
    shutil.copy2(db_path, backup_file)
    return backup_file


def swap_tables(conn: sqlite3.Connection, staging_table: str) -> str:
    backup_table = f"damodaran_global_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute(f"ALTER TABLE damodaran_global RENAME TO {backup_table}")
    cur.execute(f"ALTER TABLE {staging_table} RENAME TO damodaran_global")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_year ON damodaran_global(year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_company ON damodaran_global(company_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_country ON damodaran_global(country)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON damodaran_global(ticker)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_industry ON damodaran_global(industry)")
    conn.commit()
    return backup_table


def write_report(report_path: Path, payload: dict) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def analyze_new_companies(
    conn: sqlite3.Connection,
    staging_table: str,
) -> dict:
    """Analisa empresas novas que serão adicionadas (não existiam na base anterior)."""
    existing_tickers = set()
    try:
        rows = conn.execute("SELECT ticker FROM damodaran_global WHERE ticker IS NOT NULL").fetchall()
        existing_tickers = {r[0] for r in rows}
    except Exception:
        pass

    new_rows = conn.execute(
        f"SELECT ticker, company_name, industry, country FROM {staging_table} WHERE ticker IS NOT NULL"
    ).fetchall()

    new_companies = []
    for ticker, name, industry, country in new_rows:
        if ticker not in existing_tickers:
            new_companies.append({
                "ticker": ticker,
                "company_name": name,
                "industry": industry,
                "country": country,
            })

    # Resumo por industry
    industry_counts = {}
    country_counts = {}
    for comp in new_companies:
        ind = comp.get("industry") or "Unknown"
        industry_counts[ind] = industry_counts.get(ind, 0) + 1
        ctr = comp.get("country") or "Unknown"
        country_counts[ctr] = country_counts.get(ctr, 0) + 1

    return {
        "total_new": len(new_companies),
        "by_industry": dict(sorted(industry_counts.items(), key=lambda x: -x[1])),
        "by_country": dict(sorted(country_counts.items(), key=lambda x: -x[1])[:20]),
        "sample": new_companies[:20],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migração segura: expansão da base Damodaran para incluir TODAS as empresas (financeiras + não-financeiras)"
    )
    parser.add_argument("--db-path", default="data/damodaran_data_new.db")
    parser.add_argument(
        "--excel-path",
        default="data/damodaran_data/globalcompfirms2026.xlsx",
    )
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--execute", action="store_true", help="Executa swap final (sem isso roda como dry-run)")
    parser.add_argument("--force", action="store_true", help="Ignora validações e força swap")
    parser.add_argument("--backup-dir", default="backups")
    parser.add_argument("--report-path", default="cache/migration_full_globalcomp_report.json")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    excel_path = Path(args.excel_path)

    if not db_path.exists():
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel não encontrado: {excel_path}")

    print("=" * 70)
    print("  MIGRAÇÃO: Base Completa Damodaran (incluindo financeiras)")
    print("=" * 70)
    print(f"  Fonte: {excel_path}")
    print(f"  Banco: {db_path}")
    print(f"  Modo:  {'EXECUÇÃO' if args.execute else 'DRY-RUN (seguro)'}")
    print("=" * 70)

    print("\n📥 Lendo Excel completo...")
    df_raw = pd.read_excel(excel_path)
    print(f"   Linhas lidas: {len(df_raw):,} | Colunas: {len(df_raw.columns)}")

    print("🧹 Normalizando para schema do banco...")
    df_new = build_dataframe_for_db(df_raw, args.year)
    df_new = df_new.dropna(subset=["company_name"]).copy()
    print(f"   Linhas após limpeza: {len(df_new):,}")

    conn = sqlite3.connect(db_path)
    staging_table = "damodaran_global_full_stg"
    try:
        prod_count = conn.execute("SELECT COUNT(*) FROM damodaran_global").fetchone()[0]
        print(f"\n📊 Base atual (damodaran_global): {prod_count:,} linhas (non-fin)")
        print(f"📊 Nova base (completa):           {len(df_new):,} linhas")
        print(f"📊 Diferença estimada:             +{len(df_new) - prod_count:,} empresas")

        print("\n🧪 Criando staging e inserindo dados...")
        create_staging_table(conn, staging_table)
        stg_count = insert_staging(conn, df_new, staging_table)
        print(f"   Staging pronta: {stg_count:,} linhas")

        print("\n🔍 Analisando empresas novas...")
        new_analysis = analyze_new_companies(conn, staging_table)
        print(f"   Empresas novas: {new_analysis['total_new']:,}")
        if new_analysis["by_industry"]:
            print("   Por setor (top 10):")
            for ind, count in list(new_analysis["by_industry"].items())[:10]:
                print(f"     {ind}: {count:,}")
        if new_analysis["by_country"]:
            print("   Por país (top 10):")
            for ctr, count in list(new_analysis["by_country"].items())[:10]:
                print(f"     {ctr}: {count:,}")

        validations = run_validations(conn, df_new, int(prod_count))
        all_passed = all(v.passed for v in validations)

        print("\n✅ Validações:")
        for item in validations:
            status = "PASS" if item.passed else "FAIL"
            print(f"   [{status}] {item.name}: {item.details}")

        report_payload = {
            "timestamp": datetime.now().isoformat(),
            "db_path": str(db_path),
            "excel_path": str(excel_path),
            "year": args.year,
            "dry_run": not args.execute,
            "production_count": int(prod_count),
            "staging_count": int(stg_count),
            "new_companies": new_analysis,
            "validations": [
                {"name": v.name, "passed": v.passed, "details": v.details}
                for v in validations
            ],
            "all_passed": all_passed,
        }

        if not args.execute:
            print("\n🛡️ Dry-run concluído. Nenhuma troca de tabela foi executada.")
            print("   Para executar de fato, use: --execute")
            conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
            conn.commit()
            write_report(Path(args.report_path), report_payload)
            print(f"📝 Relatório salvo em: {args.report_path}")
            return

        if not all_passed and not args.force:
            print("\n❌ Validações falharam. Swap abortado (use --force se quiser assumir o risco).")
            conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
            conn.commit()
            write_report(Path(args.report_path), report_payload)
            print(f"📝 Relatório salvo em: {args.report_path}")
            return

        print("\n💾 Gerando backup físico do banco...")
        backup_file = backup_database(db_path, Path(args.backup_dir))
        print(f"   Backup criado: {backup_file}")

        print("🔁 Executando swap atômico de tabelas...")
        backup_table = swap_tables(conn, staging_table)
        print(f"   Swap concluído. Backup lógico: {backup_table}")

        # Verificação pós-swap
        final_count = conn.execute("SELECT COUNT(*) FROM damodaran_global").fetchone()[0]
        print(f"\n✅ Migração concluída com sucesso!")
        print(f"   damodaran_global: {prod_count:,} → {final_count:,} registros")
        print(f"   Ganho: +{final_count - prod_count:,} empresas financeiras")

        report_payload["backup_file"] = str(backup_file)
        report_payload["backup_table"] = backup_table
        report_payload["final_count"] = int(final_count)
        write_report(Path(args.report_path), report_payload)
        print(f"📝 Relatório salvo em: {args.report_path}")

        print("\n📌 PRÓXIMOS PASSOS:")
        print("   1. Sincronizar company_basic_data:")
        print("      python scripts/sync_company_basic_data.py")
        print("   2. Enriquecer com Yahoo Finance:")
        print("      python scripts/update_company_data_from_yahoo_fast.py")
        print("   3. Importar campos adicionais do Excel:")
        print("      python scripts/import_excel_full_fields.py --excel-path data/damodaran_data/globalcompfirms2026.xlsx --update-existing")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
