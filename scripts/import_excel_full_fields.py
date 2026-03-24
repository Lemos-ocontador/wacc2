import argparse
import csv
import sqlite3
from pathlib import Path

import pandas as pd


def normalize_column_name(column_name: str) -> str:
    text = str(column_name).strip().lower()
    text = ''.join(ch if ch.isalnum() else '_' for ch in text)
    while '__' in text:
        text = text.replace('__', '_')
    return text.strip('_')


def get_column_overrides() -> dict[str, str]:
    return {
        normalize_column_name('EV/Sales'): 'ev_revenue',
        normalize_column_name('EV/EBITDA'): 'ev_ebitda',
        normalize_column_name('EV/EBIT'): 'ev_ebit',
        normalize_column_name('PBV'): 'pb_ratio',
        normalize_column_name('PEG'): 'peg_ratio',
        normalize_column_name('Forward PE'): 'forward_pe',
        normalize_column_name('Current PE'): 'current_pe',
        normalize_column_name('Trailing PE'): 'trailing_pe',
    }


def build_column_mapping(columns: list[str]) -> dict[str, str]:
    overrides = get_column_overrides()

    mapping: dict[str, str] = {}
    used: dict[str, int] = {}

    for col in columns:
        normalized = normalize_column_name(col)
        target = overrides.get(normalized, normalized)
        if target in used:
            used[target] += 1
            target = f"{target}_{used[target]}"
        else:
            used[target] = 1
        mapping[col] = target

    return mapping


def load_mapping_csv(mapping_path: Path) -> list[dict[str, str]]:
    if not mapping_path.exists():
        return []

    with mapping_path.open(newline='', encoding='utf-8') as handle:
        reader = csv.DictReader(handle)
        cleaned_rows = []
        for row in reader:
            cleaned_row = {}
            for key, value in row.items():
                if key is None:
                    continue
                clean_key = key.strip().lower().lstrip('\ufeff')
                cleaned_row[clean_key] = (value or '').strip()
            cleaned_rows.append(cleaned_row)
        return cleaned_rows


def build_mapping_from_csv(rows: list[dict[str, str]]) -> dict[str, str]:
    overrides = get_column_overrides()
    mapping: dict[str, str] = {}

    for row in rows:
        campo = row.get('campo', '').strip()
        if not campo:
            continue

        if row.get('no_excel', '').upper() != 'SIM':
            continue

        normalized = normalize_column_name(campo)
        target = overrides.get(normalized, normalized)
        mapping[campo] = target

    return mapping


def ensure_unique_mapping(mapping: dict[str, str]) -> dict[str, str]:
    used: dict[str, int] = {}
    unique: dict[str, str] = {}

    for source, target in mapping.items():
        count = used.get(target, 0) + 1
        used[target] = count
        if count == 1:
            unique[source] = target
        else:
            unique[source] = f"{target}_{count}"

    return unique


def infer_sql_type(series: pd.Series) -> str:
    non_null = series.dropna()
    if non_null.empty:
        return 'TEXT'

    numeric = pd.to_numeric(non_null, errors='coerce')
    numeric_ratio = numeric.notna().mean()
    if numeric_ratio >= 0.9:
        return 'REAL'

    return 'TEXT'


def infer_sql_type_from_name(field_name: str) -> str:
    text_fields = [
        'name', 'country', 'exchange', 'ticker', 'group', 'sector', 'industry',
        'sic', 'classification'
    ]
    lower_name = field_name.lower()
    if any(key in lower_name for key in text_fields):
        return 'TEXT'
    return 'REAL'


def ensure_table_exists(conn: sqlite3.Connection) -> None:
    """Cria a tabela damodaran_global se ela não existir."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS damodaran_global (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_ticker TEXT UNIQUE NOT NULL
        )
    """)
    conn.commit()


def add_missing_columns(conn: sqlite3.Connection, columns: dict[str, str]) -> list[str]:
    cur = conn.cursor()
    cur.execute('PRAGMA table_info(damodaran_global)')
    existing = {row[1] for row in cur.fetchall()}

    added: list[str] = []
    for col_name, col_type in columns.items():
        if col_name in existing:
            continue
        cur.execute(f"ALTER TABLE damodaran_global ADD COLUMN {col_name} {col_type}")
        added.append(col_name)

    conn.commit()
    return added


def add_missing_columns_from_mapping(
    conn: sqlite3.Connection,
    mapping_rows: list[dict[str, str]],
) -> list[str]:
    if not mapping_rows:
        return []

    overrides = get_column_overrides()
    cur = conn.cursor()
    cur.execute('PRAGMA table_info(damodaran_global)')
    existing = {row[1] for row in cur.fetchall()}

    added: list[str] = []
    for row in mapping_rows:
        if row.get('no_db', '').upper() == 'SIM':
            continue

        campo = row.get('campo', '').strip()
        if not campo:
            continue

        normalized = normalize_column_name(campo)
        field_name = overrides.get(normalized, normalized)

        if field_name in existing:
            continue

        col_type = infer_sql_type_from_name(field_name)
        cur.execute(f"ALTER TABLE damodaran_global ADD COLUMN {field_name} {col_type}")
        added.append(field_name)
        existing.add(field_name)

    conn.commit()
    return added


def update_columns(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    ticker_col: str,
    columns: list[str],
    batch_size: int,
) -> int:
    if not columns:
        return 0

    updated = 0
    cur = conn.cursor()

    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        if batch.empty:
            continue

        # Usar INSERT OR REPLACE para fazer upsert
        all_cols = [ticker_col] + columns
        placeholders = ', '.join(['?' for _ in all_cols])
        col_names = ', '.join(all_cols)
        sql = f"INSERT OR REPLACE INTO damodaran_global ({col_names}) VALUES ({placeholders})"

        values = batch[all_cols].where(pd.notna(batch[all_cols]), None)
        cur.executemany(sql, values.itertuples(index=False, name=None))
        updated += cur.rowcount
        conn.commit()

    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description='Importa todos os campos do Excel para damodaran_global.')
    parser.add_argument('--excel-path', default='data/damodaran_data/globalcompfirms2026.xlsx')
    parser.add_argument('--db-path', default='data/damodaran_db.db')
    parser.add_argument('--mapping-csv', default='data/mapeamento_campos_damodaran_20250926_235026.csv')
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--limit', type=int, default=0, help='Limita linhas (0 = sem limite)')
    parser.add_argument('--update-existing', action='store_true', help='Atualiza colunas existentes alem das novas')
    args = parser.parse_args()

    excel_path = Path(args.excel_path)
    db_path = Path(args.db_path)

    if not excel_path.exists():
        raise FileNotFoundError(f"Excel nao encontrado: {excel_path}")

    print(f"Lendo Excel: {excel_path}")
    df = pd.read_excel(excel_path)
    if args.limit and args.limit > 0:
        df = df.head(args.limit)

    mapping_rows = load_mapping_csv(Path(args.mapping_csv))
    csv_mapping = build_mapping_from_csv(mapping_rows)
    csv_mapping_lower = {key.lower(): value for key, value in csv_mapping.items()}
    csv_mapping_normalized = {
        normalize_column_name(key): value for key, value in csv_mapping.items()
    }

    rename_mapping: dict[str, str] = {}
    fallback_mapping = build_column_mapping(list(df.columns))
    missing_in_csv: list[str] = []

    for col in df.columns:
        col_key = str(col).strip()
        target = csv_mapping.get(col_key)
        if not target:
            target = csv_mapping_lower.get(col_key.lower())
        if not target:
            target = csv_mapping_normalized.get(normalize_column_name(col_key))
        if not target:
            missing_in_csv.append(col_key)
            target = fallback_mapping[col]

        rename_mapping[col] = target

    rename_mapping = ensure_unique_mapping(rename_mapping)

    if missing_in_csv:
        print(
            "Aviso: algumas colunas do Excel nao estao no mapeamento CSV e "
            "serao normalizadas automaticamente."
        )
        print("Colunas sem mapeamento:", missing_in_csv[:10])
        if len(missing_in_csv) > 10:
            print(f"... mais {len(missing_in_csv) - 10} coluna(s)")

    df = df.rename(columns=rename_mapping)

    ticker_col = 'exchange_ticker' if 'exchange_ticker' in df.columns else None
    if not ticker_col:
        raise ValueError('Coluna Exchange:Ticker nao encontrada no Excel')

    df[ticker_col] = df[ticker_col].astype(str).str.strip()
    df = df[df[ticker_col].notna() & (df[ticker_col] != '')].copy()

    conn = sqlite3.connect(db_path)
    try:
        # Garantir que a tabela existe antes de tentar adicionar colunas
        ensure_table_exists(conn)
        
        column_types: dict[str, str] = {}
        for col in df.columns:
            if col == ticker_col:
                continue
            column_types[col] = infer_sql_type(df[col])

        added = add_missing_columns(conn, column_types)
        added_from_mapping = add_missing_columns_from_mapping(conn, mapping_rows)
        added_total = len(added) + len(added_from_mapping)
        if added_total:
            print(
                f"Colunas novas adicionadas: {added_total} "
                f"(Excel: {len(added)}, Mapeamento: {len(added_from_mapping)})"
            )
        else:
            print("Colunas novas adicionadas: 0")

        if args.update_existing:
            update_cols = [col for col in df.columns if col != ticker_col]
            updated = update_columns(conn, df, ticker_col, update_cols, args.batch_size)
            print(f"Registros atualizados (todas as colunas mapeadas): {updated}")
            return

        # Atualizar apenas colunas novas para evitar sobrescrever dados existentes
        new_cols = [col for col in df.columns if col in added or col in added_from_mapping]
        if not new_cols:
            print('Nenhuma coluna nova para atualizar.')
            return

        updated = update_columns(conn, df, ticker_col, new_cols, args.batch_size)
        print(f"Registros atualizados (colunas novas): {updated}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
