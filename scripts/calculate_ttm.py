"""
calculate_ttm.py
================
Calcula métricas TTM (Trailing Twelve Months) a partir dos dados trimestrais
e recalcula múltiplos (EV/Revenue, EV/EBITDA, EV/EBIT, P/E, P/FCF) usando TTM.

Para cada registro (annual ou quarterly), o TTM é calculado como a soma
dos 4 últimos trimestres disponíveis até aquela data-base.
Para anuais, o TTM = próprio valor anual (já representa 12 meses).

Uso:
  python scripts/calculate_ttm.py --sector "Utilities"
  python scripts/calculate_ttm.py --company POSI3.SA
  python scripts/calculate_ttm.py  # processa tudo
"""

import argparse
import sqlite3
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("calc_ttm")

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "damodaran_data_new.db"

# Campos para os quais calculamos TTM (são acumulados, não snapshot)
TTM_FIELDS = [
    ("total_revenue", "total_revenue_ttm"),
    ("ebitda", "ebitda_ttm"),
    ("ebit", "ebit_ttm"),
    ("free_cash_flow", "free_cash_flow_ttm"),
    ("net_income", "net_income_ttm"),
]


def ensure_ttm_columns(conn):
    """Adiciona colunas TTM se não existirem."""
    for _, ttm_col in TTM_FIELDS:
        try:
            conn.execute(f"ALTER TABLE company_financials_historical ADD COLUMN {ttm_col} REAL")
            log.info(f"Coluna '{ttm_col}' adicionada.")
        except sqlite3.OperationalError:
            pass  # já existe
    # Coluna para rastrear quantos trimestres compõem o TTM
    try:
        conn.execute("ALTER TABLE company_financials_historical ADD COLUMN ttm_quarters_count INTEGER")
        log.info("Coluna 'ttm_quarters_count' adicionada.")
    except sqlite3.OperationalError:
        pass
    # Também garantir close_price
    try:
        conn.execute("ALTER TABLE company_financials_historical ADD COLUMN close_price REAL")
        log.info("Coluna 'close_price' adicionada.")
    except sqlite3.OperationalError:
        pass
    conn.commit()


def get_companies(conn, args) -> list[dict]:
    """Retorna empresas para processar."""
    query = """
        SELECT DISTINCT cfh.company_basic_data_id, cfh.yahoo_code, cbd.yahoo_sector
        FROM company_financials_historical cfh
        JOIN company_basic_data cbd ON cbd.id = cfh.company_basic_data_id
        WHERE 1=1
    """
    params = []
    if args.sector:
        query += " AND cbd.yahoo_sector = ?"
        params.append(args.sector)
    if args.company:
        query += " AND (cfh.yahoo_code = ? OR cbd.ticker LIKE ?)"
        params.extend([args.company, f"%{args.company}%"])
    query += " ORDER BY cfh.yahoo_code"
    
    rows = conn.execute(query, params).fetchall()
    return [{"company_basic_data_id": r[0], "yahoo_code": r[1], "sector": r[2]} for r in rows]


def calculate_ttm_for_company(conn, company_id: int, yahoo_code: str) -> int:
    """
    Calcula TTM para todos os registros (annual e quarterly) de uma empresa.
    
    Lógica:
    - Para ANNUAL: TTM = valor do próprio período (já é 12 meses)
    - Para QUARTERLY: TTM = soma dos 4 últimos trimestres <= period_date
      (incluindo o trimestre atual)
    
    Retorna: número de registros atualizados.
    """
    # Buscar todos os registros da empresa, ordenados por data
    rows = conn.execute("""
        SELECT id, period_type, period_date, fiscal_year, fiscal_quarter,
               total_revenue, ebitda, ebit, free_cash_flow, net_income
        FROM company_financials_historical
        WHERE company_basic_data_id = ?
        ORDER BY period_date
    """, (company_id,)).fetchall()
    
    if not rows:
        return 0
    
    # Separar trimestrais e anuais
    quarterly_records = []
    annual_records = []
    for r in rows:
        rec = {
            "id": r[0], "period_type": r[1], "period_date": r[2],
            "fiscal_year": r[3], "fiscal_quarter": r[4],
            "total_revenue": r[5], "ebitda": r[6], "ebit": r[7],
            "free_cash_flow": r[8], "net_income": r[9],
        }
        if rec["period_type"] == "quarterly":
            quarterly_records.append(rec)
        else:
            annual_records.append(rec)
    
    updated = 0
    
    # --- Para ANUAIS: TTM = próprio valor, quarters_count = 4 ---
    for rec in annual_records:
        ttm_values = {}
        for src_field, ttm_field in TTM_FIELDS:
            ttm_values[ttm_field] = rec[src_field]
        
        # Recalcular múltiplos usando TTM (que para anuais são os mesmos valores)
        _update_record_ttm(conn, rec["id"], ttm_values, quarters_count=4)
        updated += 1
    
    # --- Para TRIMESTRAIS: TTM = soma 4 últimos Q ---
    # Ordenar trimestrais por data
    quarterly_records.sort(key=lambda x: x["period_date"])
    
    for i, rec in enumerate(quarterly_records):
        # Pegar os 4 últimos trimestres (incluindo o atual)
        # Buscar os 3 anteriores + atual
        start_idx = max(0, i - 3)
        window = quarterly_records[start_idx:i + 1]
        
        ttm_values = {}
        # Usar revenue como referência para contagem de trimestres
        rev_vals = [w["total_revenue"] for w in window if w["total_revenue"] is not None]
        quarters_count = len(rev_vals)
        
        for src_field, ttm_field in TTM_FIELDS:
            # Somar os valores não-nulos dos 4 trimestres
            vals = [w[src_field] for w in window if w[src_field] is not None]
            if len(vals) >= 2:  # mínimo 2 trimestres com dados para ser razoável
                ttm_values[ttm_field] = sum(vals)
            else:
                ttm_values[ttm_field] = None
        
        _update_record_ttm(conn, rec["id"], ttm_values, quarters_count=quarters_count)
        updated += 1
    
    return updated


def _update_record_ttm(conn, record_id: int, ttm_values: dict, quarters_count: int = 4):
    """Atualiza um registro com os valores TTM e recalcula múltiplos."""
    # Buscar dados necessários para recalcular múltiplos
    row = conn.execute("""
        SELECT enterprise_value_estimated, market_cap_estimated, fx_rate_to_usd
        FROM company_financials_historical
        WHERE id = ?
    """, (record_id,)).fetchone()
    
    if not row:
        return
    
    ev = row[0]
    mcap = row[1]
    fx_rate = row[2] or 1.0
    
    rev_ttm = ttm_values.get("total_revenue_ttm")
    ebitda_ttm = ttm_values.get("ebitda_ttm")
    ebit_ttm = ttm_values.get("ebit_ttm")
    fcf_ttm = ttm_values.get("free_cash_flow_ttm")
    net_income_ttm = ttm_values.get("net_income_ttm")
    
    # Recalcular múltiplos com TTM
    # Só calcular múltiplos EV quando TTM é confiável (4 trimestres completos)
    ev_revenue = None
    ev_ebitda = None
    ev_ebit = None
    
    ttm_reliable = quarters_count >= 4
    
    # Materialidade: receita TTM mínima de $100K USD para evitar distorções
    rev_usd = abs(rev_ttm * fx_rate) if rev_ttm else 0
    rev_material = ttm_reliable and rev_ttm and rev_ttm != 0 and rev_usd >= 100_000
    
    if rev_material and ev:
        ratio = ev / rev_ttm
        ev_revenue = ratio if abs(ratio) <= 500 else None
    
    if ttm_reliable and ev and ebitda_ttm and ebitda_ttm != 0:
        ratio = ev / ebitda_ttm
        ebitda_usd = abs(ebitda_ttm * fx_rate)
        if ebitda_usd >= 100 and abs(ratio) <= 500:
            ev_ebitda = ratio
    
    if ttm_reliable and ev and ebit_ttm and ebit_ttm > 0:
        ratio = ev / ebit_ttm
        ev_ebit = ratio if abs(ratio) <= 500 else None
    
    # Update
    conn.execute("""
        UPDATE company_financials_historical
        SET total_revenue_ttm = ?,
            ebitda_ttm = ?,
            ebit_ttm = ?,
            free_cash_flow_ttm = ?,
            net_income_ttm = ?,
            ev_revenue = ?,
            ev_ebitda = ?,
            ev_ebit = ?,
            ttm_quarters_count = ?
        WHERE id = ?
    """, (
        rev_ttm, ebitda_ttm, ebit_ttm, fcf_ttm, net_income_ttm,
        ev_revenue, ev_ebitda, ev_ebit,
        quarters_count,
        record_id,
    ))


def main():
    parser = argparse.ArgumentParser(description="Calcula TTM e recalcula múltiplos")
    parser.add_argument("--sector", type=str, help="Filtrar por Yahoo sector")
    parser.add_argument("--company", type=str, help="Yahoo code específico")
    parser.add_argument("--db", type=str, default=None, help="Caminho do banco")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else DB_PATH
    log.info(f"DB: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    
    # Garantir colunas TTM existem
    ensure_ttm_columns(conn)
    
    # Buscar empresas
    companies = get_companies(conn, args)
    if not companies:
        log.info("Nenhuma empresa encontrada com os filtros.")
        conn.close()
        return
    
    log.info(f"Empresas para processar: {len(companies)}")
    
    total_updated = 0
    for i, comp in enumerate(companies):
        n = calculate_ttm_for_company(conn, comp["company_basic_data_id"], comp["yahoo_code"])
        total_updated += n
        
        if (i + 1) % 100 == 0 or (i + 1) == len(companies):
            conn.commit()
            log.info(f"[{i+1}/{len(companies)}] {comp['yahoo_code']} - {n} registros | Total: {total_updated}")
    
    conn.commit()
    conn.close()
    
    log.info(f"Concluído: {total_updated} registros atualizados em {len(companies)} empresas.")


if __name__ == "__main__":
    main()
