"""
Update otimizado em LOTES GRANDES
Sabendo que o campo é longBusinessSummary, processa em batches maiores
"""
import argparse
import sqlite3
import time
import yfinance as yf
from pathlib import Path

def update_batch(db_path, exchanges_list, limit_per_batch, sleep_seconds):
    """Atualiza um lote de registros"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Montar WHERE para exchanges
    where_clause = "WHERE 1=1"
    params = []
    
    if exchanges_list:
        placeholders = ",".join(["?" for _ in exchanges_list])
        where_clause += f" AND UPPER(SUBSTR(ticker, 1, INSTR(ticker, ':')-1)) IN ({placeholders})"
        params.extend([ex.upper() for ex in exchanges_list])
    
    # Só pegar registros com yahoo_code e sem about
    where_clause += " AND yahoo_code IS NOT NULL AND TRIM(yahoo_code) != ''"
    where_clause += " AND (about IS NULL OR TRIM(about) = '')"
    
    sql = f"""
        SELECT id, ticker, yahoo_code, company_name
        FROM company_basic_data
        {where_clause}
        ORDER BY id
        LIMIT {int(limit_per_batch)}
    """
    
    print(f"\n{'='*100}")
    print(f"🔄 BATCH UPDATE - Exchanges: {', '.join(exchanges_list) if exchanges_list else 'TODAS'}")
    print(f"{'='*100}")
    
    try:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"❌ Erro na query: {e}")
        conn.close()
        return 0, 0
    
    print(f"📊 Encontrados {len(rows)} registros para atualizar")
    
    if not rows:
        conn.close()
        return 0, 0
    
    updated = 0
    failed = 0
    
    for idx, (row_id, ticker, yahoo_code, company_name) in enumerate(rows, 1):
        try:
            t = yf.Ticker(yahoo_code)
            info = t.get_info()
            about = info.get('longBusinessSummary')
            
            if about:
                about = str(about).strip()
                cursor.execute(
                    "UPDATE company_basic_data SET about = ? WHERE id = ?",
                    (about, row_id)
                )
                updated += 1
                
                if idx % 50 == 0:  # Status a cada 50 registros
                    print(f"   {idx}/{len(rows)}: {updated} ✓ | {failed} ✗")
            else:
                failed += 1
        
        except Exception as e:
            failed += 1
        
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Atualizados: {updated}")
    print(f"❌ Falhados: {failed}")
    print(f"Total processado: {updated + failed}")
    
    return updated, failed


def main():
    parser = argparse.ArgumentParser(description="Update de about em LOTES GRANDES")
    parser.add_argument("--db", default="data/damodaran_data_new.db", help="Caminho do DB")
    parser.add_argument("--exchanges", default="", help="Exchanges separadas por vírgula (ex: TSE,SZSE,OTCPK)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Tamanho do lote (default: 1000)")
    parser.add_argument("--sleep", type=float, default=0.05, help="Pausa entre requisições (default: 0.05)")
    parser.add_argument("--max-batches", type=int, default=10, help="Máximo de batches a rodar (default: 10)")
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ Banco não encontrado: {db_path}")
        return
    
    exchanges = [x.strip() for x in args.exchanges.split(",") if x.strip()] if args.exchanges else []
    
    print(f"\n{'#'*100}")
    print(f"# PROCESSANDO {args.max_batches} BATCHES DE {args.batch_size} REGISTROS")
    print(f"{'#'*100}")
    
    total_updated = 0
    total_failed = 0
    batch_num = 0
    
    for batch_num in range(1, args.max_batches + 1):
        print(f"\n\n{'='*100}")
        print(f"📍 BATCH {batch_num}/{args.max_batches}")
        print(f"{'='*100}")
        
        updated, failed = update_batch(db_path, exchanges, args.batch_size, args.sleep)
        total_updated += updated
        total_failed += failed
        
        if updated == 0 and failed == 0:
            print(f"\n⏹️  Nenhum registro encontrado - encerrando")
            break
    
    print(f"\n\n{'#'*100}")
    print(f"# RESUMO FINAL")
    print(f"{'#'*100}")
    print(f"Batches processados: {batch_num}")
    print(f"Total atualizados: {total_updated}")
    print(f"Total falhados: {total_failed}")
    
    # Contar total no DB
    conn = sqlite3.connect(db_path)
    total_com_about = conn.execute("SELECT COUNT(*) FROM company_basic_data WHERE about IS NOT NULL AND TRIM(about) != ''").fetchone()[0]
    total_records = conn.execute("SELECT COUNT(*) FROM company_basic_data").fetchone()[0]
    conn.close()
    
    pct = (total_com_about * 100) // total_records if total_records > 0 else 0
    print(f"Total com about no DB: {total_com_about} / {total_records} ({pct}%)")
    print(f"{'#'*100}")


if __name__ == "__main__":
    main()
