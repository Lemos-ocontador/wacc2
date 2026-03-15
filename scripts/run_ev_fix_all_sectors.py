"""
run_ev_fix_all_sectors.py
=========================
Orquestra a re-extração de dados financeiros históricos por setor,
do menor para o maior, respeitando limites do Yahoo Finance.

Uso:
  python scripts/run_ev_fix_all_sectors.py
  python scripts/run_ev_fix_all_sectors.py --start-from 3    # começa pelo 3o setor
  python scripts/run_ev_fix_all_sectors.py --dry-run          # só mostra o plano
"""

import subprocess
import sys
import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ev_fix")

# Setores ordenados do menor para o maior (por nº de empresas com dados históricos)
SECTORS = [
    ("Financial Services",    648),
    ("Utilities",             735),
    ("Energy",              1_042),
    ("Communication Services",1_664),
    ("Real Estate",         2_063),
    ("Consumer Defensive",  2_468),
    ("Healthcare",          3_903),
    ("Basic Materials",     5_050),
    ("Consumer Cyclical",   5_098),
    ("Technology",          5_490),
    ("Industrials",         7_294),
]

PAUSE_BETWEEN_SECTORS = 30  # segundos entre setores

def run_sector(sector_name: str, index: int, total: int):
    """Executa re-extração de um setor."""
    log.info(f"{'='*60}")
    log.info(f"[{index}/{total}] Iniciando: {sector_name}")
    log.info(f"{'='*60}")
    
    start = time.time()
    result = subprocess.run(
        [
            sys.executable, "scripts/fetch_historical_financials.py",
            "--sector", sector_name,
            "--force",
            "--workers", "2",
            "--max-rps", "1.5",
        ],
        capture_output=False,
    )
    elapsed = time.time() - start
    
    if result.returncode == 0:
        log.info(f"[{index}/{total}] {sector_name} concluído em {elapsed/60:.1f} min")
    else:
        log.error(f"[{index}/{total}] {sector_name} FALHOU (code={result.returncode}) após {elapsed/60:.1f} min")
    
    return result.returncode


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-from", type=int, default=1, help="Começar a partir de qual setor (1-based)")
    parser.add_argument("--dry-run", action="store_true", help="Só mostra o plano sem executar")
    args = parser.parse_args()
    
    total = len(SECTORS)
    
    log.info(f"Re-extração de dados históricos com EV corrigido")
    log.info(f"Total de setores: {total}")
    log.info(f"Começando do setor #{args.start_from}")
    log.info(f"Pausa entre setores: {PAUSE_BETWEEN_SECTORS}s")
    log.info("")
    
    for i, (sector, est_companies) in enumerate(SECTORS, 1):
        status = "PULAR" if i < args.start_from else "EXECUTAR"
        log.info(f"  {i:2d}. {sector:30s} ~{est_companies:>6,} empresas  [{status}]")
    
    if args.dry_run:
        log.info("\n[DRY RUN] Nenhuma ação executada.")
        return
    
    log.info("")
    global_start = time.time()
    results = {}
    
    for i, (sector, _) in enumerate(SECTORS, 1):
        if i < args.start_from:
            continue
        
        rc = run_sector(sector, i, total)
        results[sector] = "OK" if rc == 0 else f"FALHOU (code={rc})"
        
        # Pausa entre setores para não sobrecarregar o Yahoo
        if i < total:
            log.info(f"Pausa de {PAUSE_BETWEEN_SECTORS}s antes do próximo setor...")
            time.sleep(PAUSE_BETWEEN_SECTORS)
    
    # Resumo final
    total_elapsed = time.time() - global_start
    log.info(f"\n{'='*60}")
    log.info(f"RESUMO FINAL — {total_elapsed/60:.0f} min ({total_elapsed/3600:.1f}h)")
    log.info(f"{'='*60}")
    for sector, status in results.items():
        log.info(f"  {sector:30s}  {status}")


if __name__ == "__main__":
    main()
