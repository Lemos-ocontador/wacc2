#!/usr/bin/env python3
"""
populate_etf_database.py
========================
Script para popular a base de dados de ETFs.
Fontes: yfinance (metadados + top holdings), SEC EDGAR (holdings completos EUA),
        CVM (holdings ETFs brasileiros).

Uso:
  python scripts/populate_etf_database.py                    # processa todos (~350 ETFs)
  python scripts/populate_etf_database.py --region us        # apenas ETFs dos EUA
  python scripts/populate_etf_database.py --region br        # apenas ETFs brasileiros
  python scripts/populate_etf_database.py --ticker SPY       # apenas 1 ETF
  python scripts/populate_etf_database.py --ticker SPY,VOO   # lista específica
  python scripts/populate_etf_database.py --batch-size 10    # lotes de 10
  python scripts/populate_etf_database.py --stats            # mostra estatísticas da base
  python scripts/populate_etf_database.py --list             # lista ETFs cadastrados
  python scripts/populate_etf_database.py --search AAPL      # busca reversa: ticker → ETFs
  python scripts/populate_etf_database.py --update-stale 7   # atualiza ETFs com >7 dias
  python scripts/populate_etf_database.py --overlap SPY,VOO  # sobreposição entre 2 ETFs
  python scripts/populate_etf_database.py --no-sec           # desabilita SEC EDGAR
  python scripts/populate_etf_database.py --no-cvm           # desabilita CVM
"""

import argparse
import os
import sys
import logging
from pathlib import Path

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.setdefault("CURL_CA_BUNDLE", r"C:\cacerts\cacert.pem")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("populate_etf")


def main():
    parser = argparse.ArgumentParser(description="Popular base de dados de ETFs")
    parser.add_argument("--region", choices=["us", "br", "global", "all"],
                        default="all", help="Região dos ETFs (padrão: all)")
    parser.add_argument("--ticker", type=str, default=None,
                        help="Ticker(s) específico(s) separados por vírgula")
    parser.add_argument("--batch-size", type=int, default=20,
                        help="Tamanho do lote (padrão: 20)")
    parser.add_argument("--pause", type=float, default=5.0,
                        help="Pausa entre lotes em segundos (padrão: 5)")
    parser.add_argument("--rate-limit", type=float, default=1.5,
                        help="Requisições por segundo (padrão: 1.5)")
    parser.add_argument("--stats", action="store_true",
                        help="Mostra estatísticas da base")
    parser.add_argument("--list", action="store_true",
                        help="Lista todos os ETFs cadastrados")
    parser.add_argument("--search", type=str, default=None,
                        help="Busca reversa: em quais ETFs um ticker aparece")
    parser.add_argument("--holdings", type=str, default=None,
                        help="Mostra holdings de um ETF específico")
    parser.add_argument("--update-stale", type=int, default=None, metavar="DAYS",
                        help="Atualiza ETFs não atualizados há mais de N dias")
    parser.add_argument("--overlap", type=str, default=None,
                        help="Calcula sobreposição entre 2 ETFs (ex: SPY,VOO)")
    parser.add_argument("--no-sec", action="store_true",
                        help="Desabilita SEC EDGAR como fonte de holdings")
    parser.add_argument("--no-cvm", action="store_true",
                        help="Desabilita CVM como fonte de holdings")

    args = parser.parse_args()

    from data_extractors.etf_extractor import (
        ETFExtractor, ETF_LIST_US, ETF_LIST_BR, ETF_LIST_GLOBAL, ALL_ETFS
    )

    extractor = ETFExtractor(
        rate_limit=args.rate_limit,
        use_sec=not args.no_sec,
        use_cvm=not args.no_cvm,
    )

    # ── Modo estatísticas ──
    if args.stats:
        stats = extractor.get_stats()
        print("\n╔══════════════════════════════════════╗")
        print("║      ESTATÍSTICAS DA BASE ETF        ║")
        print("╠══════════════════════════════════════╣")
        print(f"║  ETFs cadastrados:  {stats['etfs']:>14}  ║")
        print(f"║  Holdings totais:   {stats['holdings_total']:>14}  ║")
        print(f"║  Holdings únicos:   {stats['unique_holdings']:>14}  ║")
        print(f"║  Última atualização: {(stats['last_update'] or 'N/A')[:16]:>13}  ║")
        print("╚══════════════════════════════════════╝")
        by_source = stats.get('by_source', {})
        if by_source:
            print("\n  Fontes de dados:")
            for src, cnt in sorted(by_source.items()):
                print(f"    {src:<15} {cnt:>5} ETFs")
        by_region = stats.get('by_region', {})
        if by_region:
            print("\n  Por região:")
            for reg, cnt in sorted(by_region.items()):
                print(f"    {reg:<15} {cnt:>5} ETFs")
        print()
        return

    # ── Modo listagem ──
    if args.list:
        etfs = extractor.get_all_etfs()
        if not etfs:
            print("Nenhum ETF cadastrado.")
            return
        print(f"\n{'Ticker':<12} {'Nome':<40} {'Categoria':<20} {'AUM':>12} {'Holdings':>8} {'Fonte':<10}")
        print("─" * 105)
        for e in etfs:
            aum_str = f"${e['aum']/1e9:.1f}B" if e.get("aum") else "N/A"
            src = (e.get('data_source') or '-')[:8]
            print(f"{e['ticker']:<12} {(e['name'] or '')[:38]:<40} {(e['category'] or '')[:18]:<20} {aum_str:>12} {e.get('total_holdings') or 0:>8} {src:<10}")
        print(f"\nTotal: {len(etfs)} ETFs\n")
        return

    # ── Modo busca reversa ──
    if args.search:
        results = extractor.find_etfs_containing(args.search.upper())
        if not results:
            print(f"'{args.search}' não encontrado em nenhum ETF.")
            return
        print(f"\n'{args.search}' aparece em {len(results)} ETF(s):\n")
        print(f"{'ETF':<12} {'Nome do ETF':<35} {'Peso (%)':>10} {'Holding':<20}")
        print("─" * 80)
        for r in results:
            w = f"{r['weight']:.2f}%" if r.get("weight") else "N/A"
            print(f"{r['etf_ticker']:<12} {(r['etf_name'] or '')[:33]:<35} {w:>10} {(r['holding_name'] or '')[:18]:<20}")
        print()
        return

    # ── Modo holdings ──
    if args.holdings:
        holdings = extractor.get_holdings_for(args.holdings.upper())
        if not holdings:
            print(f"Nenhum holding encontrado para '{args.holdings}'.")
            return
        print(f"\nHoldings de {args.holdings.upper()} ({len(holdings)} ativos):\n")
        print(f"{'#':>3} {'Ticker':<12} {'Nome':<30} {'Peso (%)':>10}")
        print("─" * 58)
        for i, h in enumerate(holdings, 1):
            w = f"{h['weight']:.2f}%" if h.get("weight") else "N/A"
            print(f"{i:>3} {(h['holding_ticker'] or 'N/A'):<12} {(h['holding_name'] or '')[:28]:<30} {w:>10}")
        print()
        return

    # ── Modo update stale ──
    if args.update_stale is not None:
        summary = extractor.update_stale(
            days_old=args.update_stale,
            batch_size=args.batch_size,
            pause_between_batches=args.pause,
        )
        print(f"\nAtualização concluída: {summary['success']}/{summary['total']} ETFs atualizados\n")
        return

    # ── Modo overlap ──
    if args.overlap:
        parts = [t.strip().upper() for t in args.overlap.split(",")]
        if len(parts) != 2:
            print("Use --overlap TICKER1,TICKER2")
            return
        result = extractor.get_overlap(parts[0], parts[1])
        print(f"\nSobreposição {result['etf1']} vs {result['etf2']}:")
        print(f"  Holdings {result['etf1']}: {result['holdings_etf1']}")
        print(f"  Holdings {result['etf2']}: {result['holdings_etf2']}")
        print(f"  Em comum:         {result['common']}")
        print(f"  Sobreposição:     {result['overlap_pct']}%")
        if result['common_tickers']:
            shown = result['common_tickers'][:20]
            print(f"  Tickers comuns:   {', '.join(shown)}")
            if len(result['common_tickers']) > 20:
                print(f"                    ... e mais {len(result['common_tickers']) - 20}")
        print()
        return

    # ── Modo processamento ──
    if args.ticker:
        tickers = [t.strip().upper() for t in args.ticker.split(",")]
    elif args.region == "us":
        tickers = ETF_LIST_US
    elif args.region == "br":
        tickers = ETF_LIST_BR
    elif args.region == "global":
        tickers = ETF_LIST_GLOBAL
    else:
        tickers = ALL_ETFS

    log.info(f"Processando {len(tickers)} ETFs (região: {args.region})")

    processed = [0]

    def on_etf_done(result):
        processed[0] += 1
        status = "✓" if result["metadata"] else "✗"
        h = result["holdings"]
        src = result.get("source", "?")
        log.info(f"  [{processed[0]}/{len(tickers)}] {status} {result['ticker']} – {h} holdings [{src}]")

    summary = extractor.bulk_process(
        tickers=tickers,
        batch_size=args.batch_size,
        pause_between_batches=args.pause,
        callback=on_etf_done,
    )

    print("\n" + "=" * 50)
    print("           RESUMO DO PROCESSAMENTO")
    print("=" * 50)
    print(f"  Total processados: {summary['total']}")
    print(f"  Sucesso:           {summary['success']}")
    print(f"  Falhas:            {summary['failed']}")
    print(f"  Holdings salvos:   {summary['total_holdings']}")
    if summary["errors"]:
        print(f"  Erros em:          {', '.join(summary['errors'][:20])}")
        if len(summary["errors"]) > 20:
            print(f"                     ... e mais {len(summary['errors']) - 20}")
    print("=" * 50 + "\n")

    # Mostra stats finais
    stats = extractor.get_stats()
    print(f"Base atualizada: {stats['etfs']} ETFs, {stats['holdings_total']} holdings\n")


if __name__ == "__main__":
    main()
