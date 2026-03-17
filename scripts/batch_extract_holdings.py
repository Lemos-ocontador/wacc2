"""
Extração em lote de holdings para ETFs sem dados.
Categorias:
  - BR com CNPJ: CVM → yfinance
  - BR sem CNPJ: yfinance
  - US Bond/Equity: SEC EDGAR → yfinance
  - Commodity/Crypto: yfinance (muitos não terão holdings)
"""
import sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data_extractors.etf_extractor import ETFExtractor

DB = str(Path(__file__).resolve().parent.parent / "data" / "damodaran_data_new.db")

# Todos os 57 ETFs sem holdings (agrupados por tipo para log)
ALL_MISSING = [
    # BR com CNPJ (13) - CVM path
    "ACWI11.SA", "DIVO11.SA", "FIND11.SA", "FIXA11.SA", "GOLD11.SA",
    "HASH11.SA", "IMAB11.SA", "IRFM11.SA", "NASD11.SA", "PIBB11.SA",
    "SPXI11.SA", "TECK11.SA", "XINA11.SA",
    # BR sem CNPJ (7) - yfinance only
    "5MBA11.SA", "BBSD11.SA", "BITH11.SA", "BOVB11.SA", "ETHE11.SA",
    "HTEK11.SA", "QQQM11.SA",
    # US Bond (25) - SEC → yfinance
    "BIL", "BND", "BNDX", "BWX", "EMB", "EMLC", "FLOT", "GOVT",
    "IAGG", "IEF", "IGIB", "IGSB", "JNK", "PCY", "SCHZ", "SHV",
    "SHY", "TIP", "TLT", "VCIT", "VCSH", "VGIT", "VGLT", "VGSH", "VMBS",
    # Commodity (9) - yfinance (provavelmente sem holdings)
    "BAR", "CPER", "GLD", "GLDM", "IAU", "PALL", "PPLT", "SLV", "USO",
    # Crypto (2)
    "ETHE", "GBTC",
    # Other (1)
    "PAF",
]


def main():
    ext = ETFExtractor(db_path=DB, rate_limit=2.0)

    results = {"ok": [], "empty": [], "error": []}

    print(f"=== Extração em lote: {len(ALL_MISSING)} ETFs ===\n")

    for i, ticker in enumerate(ALL_MISSING, 1):
        print(f"[{i}/{len(ALL_MISSING)}] {ticker} ...", end=" ", flush=True)

        try:
            holdings, source = ext.get_holdings_with_fallback(ticker)

            if holdings:
                saved = ext.save_holdings(ticker, holdings, source)
                print(f"OK  {saved} holdings ({source})")
                results["ok"].append((ticker, saved, source))
            else:
                print(f"--  sem holdings")
                results["empty"].append(ticker)

        except Exception as e:
            print(f"ERR {e}")
            results["error"].append((ticker, str(e)))

        # Pausa entre requests para evitar rate limit
        time.sleep(1.0)

    # Resumo
    print(f"\n{'='*50}")
    print(f"Extraídos com holdings: {len(results['ok'])}")
    for t, n, s in results["ok"]:
        print(f"  {t:15s} {n:5d} holdings ({s})")

    print(f"\nSem holdings encontrados: {len(results['empty'])}")
    for t in results["empty"]:
        print(f"  {t}")

    if results["error"]:
        print(f"\nErros: {len(results['error'])}")
        for t, e in results["error"]:
            print(f"  {t}: {e}")

    print(f"\nTotal processados: {len(results['ok']) + len(results['empty']) + len(results['error'])}")


if __name__ == "__main__":
    main()
