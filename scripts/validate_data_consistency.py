#!/usr/bin/env python3
"""
Análise de Consistência dos Dados Financeiros Históricos.

Executa regras de validação sobre company_financials_historical
para identificar dados com problemas de razoabilidade.

Uso:
    python scripts/validate_data_consistency.py [--fix] [--report] [--csv]

Opções:
    --fix     : Atualiza o campo data_quality no banco com os problemas encontrados
    --report  : Exibe relatório resumido no console
    --csv     : Gera CSV com todos os registros problemáticos em cache/
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Ajustar path para importações
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       'data', 'damodaran_data_new.db')

# ─────────────────────────────────────────────────────────────────────────────
# Definição das regras de validação
# ─────────────────────────────────────────────────────────────────────────────

RULES = [
    # ── CRITICAL ──────────────────────────────────────────────────────────────
    {
        'id': 'EV_EXTREME',
        'severity': 'critical',
        'description': 'EV/Receita absurdamente alto (>10.000x) com EV > 100B USD',
        'detail': 'EV estimado desproporcional à receita, indicando provável erro na estimativa de Market Cap (shares inflados). Exclui mega-caps legítimas.',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   enterprise_value_estimated, market_cap_estimated,
                   total_revenue, ordinary_shares_number
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND enterprise_value_estimated IS NOT NULL
              AND fx_rate_to_usd IS NOT NULL
              AND ABS(enterprise_value_estimated * fx_rate_to_usd) > 1e11
              AND total_revenue IS NOT NULL AND total_revenue > 0
              AND (enterprise_value_estimated / total_revenue) > 10000
        """,
        'fields': ['enterprise_value_estimated', 'market_cap_estimated',
                    'total_revenue', 'ordinary_shares_number'],
    },
    {
        'id': 'MCAP_EXTREME',
        'severity': 'critical',
        'description': 'Market Cap/Receita absurdamente alto (>5.000x)',
        'detail': 'Market Cap desproporcional à receita, indicando provável erro de shares. Exclui mega-caps legítimas.',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   market_cap_estimated, ordinary_shares_number, fx_rate_to_usd,
                   total_revenue
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND market_cap_estimated IS NOT NULL
              AND fx_rate_to_usd IS NOT NULL
              AND market_cap_estimated * fx_rate_to_usd > 1e9
              AND total_revenue IS NOT NULL AND total_revenue > 0
              AND (market_cap_estimated / total_revenue) > 5000
        """,
        'fields': ['market_cap_estimated', 'ordinary_shares_number', 'total_revenue'],
    },
    {
        'id': 'REVENUE_NEAR_ZERO',
        'severity': 'critical',
        'description': 'Receita ≈ 0 com margens/ratios calculados',
        'detail': 'Receita < $1.000 USD mas existem margens calculadas, gerando ratios extremos (divisão por quase zero)',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   total_revenue, fx_rate_to_usd,
                   ebitda_margin, net_margin, ev_revenue
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND total_revenue IS NOT NULL
              AND fx_rate_to_usd IS NOT NULL
              AND ABS(total_revenue * fx_rate_to_usd) < 1000
              AND (ebitda_margin IS NOT NULL OR net_margin IS NOT NULL
                   OR ev_revenue IS NOT NULL)
        """,
        'fields': ['total_revenue', 'ebitda_margin', 'net_margin', 'ev_revenue'],
    },
    {
        'id': 'NET_MARGIN_EXTREME',
        'severity': 'critical',
        'description': 'Margem líquida extrema (>10.000% ou <-10.000%)',
        'detail': 'Margem líquida com valor absoluto superior a 100x (10000%), indicando erro de escala ou receita desprezível',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   net_margin, net_income, total_revenue
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND net_margin IS NOT NULL
              AND ABS(net_margin) > 100
        """,
        'fields': ['net_margin', 'net_income', 'total_revenue'],
    },
    {
        'id': 'SHARES_EXTREME',
        'severity': 'critical',
        'description': 'Quantidade de ações extremamente alta (>100 bilhões)',
        'detail': 'Ordinary shares > 100B, provável erro de escala (unidades vs milhares), impacta Market Cap e EV',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   ordinary_shares_number, market_cap_estimated, total_revenue
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND ordinary_shares_number IS NOT NULL
              AND ordinary_shares_number > 1e11
        """,
        'fields': ['ordinary_shares_number', 'market_cap_estimated'],
    },

    # ── WARNING ───────────────────────────────────────────────────────────────
    {
        'id': 'EV_REVENUE_EXTREME',
        'severity': 'warning',
        'description': 'EV/Receita > 1.000x ou < -100x',
        'detail': 'Múltiplo EV/Receita fora de faixa razoável, típico quando EV está inflado ou receita é muito baixa',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   ev_revenue, enterprise_value_estimated, total_revenue
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND ev_revenue IS NOT NULL
              AND (ev_revenue > 1000 OR ev_revenue < -100)
        """,
        'fields': ['ev_revenue', 'enterprise_value_estimated', 'total_revenue'],
    },
    {
        'id': 'EV_EBITDA_EXTREME',
        'severity': 'warning',
        'description': 'EV/EBITDA > 10.000x ou < -1.000x',
        'detail': 'Múltiplo EV/EBITDA extremo, prejudica análises de valuation comparativo',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   ev_ebitda, enterprise_value_estimated, ebitda
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND ev_ebitda IS NOT NULL
              AND (ev_ebitda > 10000 OR ev_ebitda < -1000)
        """,
        'fields': ['ev_ebitda', 'enterprise_value_estimated', 'ebitda'],
    },
    {
        'id': 'DEBT_EBITDA_EXTREME',
        'severity': 'warning',
        'description': 'Dívida/EBITDA > 1.000x ou < -1.000x',
        'detail': 'Ratio de endividamento extremo, indica dado inconsistente ou empresa pré-receita',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   debt_ebitda, total_debt, ebitda
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND debt_ebitda IS NOT NULL
              AND ABS(debt_ebitda) > 1000
        """,
        'fields': ['debt_ebitda', 'total_debt', 'ebitda'],
    },
    {
        'id': 'MARGIN_WARNING',
        'severity': 'warning',
        'description': 'Margens entre 500% e 10.000%',
        'detail': 'Margem EBITDA, EBIT ou bruta com valor absoluto entre 5x e 100x da receita',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   ebitda_margin, ebit_margin, gross_margin, total_revenue
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND (
                  (ebitda_margin IS NOT NULL AND ABS(ebitda_margin) > 5 AND ABS(ebitda_margin) <= 100)
                  OR (ebit_margin IS NOT NULL AND ABS(ebit_margin) > 5 AND ABS(ebit_margin) <= 100)
                  OR (gross_margin IS NOT NULL AND ABS(gross_margin) > 5 AND ABS(gross_margin) <= 100)
              )
        """,
        'fields': ['ebitda_margin', 'ebit_margin', 'gross_margin', 'total_revenue'],
    },
    {
        'id': 'EBITDA_GT_REVENUE',
        'severity': 'warning',
        'description': 'EBITDA > Receita (margem > 100%)',
        'detail': 'EBITDA superior à receita total, possível erro de extração ou empresa com receita não-operacional',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   ebitda, total_revenue, ebitda_margin
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND ebitda IS NOT NULL AND total_revenue IS NOT NULL
              AND total_revenue > 0 AND ebitda > 0
              AND ebitda > total_revenue * 2
        """,
        'fields': ['ebitda', 'total_revenue', 'ebitda_margin'],
    },
    {
        'id': 'DEBT_EQUITY_EXTREME',
        'severity': 'warning',
        'description': 'Dívida/PL > 100x ou < -100x',
        'detail': 'Ratio de alavancagem extremo, possível PL próximo de zero',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   debt_equity, total_debt, stockholders_equity
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND debt_equity IS NOT NULL
              AND ABS(debt_equity) > 100
        """,
        'fields': ['debt_equity', 'total_debt', 'stockholders_equity'],
    },
    {
        'id': 'FCF_REVENUE_EXTREME',
        'severity': 'warning',
        'description': 'FCF/Receita > 500% ou < -500%',
        'detail': 'Free Cash Flow desproporcionalmente grande em relação à receita',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   fcf_revenue_ratio, free_cash_flow, total_revenue
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND fcf_revenue_ratio IS NOT NULL
              AND ABS(fcf_revenue_ratio) > 5
        """,
        'fields': ['fcf_revenue_ratio', 'free_cash_flow', 'total_revenue'],
    },
    {
        'id': 'CAPEX_REVENUE_EXTREME',
        'severity': 'warning',
        'description': 'Capex/Receita > 200%',
        'detail': 'Capex superior a 2x a receita, possível erro de extração',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   capex_revenue, capital_expenditure, total_revenue
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND capex_revenue IS NOT NULL
              AND capex_revenue > 2
        """,
        'fields': ['capex_revenue', 'capital_expenditure', 'total_revenue'],
    },

    # ── INFO ──────────────────────────────────────────────────────────────────
    {
        'id': 'EV_NEGATIVE',
        'severity': 'info',
        'description': 'Enterprise Value negativo',
        'detail': 'EV < 0, pode ocorrer quando caixa > market cap + dívida, mas merece revisão se for grande',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   enterprise_value_estimated, market_cap_estimated,
                   total_debt, cash_and_equivalents
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND enterprise_value_estimated IS NOT NULL
              AND enterprise_value_estimated < 0
              AND fx_rate_to_usd IS NOT NULL
              AND ABS(enterprise_value_estimated * fx_rate_to_usd) > 1e6
        """,
        'fields': ['enterprise_value_estimated', 'market_cap_estimated',
                    'total_debt', 'cash_and_equivalents'],
    },
    {
        'id': 'EQUITY_NEGATIVE',
        'severity': 'info',
        'description': 'Patrimônio Líquido negativo',
        'detail': 'PL < 0, pode ser legítimo (ex.: Starbucks, McDonald\'s) mas afeta ratio Dívida/PL',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   stockholders_equity, total_debt, debt_equity
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND stockholders_equity IS NOT NULL
              AND stockholders_equity < 0
        """,
        'fields': ['stockholders_equity', 'total_debt', 'debt_equity'],
    },
    {
        'id': 'REVENUE_NEGATIVE',
        'severity': 'info',
        'description': 'Receita negativa',
        'detail': 'Receita total negativa, invalida cálculos de margens',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   total_revenue, ebitda_margin, net_margin
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND total_revenue IS NOT NULL
              AND total_revenue < 0
        """,
        'fields': ['total_revenue', 'ebitda_margin', 'net_margin'],
    },
    {
        'id': 'EV_MCAP_INCONSISTENT',
        'severity': 'info',
        'description': 'EV inconsistente com Market Cap',
        'detail': 'EV difere de (MCap + Dívida - Caixa) por mais de 50%, possível erro de cálculo',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   enterprise_value_estimated, market_cap_estimated,
                   total_debt, cash_and_equivalents,
                   (market_cap_estimated + COALESCE(total_debt,0) - COALESCE(cash_and_equivalents,0)) as ev_calc
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND enterprise_value_estimated IS NOT NULL
              AND market_cap_estimated IS NOT NULL
              AND market_cap_estimated > 0
              AND ABS(enterprise_value_estimated -
                      (market_cap_estimated + COALESCE(total_debt,0) - COALESCE(cash_and_equivalents,0)))
                  > 0.5 * ABS(enterprise_value_estimated)
        """,
        'fields': ['enterprise_value_estimated', 'market_cap_estimated',
                    'total_debt', 'cash_and_equivalents'],
    },
    {
        'id': 'MISSING_KEY_FIELDS',
        'severity': 'info',
        'description': 'Campos-chave ausentes (receita, EBITDA e lucro líquido nulos)',
        'detail': 'Registro sem nenhum dado financeiro principal, possivelmente extração falhou',
        'sql': """
            SELECT id, yahoo_code, fiscal_year,
                   total_revenue, ebitda, net_income, total_assets
            FROM company_financials_historical
            WHERE period_type = 'annual'
              AND total_revenue IS NULL
              AND ebitda IS NULL
              AND net_income IS NULL
        """,
        'fields': ['total_revenue', 'ebitda', 'net_income', 'total_assets'],
    },
]


def run_validation(conn):
    """Executa todas as regras de validação e retorna os resultados."""
    cur = conn.cursor()

    # Total de registros anuais
    cur.execute("SELECT COUNT(*) FROM company_financials_historical WHERE period_type='annual'")
    total_annual = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT yahoo_code) FROM company_financials_historical WHERE period_type='annual'")
    total_companies = cur.fetchone()[0]

    results = {
        'timestamp': datetime.now().isoformat(),
        'total_records_annual': total_annual,
        'total_companies': total_companies,
        'summary': {'critical': 0, 'warning': 0, 'info': 0},
        'rules': [],
        'issues': [],          # lista flat de todos os problemas
        'affected_records': set(),  # IDs únicos com problemas
        'affected_companies': set(),
    }

    for rule in RULES:
        try:
            cur.execute(rule['sql'])
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

            rule_result = {
                'id': rule['id'],
                'severity': rule['severity'],
                'description': rule['description'],
                'detail': rule['detail'],
                'count': len(rows),
                'sample': [],
            }

            for row in rows:
                rec = dict(zip(cols, row))
                issue = {
                    'record_id': rec['id'],
                    'yahoo_code': rec['yahoo_code'],
                    'fiscal_year': rec['fiscal_year'],
                    'rule_id': rule['id'],
                    'severity': rule['severity'],
                    'description': rule['description'],
                    'data': {k: v for k, v in rec.items()
                             if k not in ('id', 'yahoo_code', 'fiscal_year')},
                }
                results['issues'].append(issue)
                results['affected_records'].add(rec['id'])
                results['affected_companies'].add(rec['yahoo_code'])

                if len(rule_result['sample']) < 5:
                    rule_result['sample'].append(rec)

            results['summary'][rule['severity']] += rule_result['count']
            results['rules'].append(rule_result)

        except Exception as e:
            print(f"  ERRO na regra {rule['id']}: {e}")
            results['rules'].append({
                'id': rule['id'], 'severity': rule['severity'],
                'description': rule['description'], 'count': -1,
                'error': str(e),
            })

    results['total_issues'] = len(results['issues'])
    results['total_affected_records'] = len(results['affected_records'])
    results['total_affected_companies'] = len(results['affected_companies'])
    results['pct_affected'] = round(
        100 * results['total_affected_records'] / total_annual, 2
    ) if total_annual else 0

    return results


def update_data_quality(conn, results):
    """Atualiza o campo data_quality no banco com base nos problemas encontrados."""
    cur = conn.cursor()

    # Primeiro, resetar todos para 'ok'
    cur.execute("UPDATE company_financials_historical SET data_quality = 'ok'")

    # Agrupar issues por record_id, priorizando por severidade
    severity_order = {'critical': 0, 'warning': 1, 'info': 2}
    record_issues = {}
    for issue in results['issues']:
        rid = issue['record_id']
        if rid not in record_issues:
            record_issues[rid] = []
        record_issues[rid].append(issue)

    # Atualizar cada registro com o problema mais grave
    for rid, issues in record_issues.items():
        issues.sort(key=lambda x: severity_order.get(x['severity'], 9))
        worst = issues[0]
        # Formato: "severity:RULE_ID[,RULE_ID2,...]"
        rule_ids = ','.join(sorted(set(i['rule_id'] for i in issues)))
        quality = f"{worst['severity']}:{rule_ids}"
        cur.execute(
            "UPDATE company_financials_historical SET data_quality = ? WHERE id = ?",
            (quality, rid)
        )

    conn.commit()
    updated = len(record_issues)
    print(f"  ✅ Atualizados {updated} registros no campo data_quality")
    return updated


def export_csv(results, output_dir='cache'):
    """Exporta registros problemáticos para CSV."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = os.path.join(output_dir, f'data_consistency_{ts}.csv')

    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow([
            'Código', 'Ano', 'Regra', 'Severidade', 'Descrição', 'Dados'
        ])
        for issue in results['issues']:
            data_str = ' | '.join(
                f"{k}={v}" for k, v in issue['data'].items() if v is not None
            )
            writer.writerow([
                issue['yahoo_code'],
                issue['fiscal_year'],
                issue['rule_id'],
                issue['severity'],
                issue['description'],
                data_str,
            ])

    print(f"  📄 CSV exportado: {filepath}")
    return filepath


def print_report(results):
    """Exibe relatório resumido no console."""
    print("\n" + "=" * 80)
    print("  ANÁLISE DE CONSISTÊNCIA DOS DADOS FINANCEIROS HISTÓRICOS")
    print("=" * 80)
    print(f"  Timestamp: {results['timestamp']}")
    print(f"  Total registros anuais: {results['total_records_annual']:,}")
    print(f"  Total empresas: {results['total_companies']:,}")
    print(f"  Registros com problemas: {results['total_affected_records']:,} "
          f"({results['pct_affected']}%)")
    print(f"  Empresas afetadas: {results['total_affected_companies']:,}")
    print()

    # Resumo por severidade
    colors = {'critical': '\033[91m', 'warning': '\033[93m', 'info': '\033[96m'}
    reset = '\033[0m'
    for sev in ['critical', 'warning', 'info']:
        c = colors.get(sev, '')
        print(f"  {c}● {sev.upper()}: {results['summary'][sev]:,} ocorrências{reset}")
    print()

    # Detalhes por regra
    print("-" * 80)
    print(f"  {'REGRA':<30} {'SEV':>8} {'QTD':>8}  DESCRIÇÃO")
    print("-" * 80)
    for rule in results['rules']:
        c = colors.get(rule['severity'], '')
        print(f"  {c}{rule['id']:<30} {rule['severity']:>8} {rule['count']:>8}{reset}  "
              f"{rule['description']}")
        if rule.get('sample'):
            for s in rule['sample'][:3]:
                vals = ', '.join(
                    f"{k}={_fmt_val(v)}" for k, v in s.items()
                    if k not in ('id',) and v is not None
                )
                print(f"    → {vals}")
    print("-" * 80)

    # Top empresas mais afetadas
    company_counts = {}
    for issue in results['issues']:
        code = issue['yahoo_code']
        company_counts[code] = company_counts.get(code, 0) + 1
    if company_counts:
        top = sorted(company_counts.items(), key=lambda x: -x[1])[:15]
        print(f"\n  TOP 15 EMPRESAS MAIS AFETADAS:")
        for code, cnt in top:
            print(f"    {code:<20} {cnt:>4} problemas")
    print()


def _fmt_val(v):
    """Formata valor para exibição."""
    if v is None:
        return '—'
    if isinstance(v, float):
        if abs(v) >= 1e12:
            return f"{v/1e12:.1f}T"
        if abs(v) >= 1e9:
            return f"{v/1e9:.1f}B"
        if abs(v) >= 1e6:
            return f"{v/1e6:.1f}M"
        if abs(v) >= 1e3:
            return f"{v/1e3:.1f}K"
        return f"{v:.2f}"
    return str(v)


def get_validation_results_for_api(conn):
    """Retorna resultados formatados para uso na API/template."""
    results = run_validation(conn)
    # Converter sets para listas para serialização JSON
    results['affected_records'] = list(results['affected_records'])
    results['affected_companies'] = sorted(list(results['affected_companies']))
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Análise de consistência dos dados financeiros históricos'
    )
    parser.add_argument('--fix', action='store_true',
                        help='Atualizar campo data_quality no banco')
    parser.add_argument('--report', action='store_true',
                        help='Exibir relatório resumido')
    parser.add_argument('--csv', action='store_true',
                        help='Gerar CSV com registros problemáticos')
    args = parser.parse_args()

    if not any([args.fix, args.report, args.csv]):
        args.report = True  # Default: mostrar relatório

    print(f"📊 Conectando ao banco: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    print("🔍 Executando validações...")
    results = run_validation(conn)

    if args.report:
        print_report(results)

    if args.csv:
        export_csv(results)

    if args.fix:
        print("💾 Atualizando data_quality no banco...")
        update_data_quality(conn, results)

    conn.close()
    print("✅ Concluído.")


if __name__ == '__main__':
    main()
