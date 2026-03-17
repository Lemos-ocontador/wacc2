#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gerenciador de Fontes de Dados WACC - Atualização e Auditoria

Gerencia a atualização de todas as bases de dados usadas no cálculo do WACC,
registra logs de auditoria e fornece status em tempo real.
"""

import sqlite3
import json
import os
import pandas as pd
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Generator
from pathlib import Path

logger = logging.getLogger(__name__)

_IS_GAE = os.environ.get('GAE_ENV', '').startswith('standard')

def _connect_db(path):
    if _IS_GAE:
        abs_path = os.path.abspath(path)
        uri = 'file:' + abs_path + '?immutable=1'
        return sqlite3.connect(uri, uri=True)
    return sqlite3.connect(path)


# ══════════════════════════════════════════════════════════════════════════════
# DEFINIÇÃO DAS FONTES DE DADOS
# ══════════════════════════════════════════════════════════════════════════════

DATA_SOURCES = [
    {
        "id": "risk_free_rate",
        "name": "Taxa Livre de Risco (Rf)",
        "description": "US Treasury 10-Year Bond Yield — média dos últimos 2 anos",
        "wacc_component": "Ke",
        "provider": "FRED / Federal Reserve",
        "frequency": "Diário",
        "audit_url": "https://fred.stlouisfed.org/series/DGS10",
        "audit_links": [
            {"label": "FRED — Série DGS10 (gráfico)", "url": "https://fred.stlouisfed.org/series/DGS10"},
            {"label": "Download CSV — FRED DGS10", "url": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"}
        ],
        "methodology": "Média aritmética das taxas de retorno (yield) dos últimos 2 anos do T-Bond 10Y. Fonte: FED St. Louis.",
        "bdwacc_field": "RF",
        "data_file": "BDWACC.json",
        "db_table": None,
        "icon": "fas fa-landmark",
        "color": "#1e3a5f"
    },
    {
        "id": "market_risk_premium",
        "name": "Prêmio de Risco de Mercado (ERP)",
        "description": "Equity Risk Premium — Média geométrica (Stocks − T.Bonds) 1928–atual",
        "wacc_component": "Ke",
        "provider": "Damodaran (NYU Stern)",
        "frequency": "Anual",
        "audit_url": "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/histretSP.html",
        "audit_links": [
            {"label": "Retornos Históricos S&P (HTML)", "url": "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/histretSP.html"},
            {"label": "📥 Download Excel (verificar média geométrica)", "url": "https://www.stern.nyu.edu/~adamodar/pc/datasets/histretSP.xls"},
            {"label": "ERP Implícito (alternativo)", "url": "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/implpr.html"}
        ],
        "methodology": "Média geométrica da diferença anual (S&P 500 total return − T-Bond return) desde 1928. O valor NÃO aparece diretamente na página HTML — verificar no Excel, aba de resumo, linha 'Geometric Average', coluna 'Stocks - T.Bonds'.",
        "bdwacc_field": "RM",
        "data_file": "BDWACC.json",
        "db_table": None,
        "icon": "fas fa-chart-line",
        "color": "#2563eb"
    },
    {
        "id": "country_risk",
        "name": "Risco País (CRP)",
        "description": "Country Risk Premium — spread soberano + ajuste volatilidade equity/bond",
        "wacc_component": "Ke",
        "provider": "Damodaran (NYU Stern)",
        "frequency": "Anual",
        "audit_url": "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html",
        "audit_links": [
            {"label": "Country Risk Premiums (HTML)", "url": "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html"},
            {"label": "📥 Download Excel — ctryprem.xlsx", "url": "https://www.stern.nyu.edu/~adamodar/pc/datasets/ctryprem.xlsx"}
        ],
        "methodology": "Spread de default soberano (rating Moody's + CDS) ajustado pela volatilidade relativa equity/bond de mercados emergentes. Brasil (Ba1): buscar na coluna 'Country Risk Premium'.",
        "bdwacc_field": "CR",
        "data_file": None,
        "db_table": "country_risk",
        "icon": "fas fa-globe-americas",
        "color": "#059669"
    },
    {
        "id": "sector_betas_global",
        "name": "Betas Setoriais + D/E (Global)",
        "description": "Beta, D/E por setor — todas as empresas mundiais (5 regiões)",
        "wacc_component": "Ke / Estrutura",
        "provider": "Damodaran (NYU Stern)",
        "frequency": "Anual",
        "audit_url": "https://www.stern.nyu.edu/~adamodar/pc/datasets/betaGlobal.xls",
        "audit_links": [
            {"label": "🌍 Excel Global", "url": "https://www.stern.nyu.edu/~adamodar/pc/datasets/betaGlobal.xls"},
            {"label": "Betas US (HTML)", "url": "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/Betas.html"},
            {"label": "Excel US", "url": "https://www.stern.nyu.edu/~adamodar/pc/datasets/betas.xls"}
        ],
        "data_file": None,
        "db_table": "damodaran_global",
        "icon": "fas fa-globe",
        "color": "#7c3aed"
    },
    {
        "id": "sector_betas_emkt",
        "name": "Betas Setoriais + D/E (Emerg.)",
        "description": "Beta, D/E por setor — mercados emergentes (broad_group=Emerging Markets)",
        "wacc_component": "Ke / Estrutura",
        "provider": "Damodaran (NYU Stern)",
        "frequency": "Anual",
        "audit_url": "https://www.stern.nyu.edu/~adamodar/pc/datasets/betaemerg.xls",
        "audit_links": [
            {"label": "🌎 Excel Emergentes", "url": "https://www.stern.nyu.edu/~adamodar/pc/datasets/betaemerg.xls"}
        ],
        "data_file": None,
        "db_table": "damodaran_global",
        "db_filter": "broad_group = 'Emerging Markets'",
        "icon": "fas fa-earth-americas",
        "color": "#9333ea"
    },
    {
        "id": "size_premium",
        "name": "Prêmio de Tamanho (SP)",
        "description": "Size Premium por decil de Market Cap — atualização anual manual",
        "wacc_component": "Ke",
        "provider": "Kroll / Duff & Phelps",
        "frequency": "Anual (manual)",
        "audit_url": "https://www.kroll.com/en/cost-of-capital",
        "audit_links": [
            {"label": "Kroll Cost of Capital", "url": "https://www.kroll.com/en/cost-of-capital"},
            {"label": "BDSize.json (local)", "url": "/api/size_premium_data"}
        ],
        "data_file": "BDSize.json",
        "db_table": "size_premium",
        "icon": "fas fa-expand-arrows-alt",
        "color": "#ea580c",
        "manual_update": True,
        "update_note": "Dados de Kroll/Duff & Phelps. Requer input manual anual do relatório."
    },
    {
        "id": "selic_rate",
        "name": "Taxa Selic (→ Kd)",
        "description": "Taxa básica de juros — proxy para custo da dívida (Kd = 150% Selic)",
        "wacc_component": "Ki",
        "provider": "Banco Central do Brasil (SGS série 432)",
        "frequency": "Diário",
        "audit_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/10?formato=json",
        "audit_links": [
            {"label": "API BCB Selic (JSON)", "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/10?formato=json"},
            {"label": "Página BCB Selic", "url": "https://www.bcb.gov.br/controleinflacao/taxaselic"}
        ],
        "data_file": "BDWACC.json",
        "db_table": None,
        "icon": "fas fa-university",
        "color": "#374151",
        "bcb_series": 432
    },
    {
        "id": "inflation_brazil",
        "name": "Inflação Brasil (IPCA)",
        "description": "IPCA acumulado 12 meses — ajuste de inflação para WACC real",
        "wacc_component": "Ajuste",
        "provider": "Banco Central do Brasil (SGS série 13522)",
        "frequency": "Mensal",
        "audit_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.13522/dados/ultimos/12?formato=json",
        "audit_links": [
            {"label": "API BCB IPCA 12m (JSON)", "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.13522/dados/ultimos/12?formato=json"},
            {"label": "Página BCB Indicadores", "url": "https://www.bcb.gov.br/controleinflacao/indicadores"}
        ],
        "data_file": "BDWACC.json",
        "db_table": None,
        "icon": "fas fa-percentage",
        "color": "#dc2626",
        "bcb_series": 13522
    },
]


class DataSourceManager:
    """
    Gerencia atualização, auditoria e status de todas as fontes de dados WACC.
    """

    def __init__(self, db_path: str = "data/damodaran_data_new.db"):
        self.db_path = db_path
        self.read_only = False
        self._ensure_log_table()

    # ──────────────────────────────────────────────────────────────────────
    # SETUP
    # ──────────────────────────────────────────────────────────────────────

    def _get_conn(self):
        return _connect_db(self.db_path)

    def _ensure_log_table(self):
        """Cria tabela de log de atualizações se não existir."""
        try:
            conn = self._get_conn()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_update_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    records_count INTEGER DEFAULT 0,
                    last_value TEXT,
                    reference_year INTEGER,
                    reference_date TEXT,
                    audit_url TEXT,
                    update_started_at TEXT,
                    update_completed_at TEXT,
                    duration_seconds REAL,
                    error_message TEXT,
                    details TEXT,
                    created_at TEXT DEFAULT (datetime('now', 'localtime'))
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_update_source 
                ON data_update_log(source_id, created_at DESC)
            """)
            conn.commit()
        except Exception:
            self.read_only = True
        conn.close()
        logger.info("Tabela data_update_log verificada/criada")

    # ──────────────────────────────────────────────────────────────────────
    # STATUS DAS FONTES
    # ──────────────────────────────────────────────────────────────────────

    def get_all_sources_status(self) -> List[Dict[str, Any]]:
        """Retorna status de todas as fontes de dados com última atualização."""
        conn = self._get_conn()
        results = []

        for source in DATA_SOURCES:
            # Buscar última atualização bem-sucedida
            row = conn.execute("""
                SELECT status, records_count, last_value, reference_year,
                       reference_date, update_completed_at, duration_seconds, 
                       error_message, details
                FROM data_update_log
                WHERE source_id = ? AND status = 'success'
                ORDER BY created_at DESC LIMIT 1
            """, (source["id"],)).fetchone()

            # Buscar última tentativa (qualquer status)
            last_attempt = conn.execute("""
                SELECT status, update_completed_at, error_message
                FROM data_update_log
                WHERE source_id = ?
                ORDER BY created_at DESC LIMIT 1
            """, (source["id"],)).fetchone()

            # Contar registros na tabela se houver
            record_count = self._get_db_record_count(conn, source)

            info = {
                **source,
                "db_records": record_count,
                "last_success": None,
                "last_attempt": None,
            }

            if row:
                info["last_success"] = {
                    "status": row[0],
                    "records_count": row[1],
                    "last_value": row[2],
                    "reference_year": row[3],
                    "reference_date": row[4],
                    "completed_at": row[5],
                    "duration_seconds": row[6],
                    "details": row[8],
                }

            if last_attempt:
                info["last_attempt"] = {
                    "status": last_attempt[0],
                    "completed_at": last_attempt[1],
                    "error_message": last_attempt[2],
                }

            results.append(info)

        conn.close()
        return results

    def _get_db_record_count(self, conn, source: dict) -> Optional[int]:
        """Conta registros na tabela do banco para a fonte (com filtro opcional)."""
        table = source.get("db_table")
        if not table:
            return None
        try:
            db_filter = source.get("db_filter")
            if db_filter:
                row = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {db_filter}").fetchone()
            else:
                row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return row[0] if row else 0
        except Exception:
            return None

    # ──────────────────────────────────────────────────────────────────────
    # ATUALIZAÇÃO DE CADA FONTE
    # ──────────────────────────────────────────────────────────────────────

    def update_source(self, source_id: str) -> Dict[str, Any]:
        """
        Atualiza uma fonte de dados específica.
        Retorna dict com resultado da operação.
        """
        source = next((s for s in DATA_SOURCES if s["id"] == source_id), None)
        if not source:
            return {"success": False, "error": f"Fonte '{source_id}' não encontrada"}

        started_at = datetime.now()
        log_id = self._log_start(source_id, source["name"], source["audit_url"])

        try:
            result = getattr(self, f"_update_{source_id}")()
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            self._log_complete(
                log_id,
                status="success",
                records_count=result.get("records_count", 0),
                last_value=result.get("last_value", ""),
                reference_year=result.get("reference_year"),
                reference_date=result.get("reference_date"),
                duration=duration,
                details=json.dumps(result.get("details", {}), ensure_ascii=False),
            )

            return {
                "success": True,
                "source_id": source_id,
                "source_name": source["name"],
                "records_count": result.get("records_count", 0),
                "last_value": result.get("last_value", ""),
                "reference_year": result.get("reference_year"),
                "duration_seconds": round(duration, 2),
                "audit_url": source["audit_url"],
            }

        except Exception as e:
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()
            error_msg = str(e)
            logger.error(f"Erro ao atualizar {source_id}: {error_msg}")

            self._log_complete(
                log_id,
                status="error",
                duration=duration,
                error_message=error_msg,
            )

            return {
                "success": False,
                "source_id": source_id,
                "source_name": source["name"],
                "error": error_msg,
                "duration_seconds": round(duration, 2),
            }

    def update_all_sources(self) -> Generator[Dict[str, Any], None, None]:
        """
        Atualiza todas as fontes sequencialmente, yield resultado de cada uma.
        Ideal para SSE (Server-Sent Events).
        """
        total = len(DATA_SOURCES)
        for i, source in enumerate(DATA_SOURCES):
            yield {
                "type": "progress",
                "current": i + 1,
                "total": total,
                "source_id": source["id"],
                "source_name": source["name"],
                "status": "updating",
            }

            result = self.update_source(source["id"])

            yield {
                "type": "result",
                "current": i + 1,
                "total": total,
                **result,
            }

        yield {"type": "complete", "total": total}

    # ──────────────────────────────────────────────────────────────────────
    # IMPLEMENTAÇÃO DE CADA ATUALIZADOR
    # ──────────────────────────────────────────────────────────────────────

    def _update_risk_free_rate(self) -> Dict[str, Any]:
        """Verifica/atualiza a taxa livre de risco do BDWACC.json."""
        bd = self._load_bdwacc()
        rf_entry = next((x for x in bd if x.get("Campo") == "RF"), None)
        if not rf_entry:
            raise ValueError("Campo RF não encontrado no BDWACC.json")

        value_str = rf_entry["Valor"].replace(",", ".").replace("%", "").strip()
        return {
            "records_count": 1,
            "last_value": rf_entry["Valor"],
            "reference_year": rf_entry.get("[ANO_REFER]"),
            "reference_date": datetime.now().strftime("%Y-%m-%d"),
            "details": {
                "campo": "RF",
                "nome": rf_entry["Nome"],
                "valor": rf_entry["Valor"],
                "ano_referencia": rf_entry.get("[ANO_REFER]"),
            },
        }

    def _update_market_risk_premium(self) -> Dict[str, Any]:
        """Verifica/atualiza o prêmio de risco de mercado do BDWACC.json."""
        bd = self._load_bdwacc()
        rm_entry = next((x for x in bd if x.get("Campo") == "RM"), None)
        if not rm_entry:
            raise ValueError("Campo RM não encontrado no BDWACC.json")

        return {
            "records_count": 1,
            "last_value": rm_entry["Valor"],
            "reference_year": rm_entry.get("[ANO_REFER]"),
            "reference_date": datetime.now().strftime("%Y-%m-%d"),
            "details": {
                "campo": "RM",
                "nome": rm_entry["Nome"],
                "valor": rm_entry["Valor"],
                "ano_referencia": rm_entry.get("[ANO_REFER]"),
            },
        }

    def _update_country_risk(self) -> Dict[str, Any]:
        """Verifica dados de risco país no SQLite."""
        conn = self._get_conn()
        row = conn.execute("SELECT COUNT(*) FROM country_risk").fetchone()
        total = row[0] if row else 0

        # Buscar valor do Brasil como referência
        br = conn.execute("""
            SELECT country, risk_premium, created_at 
            FROM country_risk WHERE country = 'Brazil'
        """).fetchone()
        conn.close()

        last_value = f"Brasil: {float(br[1])*100:.2f}%" if br else "N/A"
        created_at = br[2] if br and len(br) > 2 else None

        return {
            "records_count": total,
            "last_value": last_value,
            "reference_year": datetime.now().year,
            "reference_date": created_at or datetime.now().strftime("%Y-%m-%d"),
            "details": {
                "total_paises": total,
                "brasil_crp": last_value,
            },
        }

    def _update_sector_betas_global(self) -> Dict[str, Any]:
        """Verifica dados de betas setoriais GLOBAIS no SQLite (todas as regiões)."""
        conn = self._get_conn()

        total = conn.execute("SELECT COUNT(*) FROM damodaran_global").fetchone()[0]

        sectors = conn.execute("""
            SELECT COUNT(DISTINCT industry) FROM damodaran_global
            WHERE industry IS NOT NULL AND industry != ''
        """).fetchone()[0]

        avg_beta = conn.execute("""
            SELECT AVG(CAST(beta AS REAL))
            FROM damodaran_global
            WHERE beta IS NOT NULL AND beta != '' AND beta != 'None'
              AND CAST(beta AS REAL) > 0 AND CAST(beta AS REAL) < 10
        """).fetchone()[0]

        # Distribuição por região
        regions = conn.execute("""
            SELECT broad_group, COUNT(*) as cnt
            FROM damodaran_global
            WHERE broad_group IS NOT NULL
            GROUP BY broad_group ORDER BY cnt DESC
        """).fetchall()

        conn.close()
        regions_detail = {r[0]: r[1] for r in regions}

        return {
            "records_count": total,
            "last_value": f"{sectors} setores, {total:,} empresas, β̄={avg_beta:.3f}" if avg_beta else f"{sectors} setores",
            "reference_year": datetime.now().year,
            "reference_date": datetime.now().strftime("%Y-%m-%d"),
            "details": {
                "total_empresas": total,
                "total_setores": sectors,
                "beta_medio": round(avg_beta, 4) if avg_beta else None,
                "regioes": regions_detail,
            },
        }

    def _update_sector_betas_emkt(self) -> Dict[str, Any]:
        """Verifica dados de betas setoriais EMERGING MARKETS no SQLite."""
        conn = self._get_conn()
        emkt_filter = "broad_group = 'Emerging Markets'"

        total = conn.execute(
            f"SELECT COUNT(*) FROM damodaran_global WHERE {emkt_filter}"
        ).fetchone()[0]

        sectors = conn.execute(f"""
            SELECT COUNT(DISTINCT industry) FROM damodaran_global
            WHERE {emkt_filter} AND industry IS NOT NULL AND industry != ''
        """).fetchone()[0]

        avg_beta = conn.execute(f"""
            SELECT AVG(CAST(beta AS REAL))
            FROM damodaran_global
            WHERE {emkt_filter}
              AND beta IS NOT NULL AND beta != '' AND beta != 'None'
              AND CAST(beta AS REAL) > 0 AND CAST(beta AS REAL) < 10
        """).fetchone()[0]

        countries = conn.execute(f"""
            SELECT COUNT(DISTINCT country) FROM damodaran_global
            WHERE {emkt_filter}
        """).fetchone()[0]

        # Top países por qtd de empresas
        top_countries = conn.execute(f"""
            SELECT country, COUNT(*) as cnt
            FROM damodaran_global
            WHERE {emkt_filter} AND country IS NOT NULL
            GROUP BY country ORDER BY cnt DESC LIMIT 10
        """).fetchall()

        conn.close()

        return {
            "records_count": total,
            "last_value": f"{sectors} setores, {total:,} empresas, {countries} países, β̄={avg_beta:.3f}" if avg_beta else f"{sectors} setores, {total:,} empresas",
            "reference_year": datetime.now().year,
            "reference_date": datetime.now().strftime("%Y-%m-%d"),
            "details": {
                "total_empresas": total,
                "total_setores": sectors,
                "total_paises": countries,
                "beta_medio_emkt": round(avg_beta, 4) if avg_beta else None,
                "top_paises": {c[0]: c[1] for c in top_countries},
            },
        }

    def _update_size_premium(self) -> Dict[str, Any]:
        """Verifica dados de size premium — BDSize.json + SQLite. Atualização manual anual."""
        conn = self._get_conn()
        try:
            total_db = conn.execute("SELECT COUNT(*) FROM size_premium").fetchone()[0]
        except Exception:
            total_db = 0
        conn.close()

        bd_size = self._load_bdsize()
        ano_ref = bd_size[0].get("[ANO_REFER]") if bd_size else None

        # Montar resumo dos decis
        decis_resumo = []
        for d in bd_size:
            decis_resumo.append({
                "decil": d.get("Tamanho"),
                "de": d.get(" De ", "").strip(),
                "ate": d.get(" até ", "").strip(),
                "premio": d.get("Premio"),
            })

        return {
            "records_count": len(bd_size),
            "last_value": f"{len(bd_size)} decis, ano-ref: {ano_ref}",
            "reference_year": ano_ref,
            "reference_date": datetime.now().strftime("%Y-%m-%d"),
            "details": {
                "decis_json": len(bd_size),
                "decis_db": total_db,
                "ano_referencia": ano_ref,
                "fonte": "Kroll / Duff & Phelps Cost of Capital Navigator",
                "nota": "Dados atualizados anualmente. Requer input manual do relatório Kroll.",
                "decis": decis_resumo,
            },
        }

    def _update_selic_rate(self) -> Dict[str, Any]:
        """Atualiza taxa Selic consultando API BCB (SGS 432) + BDWACC.json."""
        api_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/10?formato=json"
        bcb_data = None
        selic_atual = None

        try:
            resp = requests.get(api_url, timeout=15)
            if resp.status_code == 200:
                bcb_data = resp.json()
                if bcb_data:
                    ultimo = bcb_data[-1]
                    selic_atual = float(ultimo['valor'])
        except Exception as e:
            logger.warning(f"Falha ao consultar API BCB Selic: {e}")

        # BDWACC.json como referência/backup
        bd = self._load_bdwacc()
        ct_entry = next((x for x in bd if x.get("Campo") == "CT"), None)
        ct_valor = ct_entry["Valor"] if ct_entry else "N/A"

        details = {
            "campo": "CT",
            "valor_bdwacc": ct_valor,
            "nota": "Kd = 150% da Selic como proxy para custo da dívida",
            "api_url": api_url,
        }

        if selic_atual is not None:
            details["selic_atual_bcb"] = f"{selic_atual:.2f}%"
            details["selic_data"] = bcb_data[-1]['data'] if bcb_data else None
            details["kd_calculado"] = f"{selic_atual * 1.5:.2f}%"
            details["ultimos_valores"] = [
                {"data": d["data"], "valor": f"{float(d['valor']):.2f}%"}
                for d in (bcb_data[-5:] if bcb_data else [])
            ]
            last_value = f"Selic: {selic_atual:.2f}% → Kd(150%): {selic_atual*1.5:.2f}%"
        else:
            last_value = ct_valor
            details["aviso"] = "API BCB indisponível, usando BDWACC.json"

        return {
            "records_count": 1,
            "last_value": last_value,
            "reference_year": ct_entry.get("[ANO_REFER]") if ct_entry else datetime.now().year,
            "reference_date": datetime.now().strftime("%Y-%m-%d"),
            "details": details,
        }

    def _update_inflation_brazil(self) -> Dict[str, Any]:
        """Atualiza inflação Brasil consultando API BCB (SGS 13522) + BDWACC.json."""
        api_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.13522/dados/ultimos/12?formato=json"
        bcb_data = None
        ipca_12m = None

        try:
            resp = requests.get(api_url, timeout=15)
            if resp.status_code == 200:
                bcb_data = resp.json()
                if bcb_data:
                    ultimo = bcb_data[-1]
                    ipca_12m = float(ultimo['valor'])
        except Exception as e:
            logger.warning(f"Falha ao consultar API BCB IPCA: {e}")

        bd = self._load_bdwacc()
        ib_entry = next((x for x in bd if x.get("Campo") == "IB"), None)
        ia_entry = next((x for x in bd if x.get("Campo") == "IA"), None)
        ib_val = ib_entry["Valor"] if ib_entry else "N/A"
        ia_val = ia_entry["Valor"] if ia_entry else "N/A"

        details = {
            "inflacao_brasil_bdwacc": ib_val,
            "inflacao_eua_bdwacc": ia_val,
            "api_url": api_url,
        }

        if ipca_12m is not None:
            details["ipca_12m_bcb"] = f"{ipca_12m:.2f}%"
            details["ipca_data"] = bcb_data[-1]['data'] if bcb_data else None
            details["ultimos_12_meses"] = [
                {"data": d["data"], "valor": f"{float(d['valor']):.2f}%"}
                for d in (bcb_data if bcb_data else [])
            ]
            last_value = f"IPCA 12m: {ipca_12m:.2f}% (BCB) | BDWACC: {ib_val}"
        else:
            last_value = f"IPCA: {ib_val}, CPI-US: {ia_val}"
            details["aviso"] = "API BCB indisponível, usando BDWACC.json"

        return {
            "records_count": 2,
            "last_value": last_value,
            "reference_year": ib_entry.get("[ANO_REFER]") if ib_entry else datetime.now().year,
            "reference_date": datetime.now().strftime("%Y-%m-%d"),
            "details": details,
        }

    # ──────────────────────────────────────────────────────────────────────
    # HELPERS: JSON
    # ──────────────────────────────────────────────────────────────────────

    def _load_bdwacc(self) -> list:
        path = Path("static/BDWACC.json")
        if not path.exists():
            raise FileNotFoundError("BDWACC.json não encontrado em static/")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # O wacc_data_connector parseia como dict com chave Campo→Valor
        return data

    def _load_bdsize(self) -> list:
        path = Path("static/BDSize.json")
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ──────────────────────────────────────────────────────────────────────
    # LOGGING
    # ──────────────────────────────────────────────────────────────────────

    def _log_start(self, source_id: str, source_name: str, audit_url: str) -> int:
        if self.read_only:
            return -1
        conn = self._get_conn()
        cursor = conn.execute("""
            INSERT INTO data_update_log 
                (source_id, source_name, status, audit_url, update_started_at)
            VALUES (?, ?, 'running', ?, datetime('now', 'localtime'))
        """, (source_id, source_name, audit_url))
        log_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return log_id

    def _log_complete(self, log_id: int, status: str, records_count: int = 0,
                      last_value: str = "", reference_year: int = None,
                      reference_date: str = None, duration: float = 0,
                      error_message: str = None, details: str = None):
        if self.read_only:
            return
        conn = self._get_conn()
        conn.execute("""
            UPDATE data_update_log SET
                status = ?,
                records_count = ?,
                last_value = ?,
                reference_year = ?,
                reference_date = ?,
                update_completed_at = datetime('now', 'localtime'),
                duration_seconds = ?,
                error_message = ?,
                details = ?
            WHERE id = ?
        """, (status, records_count, last_value, reference_year,
              reference_date, duration, error_message, details, log_id))
        conn.commit()
        conn.close()

    # ──────────────────────────────────────────────────────────────────────
    # HISTÓRICO
    # ──────────────────────────────────────────────────────────────────────

    def get_update_history(self, source_id: str = None, limit: int = 20) -> List[Dict]:
        """Retorna histórico de atualizações."""
        if self.read_only:
            return []
        conn = self._get_conn()
        if source_id:
            rows = conn.execute("""
                SELECT id, source_id, source_name, status, records_count,
                       last_value, reference_year, reference_date,
                       audit_url, update_started_at, update_completed_at,
                       duration_seconds, error_message, details, created_at
                FROM data_update_log
                WHERE source_id = ?
                ORDER BY created_at DESC LIMIT ?
            """, (source_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT id, source_id, source_name, status, records_count,
                       last_value, reference_year, reference_date,
                       audit_url, update_started_at, update_completed_at,
                       duration_seconds, error_message, details, created_at
                FROM data_update_log
                ORDER BY created_at DESC LIMIT ?
            """, (limit,)).fetchall()
        conn.close()

        cols = ["id", "source_id", "source_name", "status", "records_count",
                "last_value", "reference_year", "reference_date",
                "audit_url", "update_started_at", "update_completed_at",
                "duration_seconds", "error_message", "details", "created_at"]

        return [dict(zip(cols, r)) for r in rows]
