import sqlite3
import json
import os
import pandas as pd
import requests
from typing import Dict, List, Optional, Any
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_IS_GAE = os.environ.get('GAE_ENV', '').startswith('standard')

def _connect_db(path):
    if _IS_GAE:
        abs_path = os.path.abspath(path)
        uri = 'file:' + abs_path + '?immutable=1'
        return sqlite3.connect(uri, uri=True)
    return sqlite3.connect(path)

# ══════════════════════════════════════════════════════════════════════════
# FALLBACKS ABSURDOS — se aparecerem na interface, algo está quebrado!
# ══════════════════════════════════════════════════════════════════════════
FALLBACK_RF  = 666.66   # Taxa Livre de Risco — impossível
FALLBACK_RM  = 777.77   # Prêmio de Risco de Mercado — impossível
FALLBACK_CT  = 888.88   # Custo de Dívida (Selic) — impossível
FALLBACK_IR  = 999.99   # Alíquota IR — impossível
FALLBACK_IB  = 555.55   # Inflação Brasil — impossível
FALLBACK_IA  = 444.44   # Inflação EUA — impossível
FALLBACK_CR  = 333.33   # Risco País — impossível

# URLs da API do Banco Central do Brasil (SGS)
BCB_SELIC_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/5?formato=json"
BCB_IPCA_12M_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.13522/dados/ultimos/3?formato=json"

class WACCDataConnector:
    """
    Conector para dados WACC integrados.
    Conecta com bancos de dados SQLite e arquivos JSON para fornecer
    todos os componentes necessários para cálculo do WACC.
    """
    
    def __init__(self, 
                 damodaran_db_path: str = "data/damodaran_data_new.db",
                 country_risk_db_path: str = "data/damodaran_data_new.db",
                 wacc_json_path: str = "static/BDWACC.json"):
        """
        Inicializar o conector WACC.
        
        Args:
            damodaran_db_path: Caminho para banco de dados Damodaran global
            country_risk_db_path: Caminho para banco de dados de risco país
            wacc_json_path: Caminho para arquivo JSON com componentes WACC
        """
        self.damodaran_db = damodaran_db_path
        self.country_risk_db = country_risk_db_path
        self.wacc_json = wacc_json_path
        
        # Cache para dados frequentemente acessados
        self._wacc_components_cache = None
        self._sectors_cache = None
        self._countries_cache = None
        self._bcb_cache = {}  # Cache para chamadas BCB API
        
        logger.info("WACCDataConnector inicializado")
    
    # ──────────────────────────────────────────────────────────────────────
    # PARSING DO BDWACC.JSON (lista→dict) + BCB API
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_br_number(value_str: str) -> float:
        """Converte '4,14%' ou '13,50%' ou '34,00%' para float numérico."""
        if not value_str or not isinstance(value_str, str):
            return 0.0
        clean = value_str.replace('%', '').replace('.', '').replace(',', '.').strip()
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def _load_wacc_components(self) -> Dict[str, Any]:
        """
        Carregar componentes WACC do arquivo JSON.
        Converte lista [{Campo: 'RF', Valor: '4,14%'}, ...] em dict {'RF': 4.14, ...}
        
        Returns:
            Dict com componentes WACC (campo→valor numérico)
        """
        if self._wacc_components_cache is not None:
            return self._wacc_components_cache

        result = {}
        paths_to_try = [self.wacc_json, "static/BDWACC.json", "BDWACC.json"]
        
        for path_str in paths_to_try:
            path = Path(path_str)
            if path.exists():
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        raw = json.load(f)
                    
                    if isinstance(raw, list):
                        # Converter lista de dicts para dict {Campo: valor_numerico}
                        for item in raw:
                            campo = item.get('Campo', '')
                            valor = item.get('Valor', '')
                            if campo:
                                result[campo] = self._parse_br_number(valor)
                                # Preservar metadados
                                result[f'{campo}_raw'] = valor
                                result[f'{campo}_ano'] = item.get('[ANO_REFER]')
                        logger.info(f"BDWACC.json carregado de {path}: {list(result.keys())}")
                    elif isinstance(raw, dict):
                        result = raw
                        logger.info(f"BDWACC.json carregado como dict de {path}")
                    else:
                        logger.warning(f"BDWACC.json formato inesperado: {type(raw)}")
                    break
                except Exception as e:
                    logger.error(f"Erro ao carregar {path}: {e}")
                    continue
        
        if not result:
            logger.error("BDWACC.json não encontrado em nenhum caminho! Usando FALLBACKS ABSURDOS.")
            result = {
                'RF': FALLBACK_RF, 'RM': FALLBACK_RM, 'CT': FALLBACK_CT,
                'IR': FALLBACK_IR, 'IB': FALLBACK_IB, 'IA': FALLBACK_IA,
                'CR': FALLBACK_CR,
            }
        
        self._wacc_components_cache = result
        return result

    def _fetch_bcb_series(self, url: str, cache_key: str) -> Optional[float]:
        """Busca o último valor de uma série do BCB SGS. Cache de 30min."""
        import time
        cached = self._bcb_cache.get(cache_key)
        if cached and (time.time() - cached['ts']) < 1800:  # 30 min
            return cached['value']
        
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    value = float(data[-1]['valor'])
                    self._bcb_cache[cache_key] = {'value': value, 'ts': time.time(), 'data': data[-1]['data']}
                    logger.info(f"BCB {cache_key}: {value} ({data[-1]['data']})")
                    return value
        except Exception as e:
            logger.warning(f"Falha API BCB {cache_key}: {e}")
        return None

    def get_selic_live(self) -> Dict[str, Any]:
        """Obtém Selic ao vivo da API BCB. Fallback para BDWACC.json."""
        selic = self._fetch_bcb_series(BCB_SELIC_URL, 'selic')
        
        if selic is not None:
            kd_150 = selic * 1.5
            cached = self._bcb_cache.get('selic', {})
            return {
                'success': True,
                'selic_percentage': round(selic, 2),
                'kd_percentage': round(kd_150, 2),
                'source': 'BCB API (série 432)',
                'date': cached.get('data', ''),
                'is_live': True,
            }
        
        # Fallback para BDWACC.json
        wacc = self._load_wacc_components()
        ct = wacc.get('CT', FALLBACK_CT)
        return {
            'success': True,
            'selic_percentage': round(ct / 1.5, 2),
            'kd_percentage': round(ct, 2),
            'source': 'BDWACC.json (fallback)',
            'date': '',
            'is_live': False,
            'warning': 'API BCB indisponível, usando BDWACC.json' if ct != FALLBACK_CT else 'FALLBACK ABSURDO — dados não carregados!',
        }

    def get_ipca_live(self) -> Dict[str, Any]:
        """Obtém IPCA 12m ao vivo da API BCB. Fallback para BDWACC.json."""
        ipca = self._fetch_bcb_series(BCB_IPCA_12M_URL, 'ipca_12m')
        
        if ipca is not None:
            cached = self._bcb_cache.get('ipca_12m', {})
            return {
                'success': True,
                'ipca_percentage': round(ipca, 2),
                'source': 'BCB API (série 13522)',
                'date': cached.get('data', ''),
                'is_live': True,
            }
        
        wacc = self._load_wacc_components()
        ib = wacc.get('IB', FALLBACK_IB)
        return {
            'success': True,
            'ipca_percentage': round(ib, 2),
            'source': 'BDWACC.json (fallback)',
            'date': '',
            'is_live': False,
            'warning': 'API BCB indisponível' if ib != FALLBACK_IB else 'FALLBACK ABSURDO!',
        }
    
    def get_risk_free_rate_options(self) -> Dict[str, Any]:
        """
        Obter opções disponíveis para taxa livre de risco.
        """
        try:
            wacc_data = self._load_wacc_components()
            rf_rate = wacc_data.get('RF', FALLBACK_RF)
            
            return {
                'success': True,
                'options': [
                    {
                        'id': '10y',
                        'name': 'US Treasury 10Y',
                        'description': 'Taxa do Tesouro Americano 10 anos',
                        'current_rate': rf_rate,
                        'source': 'BDWACC.json' if rf_rate != FALLBACK_RF else 'FALLBACK!'
                    },
                    {
                        'id': '30y',
                        'name': 'US Treasury 30Y',
                        'description': 'Taxa do Tesouro Americano 30 anos',
                        'current_rate': rf_rate + 0.5 if rf_rate != FALLBACK_RF else FALLBACK_RF,
                        'source': 'BDWACC.json' if rf_rate != FALLBACK_RF else 'FALLBACK!'
                    },
                    {
                        'id': 'custom',
                        'name': 'Taxa Personalizada',
                        'description': 'Inserir taxa manualmente',
                        'current_rate': None,
                        'source': 'Manual'
                    }
                ],
                'default': '10y',
                'is_fallback': rf_rate == FALLBACK_RF,
                'last_updated': pd.Timestamp.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Erro ao obter opções de taxa livre de risco: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_risk_free_rate(self, term: str = '10y') -> Dict[str, Any]:
        """
        Obter taxa livre de risco específica.
        """
        try:
            wacc_data = self._load_wacc_components()
            rf_rate = wacc_data.get('RF', FALLBACK_RF)
            
            if term == '30y':
                rate = rf_rate + 0.5 if rf_rate != FALLBACK_RF else FALLBACK_RF
            else:
                rate = rf_rate
            
            return {
                'success': True,
                'term': term,
                'rate_decimal': rate / 100,
                'rate_percentage': rate,
                'source': 'BDWACC.json' if rf_rate != FALLBACK_RF else 'FALLBACK ABSURDO!',
                'is_fallback': rf_rate == FALLBACK_RF,
                'reference_year': wacc_data.get('RF_ano'),
                'last_updated': pd.Timestamp.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Erro ao obter taxa livre de risco: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_available_sectors(self) -> Dict[str, Any]:
        """
        Obter setores disponíveis para cálculo de beta.
        Usa metodologia Damodaran: beta = média simples (incl. negativos),
        D/E = média ponderada por market_cap.
        
        Returns:
            Dict com lista de setores
        """
        if self._sectors_cache is not None:
            return self._sectors_cache
        
        try:
            conn = _connect_db(self.damodaran_db)
            
            # Metodologia Damodaran:
            # - Beta: média simples de TODOS os betas válidos (inclui negativos)
            # - D/E: média ponderada por market_cap
            # - Unlevered beta: bottom_up_beta_for_sector (pré-calculado pelo Damodaran)
            query = """
            SELECT 
                industry,
                COUNT(*) as company_count,
                AVG(CASE WHEN beta IS NOT NULL AND beta != '' AND beta != 'None' 
                    THEN CAST(beta as REAL) ELSE NULL END) as avg_beta,
                SUM(CASE WHEN debt_equity IS NOT NULL AND market_cap IS NOT NULL AND market_cap > 0
                    THEN CAST(debt_equity as REAL) * CAST(market_cap as REAL) ELSE 0 END) /
                NULLIF(SUM(CASE WHEN debt_equity IS NOT NULL AND market_cap IS NOT NULL AND market_cap > 0
                    THEN CAST(market_cap as REAL) ELSE 0 END), 0) as avg_debt_equity,
                AVG(bottom_up_beta_for_sector) as unlevered_beta_damodaran
            FROM damodaran_global 
            WHERE industry IS NOT NULL
                AND industry != ''
                AND industry != 'None'
            GROUP BY industry
            HAVING COUNT(*) >= 3
                AND AVG(CASE WHEN beta IS NOT NULL AND beta != '' AND beta != 'None' 
                    THEN CAST(beta as REAL) ELSE NULL END) IS NOT NULL
            ORDER BY company_count DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            sectors = []
            for _, row in df.iterrows():
                avg_beta = round(float(row['avg_beta']), 3) if pd.notna(row['avg_beta']) else None
                sectors.append({
                    'sector': row['industry'],
                    'value': row['industry'],
                    'label': row['industry'],
                    'company_count': int(row['company_count']),
                    'companies_count': int(row['company_count']),
                    'avg_beta': avg_beta,
                    'avg_debt_equity': round(float(row['avg_debt_equity']), 3) if pd.notna(row['avg_debt_equity']) else None
                })
            
            result = {
                'success': True,
                'sectors': sectors,
                'total_sectors': len(sectors),
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
            self._sectors_cache = result
            logger.info(f"Carregados {len(sectors)} setores")
            return result
            
        except Exception as e:
            logger.error(f"Erro ao obter setores: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_sector_beta(self, sector: str, region: str = 'global') -> Dict[str, Any]:
        """
        Obter beta de um setor específico com metodologia Damodaran.
        
        Metodologia Damodaran (betaGlobal.xls):
        - Beta alavancado: média simples de TODAS empresas com beta válido (inclui negativos)
        - D/E: média ponderada por market_cap
        - Effective Tax Rate: ponderada por market_cap
        - Beta desalavancado: βU = βL / [1 + (1-T_eff) × D/E]
        - Cash/Firm Value: ponderada por market_cap (informativo)
        - bottom_up_beta_for_sector: βU corrigido por cash (informativo)
        
        Args:
            sector: Nome do setor
            region: Região (global, emkt)
        
        Returns:
            Dict com beta alavancado e desalavancado
        """
        try:
            conn = _connect_db(self.damodaran_db)
            
            # Query: pegar TODAS as empresas do setor (inclui betas negativos)
            base_query = """
            SELECT 
                CAST(beta as REAL) as beta,
                CAST(debt_equity as REAL) as debt_equity,
                CAST(market_cap as REAL) as market_cap,
                CAST(effective_tax_rate as REAL) as effective_tax_rate,
                CAST(cash_firm_value as REAL) as cash_firm_value,
                bottom_up_beta_for_sector,
                country,
                broad_group
            FROM damodaran_global 
            WHERE industry = ?
                AND beta IS NOT NULL 
                AND beta != ''
            """
            
            # Filtrar por região se necessário
            if region == 'emkt':
                query = base_query + " AND broad_group = 'Emerging Markets'"
            else:
                query = base_query
            
            df = pd.read_sql_query(query, conn, params=[sector])
            
            # Total de empresas no setor (para info)
            total_df = pd.read_sql_query(
                "SELECT COUNT(*) as n FROM damodaran_global WHERE industry = ?",
                conn, params=[sector]
            )
            total_companies = int(total_df['n'].values[0])
            
            conn.close()
            
            if df.empty:
                return {
                    'success': False,
                    'error': f'Setor "{sector}" não encontrado na região "{region}"'
                }
            
            # === METODOLOGIA DAMODARAN (betaGlobal.xls) ===
            
            # Beta alavancado: média simples INCLUINDO negativos (metodologia Damodaran)
            levered_beta = df['beta'].mean()
            
            # D/E: média ponderada por market_cap (metodologia Damodaran)
            mask_de = df['debt_equity'].notna() & df['market_cap'].notna() & (df['market_cap'] > 0)
            if mask_de.any():
                import numpy as np
                avg_debt_equity = np.average(df.loc[mask_de, 'debt_equity'], 
                                             weights=df.loc[mask_de, 'market_cap'])
            else:
                avg_debt_equity = df['debt_equity'].mean() if df['debt_equity'].notna().any() else 0.3
            
            # Effective tax rate: ponderada por market_cap
            import numpy as np
            mask_tax = df['effective_tax_rate'].notna() & df['market_cap'].notna() & (df['market_cap'] > 0)
            if mask_tax.any():
                effective_tax = np.average(df.loc[mask_tax, 'effective_tax_rate'],
                                           weights=df.loc[mask_tax, 'market_cap'])
            else:
                effective_tax = df['effective_tax_rate'].mean() if df['effective_tax_rate'].notna().any() else 0.20
            
            # Beta desalavancado: βU = βL / [1 + (1-T_eff) × D/E]
            # Este é o valor publicado no Excel do Damodaran (coluna "Unlevered beta")
            unlevered_beta = levered_beta / (1 + (1 - effective_tax) * avg_debt_equity)
            
            # bottom_up_beta_for_sector: βU corrigido por cash (informativo)
            bu_col = df['bottom_up_beta_for_sector'].dropna()
            bu_corrected_cash = float(bu_col.iloc[0]) if len(bu_col) > 0 else None
            
            # Cash/Firm value: ponderada por market_cap
            mask_cash = df['cash_firm_value'].notna() & df['market_cap'].notna() & (df['market_cap'] > 0)
            if mask_cash.any():
                cash_firm_value = np.average(df.loc[mask_cash, 'cash_firm_value'],
                                              weights=df.loc[mask_cash, 'market_cap'])
            else:
                cash_firm_value = df['cash_firm_value'].mean() if df['cash_firm_value'].notna().any() else 0.0
            
            return {
                'success': True,
                'sector': sector,
                'region': region,
                'levered_beta': round(levered_beta, 4),
                'unlevered_beta': round(unlevered_beta, 4),
                'unlevered_beta_corrected_cash': round(bu_corrected_cash, 4) if bu_corrected_cash else None,
                'avg_debt_equity': round(avg_debt_equity, 4),
                'debt_equity_ratio': round(avg_debt_equity, 4),
                'effective_tax_rate': round(effective_tax, 4),
                'cash_firm_value': round(cash_firm_value, 4),
                'companies_count': len(df),
                'company_count': len(df),
                'total_companies_sector': total_companies,
                'data_quality': 'high' if len(df) >= 30 else ('medium' if len(df) >= 10 else 'low'),
                'methodology': 'damodaran',
                'formula': 'βU = βL / [1 + (1-T_eff) × D/E]',
                'formula_detail': f'Beta: média simples (incl. negativos) | D/E: {avg_debt_equity*100:.2f}% pond. mktcap | T_eff: {effective_tax*100:.2f}%',
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter beta do setor: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_available_countries(self) -> Dict[str, Any]:
        """
        Obter países disponíveis para prêmio de risco.
        
        Returns:
            Dict com lista de países
        """
        if self._countries_cache is not None:
            return self._countries_cache
        
        try:
            conn = _connect_db(self.country_risk_db)
            
            query = """
            SELECT 
                country,
                CAST(risk_premium as REAL) as risk_premium
            FROM country_risk 
            WHERE risk_premium IS NOT NULL 
                AND risk_premium != ''
                AND CAST(risk_premium as REAL) >= 0
            ORDER BY country
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # Países principais para destaque
            principais_list = ['Brazil', 'United States', 'China', 'India', 'Japan',
                              'Germany', 'United Kingdom', 'France', 'Mexico', 'Argentina',
                              'Chile', 'Colombia', 'South Korea', 'Canada', 'Australia']
            
            principais = []
            outros = []
            for _, row in df.iterrows():
                premium_pct = round(float(row['risk_premium']) * 100, 2)
                entry = {
                    'country': row['country'],
                    'value': row['country'],
                    'label': row['country'],
                    'risk_premium': premium_pct,
                    'risk_premium_decimal': round(float(row['risk_premium']), 4),
                    'risk_premium_percentage': premium_pct
                }
                if row['country'] in principais_list:
                    principais.append(entry)
                else:
                    outros.append(entry)
            
            result = {
                'success': True,
                'countries': {
                    'principais': sorted(principais, key=lambda x: x['label']),
                    'outros': sorted(outros, key=lambda x: x['label'])
                },
                'total_countries': len(principais) + len(outros),
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
            self._countries_cache = result
            logger.info(f"Carregados {len(principais) + len(outros)} países")
            return result
            
        except Exception as e:
            logger.error(f"Erro ao obter países: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_country_risk(self, country: str) -> Dict[str, Any]:
        """
        Obter prêmio de risco de um país específico.
        
        Args:
            country: Nome do país
        
        Returns:
            Dict com prêmio de risco do país
        """
        try:
            conn = _connect_db(self.country_risk_db)
            
            query = """
            SELECT 
                country,
                CAST(risk_premium as REAL) as risk_premium,
                created_at
            FROM country_risk 
            WHERE country = ?
                AND risk_premium IS NOT NULL 
                AND risk_premium != ''
            """
            
            df = pd.read_sql_query(query, conn, params=[country])
            conn.close()
            
            if df.empty:
                return {
                    'success': False,
                    'error': f'País "{country}" não encontrado'
                }
            
            risk_premium = float(df.iloc[0]['risk_premium'])
            
            return {
                'success': True,
                'country': country,
                'risk_premium_decimal': round(risk_premium, 4),
                'risk_premium_percentage': round(risk_premium * 100, 2),
                'source': 'Damodaran Country Risk',
                'last_updated': df.iloc[0]['created_at'] if 'created_at' in df.columns else pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter risco do país: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_market_risk_premium(self) -> Dict[str, Any]:
        """
        Obter prêmio de risco de mercado (ERP).
        """
        try:
            wacc_data = self._load_wacc_components()
            mrp = wacc_data.get('RM', FALLBACK_RM)
            
            return {
                'success': True,
                'market_risk_premium_decimal': round(mrp / 100, 4),
                'market_risk_premium_percentage': round(mrp, 2),
                'source': 'BDWACC.json' if mrp != FALLBACK_RM else 'FALLBACK ABSURDO!',
                'is_fallback': mrp == FALLBACK_RM,
                'reference_year': wacc_data.get('RM_ano'),
                'methodology': 'Historical US Market Premium (Damodaran)',
                'last_updated': pd.Timestamp.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Erro ao obter prêmio de risco de mercado: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_wacc_components(self, sector: str, country: str = 'Brazil', region: str = 'global') -> Dict[str, Any]:
        """
        Obter todos os componentes WACC de uma vez.
        
        Args:
            sector: Setor da empresa
            country: País da empresa
            region: Região para beta (global, emkt)
        
        Returns:
            Dict com todos os componentes WACC
        """
        try:
            # Obter todos os componentes
            rf_data = self.get_risk_free_rate('10y')
            beta_data = self.get_sector_beta(sector, region)
            country_risk_data = self.get_country_risk(country)
            market_risk_data = self.get_market_risk_premium()
            
            # Carregar outros componentes do JSON
            wacc_data = self._load_wacc_components()
            
            # Verificar se todos os componentes foram obtidos com sucesso
            if not all([rf_data['success'], beta_data['success'], 
                       country_risk_data['success'], market_risk_data['success']]):
                errors = []
                if not rf_data['success']: errors.append(f"Taxa livre de risco: {rf_data.get('error', 'Erro desconhecido')}")
                if not beta_data['success']: errors.append(f"Beta: {beta_data.get('error', 'Erro desconhecido')}")
                if not country_risk_data['success']: errors.append(f"Risco país: {country_risk_data.get('error', 'Erro desconhecido')}")
                if not market_risk_data['success']: errors.append(f"Prêmio de mercado: {market_risk_data.get('error', 'Erro desconhecido')}")
                
                return {
                    'success': False,
                    'error': 'Erro ao obter componentes: ' + '; '.join(errors)
                }
            
            # Extrair valores
            risk_free_rate = rf_data['rate_percentage']
            beta = beta_data['unlevered_beta']  # Usar beta desalavancado
            market_risk_premium = market_risk_data['market_risk_premium_percentage']
            country_risk_premium = country_risk_data['risk_premium_percentage']
            
            # Outros componentes
            wacc_data = self._load_wacc_components()
            tax_rate = wacc_data.get('IR', FALLBACK_IR)
            
            # Selic: tentar BCB ao vivo, fallback para BDWACC.json
            selic_data = self.get_selic_live()
            cost_of_debt = selic_data['kd_percentage']
            cost_of_debt_source = selic_data['source']
            
            # Calcular custo do patrimônio
            # Ke = Rf + β × (Rm + Rp)
            cost_of_equity = risk_free_rate + beta * (market_risk_premium + country_risk_premium)
            
            # Para WACC simplificado (assumindo 100% patrimônio)
            wacc = cost_of_equity
            
            return {
                'success': True,
                'components': {
                    'risk_free_rate': {
                        'value_percentage': risk_free_rate,
                        'value_decimal': risk_free_rate / 100,
                        'source': 'FRED/Damodaran'
                    },
                    'beta': {
                        'levered_beta': beta_data['levered_beta'],
                        'unlevered_beta': beta,
                        'sector': sector,
                        'region': region,
                        'company_count': beta_data['company_count'],
                        'source': 'Damodaran Global'
                    },
                    'market_risk_premium': {
                        'value_percentage': market_risk_premium,
                        'value_decimal': market_risk_premium / 100,
                        'source': 'Damodaran ERP'
                    },
                    'country_risk_premium': {
                        'value_percentage': country_risk_premium,
                        'value_decimal': country_risk_premium / 100,
                        'country': country,
                        'source': 'Damodaran Country Risk'
                    },
                    'tax_rate': {
                        'value_percentage': tax_rate,
                        'value_decimal': tax_rate / 100,
                        'source': 'BDWACC.json' if tax_rate != FALLBACK_IR else 'FALLBACK ABSURDO!',
                        'is_fallback': tax_rate == FALLBACK_IR
                    },
                    'cost_of_debt': {
                        'value_percentage': cost_of_debt,
                        'value_decimal': cost_of_debt / 100,
                        'source': cost_of_debt_source,
                        'is_live': selic_data.get('is_live', False)
                    }
                },
                'calculated': {
                    'cost_of_equity_percentage': round(cost_of_equity, 2),
                    'cost_of_equity_decimal': round(cost_of_equity / 100, 4),
                    'wacc_percentage': round(wacc, 2),
                    'wacc_decimal': round(wacc / 100, 4),
                    'formula': 'Ke = Rf + β × (Rm + Rp)'
                },
                'metadata': {
                    'sector': sector,
                    'country': country,
                    'region': region,
                    'calculation_date': pd.Timestamp.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter componentes WACC: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_size_premium(self, market_cap: float = None) -> Dict[str, Any]:
        """
        Obter prêmio de tamanho baseado no valor de mercado da empresa.
        
        Args:
            market_cap: Valor de mercado da empresa em reais
        
        Returns:
            Dict com prêmio de tamanho aplicável
        """
        try:
            conn = _connect_db(self.damodaran_db)
            
            if market_cap is None:
                # Retornar todos os decis disponíveis
                query = """
                SELECT 
                    size_decile,
                    market_cap_min,
                    market_cap_max,
                    premium_decimal,
                    premium_percentage,
                    reference_year
                FROM size_premium 
                ORDER BY size_decile
                """
                
                df = pd.read_sql_query(query, conn)
                conn.close()
                
                if df.empty:
                    return {
                        'success': False,
                        'error': 'Dados de size premium não encontrados'
                    }
                
                size_data = []
                for _, row in df.iterrows():
                    size_data.append({
                        'decile': int(row['size_decile']),
                        'market_cap_min': float(row['market_cap_min']),
                        'market_cap_max': float(row['market_cap_max']),
                        'premium_decimal': float(row['premium_decimal']),
                        'premium_percentage': float(row['premium_decimal'] * 100),
                        'premium_display': row['premium_percentage']
                    })
                
                return {
                    'success': True,
                    'size_premiums': size_data,
                    'reference_year': int(df.iloc[0]['reference_year']),
                    'source': 'BDSize.json',
                    'total_deciles': len(size_data)
                }
            
            else:
                # Encontrar o decil apropriado para o valor de mercado fornecido
                query = """
                SELECT 
                    size_decile,
                    market_cap_min,
                    market_cap_max,
                    premium_decimal,
                    premium_percentage,
                    reference_year
                FROM size_premium 
                WHERE ? >= market_cap_min AND ? <= market_cap_max
                """
                
                df = pd.read_sql_query(query, conn, params=[market_cap, market_cap])
                conn.close()
                
                if df.empty:
                    return {
                        'success': False,
                        'error': f'Nenhum decil encontrado para valor de mercado: R$ {market_cap:,.0f}'
                    }
                
                row = df.iloc[0]
                
                return {
                    'success': True,
                    'market_cap': market_cap,
                    'size_decile': int(row['size_decile']),
                    'market_cap_range': {
                        'min': float(row['market_cap_min']),
                        'max': float(row['market_cap_max'])
                    },
                    'size_premium': {
                        'decimal': float(row['premium_decimal']),
                        'percentage': float(row['premium_decimal'] * 100),
                        'display': row['premium_percentage']
                    },
                    'reference_year': int(row['reference_year']),
                    'source': 'BDSize.json'
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter size premium: {e}")
            return {
                'success': False,
                'error': str(e)
            }