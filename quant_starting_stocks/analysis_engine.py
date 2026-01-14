import pandas as pd
import numpy as np
import os
import time
from vnstock import Vnstock

# --- CACHE CONFIGURATION ---
CACHE_DIR = 'data/cache'
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

class AnalysisEngine:
    
    @staticmethod
    def rank_and_filter(df, top_n=50):
        """
        Takes a raw dataframe of stocks with P/E, P/B, ROE.
        Returns the top N candidates based on a composite ranking.
        """
        # 1. Clean Data
        cols = ['pe', 'pb', 'roe']
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 2. Basic Value Filter (Positive P/E and P/B)
        df_clean = df[(df['pe'] > 0) & (df['pb'] > 0)].copy()
        
        if df_clean.empty:
            return pd.DataFrame()

        # 3. Calculate Ranks
        # Lower P/E is better (Ascending)
        df_clean['pe_rank'] = df_clean['pe'].rank(ascending=True)
        # Lower P/B is better (Ascending)
        df_clean['pb_rank'] = df_clean['pb'].rank(ascending=True)
        # Higher ROE is better (Descending)
        df_clean['roe_rank'] = df_clean['roe'].rank(ascending=False)
        
        # 4. Composite Score (Lower is better)
        df_clean['composite_rank_score'] = df_clean['pe_rank'] + df_clean['pb_rank'] + df_clean['roe_rank']
        df_clean['initial_rank'] = df_clean['composite_rank_score'].rank(ascending=True)
        
        # 5. Return Top N
        return df_clean.sort_values('initial_rank').head(top_n).copy()

    @staticmethod
    def get_piotroski_score(symbol):
        """
        Calculates the Piotroski F-Score (0-9) for a given stock.
        Handles its own caching of financial reports.
        """
        # --- Internal Cache Helpers ---
        def _get_cached(sym, r_type):
            path = os.path.join(CACHE_DIR, f"{sym}_{r_type}.json")
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f: return pd.read_json(f)
                except ValueError: return None
            return None

        def _save_cache(sym, r_type, data):
            path = os.path.join(CACHE_DIR, f"{sym}_{r_type}.json")
            data.to_json(path)

        def _get_val(df, idx, keywords):
            for col in df.columns:
                if any(k.lower() in col.lower() for k in keywords):
                    return df.iloc[idx][col]
            return 0

        try:
            # 1. Load Data (Cache First, then API)
            bs = _get_cached(symbol, 'bs')
            is_ = _get_cached(symbol, 'is')
            cf = _get_cached(symbol, 'cf')
            
            if bs is None or is_ is None or cf is None:
                stock = Vnstock().stock(symbol=symbol, source='VCI')
                # Retry logic for fetching
                for attempt in range(3):
                    try:
                        if bs is None:
                            bs = stock.finance.balance_sheet(period='year', lang='en', dropna=True)
                            if bs is not None: _save_cache(symbol, 'bs', bs)
                        if is_ is None:
                            is_ = stock.finance.income_statement(period='year', lang='en', dropna=True)
                            if is_ is not None: _save_cache(symbol, 'is', is_)
                        if cf is None:
                            cf = stock.finance.cash_flow(period='year', dropna=True)
                            if cf is not None: _save_cache(symbol, 'cf', cf)
                        break
                    except Exception:
                        time.sleep(1)
            
            if bs is None or is_ is None or cf is None or len(bs) < 2:
                return 0 # Fail safe

            # 2. Calculate F-Score
            score = 0
            # Profitability
            net_income = _get_val(is_, 0, ['Net Profit', 'Net Income', 'Profit after tax'])
            avg_assets = (_get_val(bs, 0, ['Total Assets']) + _get_val(bs, 1, ['Total Assets'])) / 2
            roa = net_income / avg_assets if avg_assets else 0
            score += 1 if roa > 0 else 0
            
            cfo = _get_val(cf, 0, ['Net Cash Flows'])
            score += 1 if cfo > 0 else 0
            
            net_income_py = _get_val(is_, 1, ['Net Profit'])
            avg_assets_py = _get_val(bs, 1, ['Total Assets']) 
            roa_py = net_income_py / avg_assets_py if avg_assets_py else 0
            score += 1 if roa > roa_py else 0
            score += 1 if cfo > net_income else 0
            
            # Leverage/Liquidity
            lt_debt_cy = _get_val(bs, 0, ['Long-term', 'Non-current liabilities'])
            lt_debt_py = _get_val(bs, 1, ['Long-term', 'Non-current liabilities'])
            avg_assets_cy = (_get_val(bs, 0, ['Total Assets']) + _get_val(bs, 1, ['Total Assets'])) / 2
            avg_assets_py = _get_val(bs, 1, ['Total Assets'])
            
            lev_cy = lt_debt_cy / avg_assets_cy if avg_assets_cy else 0
            lev_py = lt_debt_py / avg_assets_py if avg_assets_py else 0
            score += 1 if lev_cy < lev_py else 0
            
            curr_assets_cy = _get_val(bs, 0, ['Current assets'])
            curr_liab_cy = _get_val(bs, 0, ['Current liabilities'])
            curr_assets_py = _get_val(bs, 1, ['Current assets'])
            curr_liab_py = _get_val(bs, 1, ['Current liabilities'])
            cr_cy = curr_assets_cy / curr_liab_cy if curr_liab_cy else 0
            cr_py = curr_assets_py / curr_liab_py if curr_liab_py else 0
            score += 1 if cr_cy > cr_py else 0
            
            shares_cy = _get_val(bs, 0, ['Share capital']) 
            shares_py = _get_val(bs, 1, ['Share capital'])
            score += 1 if shares_cy <= shares_py else 0
            
            # Efficiency
            rev_cy = _get_val(is_, 0, ['Revenue'])
            rev_py = _get_val(is_, 1, ['Revenue'])
            gm_cy = (rev_cy - abs(_get_val(is_, 0, ['Cost of Goods']))) / rev_cy if rev_cy else 0
            gm_py = (rev_py - abs(_get_val(is_, 1, ['Cost of Goods']))) / rev_py if rev_py else 0
            score += 1 if gm_cy > gm_py else 0
            
            at_cy = rev_cy / avg_assets_cy if avg_assets_cy else 0
            at_py = rev_py / avg_assets_py if avg_assets_py else 0
            score += 1 if at_cy > at_py else 0
            
            return score
        except Exception:
            return 0