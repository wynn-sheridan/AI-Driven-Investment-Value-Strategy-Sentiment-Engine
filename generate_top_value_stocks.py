import pandas as pd
from vnstock import Screener, Vnstock
import time
from tqdm import tqdm
import numpy as np
import sys
import os
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed


  
# --- 1. SETUP LOGGING ---
# This will save all errors to a file so you don't miss them in the console scrolling!!
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extraction_debug.log"), # Saves to file
        logging.StreamHandler(sys.stdout)            # Prints to console
    ]
)

# Simple file-based cache
CACHE_DIR = 'data/cache'
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cached_data(symbol, report_type):
    file_path = os.path.join(CACHE_DIR, f"{symbol}_{report_type}.json")
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                return pd.read_json(f)
        except ValueError:
            logging.warning(f"[{symbol}] Cache corrupted for {report_type}")
            return None 
    return None

def save_to_cache(symbol, report_type, df):
    file_path = os.path.join(CACHE_DIR, f"{symbol}_{report_type}.json")
    df.to_json(file_path)

def get_piotroski_score(symbol):
    try:
        # Check cache first
        bs = get_cached_data(symbol, 'bs')
        is_ = get_cached_data(symbol, 'is')
        cf = get_cached_data(symbol, 'cf')
        
        # Flag to track if we needed to fetch data (for debugging)
        fetched_new_data = False
        
        if bs is None or is_ is None or cf is None:
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            fetched_new_data = True
            
            # --- RETRY LOGIC WITH JITTER ---
            max_retries = 4
            for attempt in range(max_retries):
                try:
                    if bs is None:
                        bs = stock.finance.balance_sheet(period='year', lang='en', dropna=True)
                        if bs is None or bs.empty: raise ValueError("Empty Balance Sheet returned")
                        save_to_cache(symbol, 'bs', bs)
                        
                    if is_ is None:
                        is_ = stock.finance.income_statement(period='year', lang='en', dropna=True)
                        if is_ is None or is_.empty: raise ValueError("Empty Income Statement returned")
                        save_to_cache(symbol, 'is', is_)
                        
                    if cf is None:
                        cf = stock.finance.cash_flow(period='year', dropna=True)
                        if cf is None or cf.empty: raise ValueError("Empty Cash Flow returned")
                        save_to_cache(symbol, 'cf', cf)
                    
                    # If we got here, success!
                    break
                except Exception as e:
                    err_msg = str(e)
                    if "Rate limit" in err_msg or "429" in err_msg:
                        wait = (5 * (attempt + 1)) + random.uniform(1, 5)
                        logging.warning(f"[{symbol}] Rate Limit Hit. Sleeping {wait:.1f}s (Attempt {attempt+1}/{max_retries})")
                        time.sleep(wait)
                    else:
                        logging.warning(f"[{symbol}] Error fetching data: {err_msg} (Attempt {attempt+1}/{max_retries})")
                        if attempt == max_retries - 1:
                            logging.error(f"[{symbol}] FAILED to fetch data after {max_retries} attempts.")
                            return np.nan
                        time.sleep(2)
        
        # --- DATA VALIDATION ---
        # Debugging: Print exactly what is missing
        missing_parts = []
        if bs is None or bs.empty: missing_parts.append("Balance Sheet")
        if is_ is None or is_.empty: missing_parts.append("Income Statement")
        if cf is None or cf.empty: missing_parts.append("Cash Flow")
        
        if missing_parts:
            logging.error(f"[{symbol}] Missing data parts: {', '.join(missing_parts)}")
            return np.nan

        if len(bs) < 2 or len(is_) < 2 or len(cf) < 2:
            logging.info(f"[{symbol}] Insufficient history (Needs 2 years). BS: {len(bs)}, IS: {len(is_)}, CF: {len(cf)}")
            return np.nan
            
        # Helper to get value safely (Unchanged)
        def get_val(df, idx, keywords):
            for col in df.columns:
                if any(k.lower() in col.lower() for k in keywords):
                    return df.iloc[idx][col]
            return 0

        # --- CALCULATION (Condensed for brevity, logic unchanged) ---
        net_income_cy = get_val(is_, 0, ['Net Profit', 'Net Income', 'Profit after tax'])
        avg_assets_cy = (get_val(bs, 0, ['Total Assets']) + get_val(bs, 1, ['Total Assets'])) / 2
        roa_cy = net_income_cy / avg_assets_cy if avg_assets_cy else 0
        score = 1 if roa_cy > 0 else 0
        
        cfo_cy = get_val(cf, 0, ['Net Cash Flows from Operating', 'Net cash inflows/outflows from operating'])
        score += 1 if cfo_cy > 0 else 0
        
        net_income_py = get_val(is_, 1, ['Net Profit', 'Net Income'])
        avg_assets_py = get_val(bs, 1, ['Total Assets']) 
        roa_py = net_income_py / avg_assets_py if avg_assets_py else 0
        score += 1 if roa_cy > roa_py else 0
        score += 1 if cfo_cy > net_income_cy else 0
        
        lt_debt_cy = get_val(bs, 0, ['Long-term liabilities', 'Non-current liabilities'])
        lt_debt_py = get_val(bs, 1, ['Long-term liabilities', 'Non-current liabilities'])
        lev_cy = lt_debt_cy / avg_assets_cy if avg_assets_cy else 0
        lev_py = lt_debt_py / avg_assets_py if avg_assets_py else 0
        score += 1 if lev_cy < lev_py else 0
        
        curr_assets_cy = get_val(bs, 0, ['Current assets', 'Short-term assets'])
        curr_liab_cy = get_val(bs, 0, ['Current liabilities', 'Short-term liabilities'])
        curr_assets_py = get_val(bs, 1, ['Current assets'])
        curr_liab_py = get_val(bs, 1, ['Current liabilities'])
        cr_cy = curr_assets_cy / curr_liab_cy if curr_liab_cy else 0
        cr_py = curr_assets_py / curr_liab_py if curr_liab_py else 0
        score += 1 if cr_cy > cr_py else 0
        
        shares_cy = get_val(bs, 0, ['Share capital', 'Paid-in capital', 'Charter capital']) 
        shares_py = get_val(bs, 1, ['Share capital', 'Paid-in capital'])
        score += 1 if shares_cy <= shares_py else 0
        
        rev_cy = get_val(is_, 0, ['Revenue', 'Net Revenue'])
        cogs_cy = get_val(is_, 0, ['Cost of Goods Sold', 'Cost of Sales'])
        gm_cy = (rev_cy - abs(cogs_cy)) / rev_cy if rev_cy else 0
        rev_py = get_val(is_, 1, ['Revenue', 'Net Revenue'])
        cogs_py = get_val(is_, 1, ['Cost of Goods Sold'])
        gm_py = (rev_py - abs(cogs_py)) / rev_py if rev_py else 0
        score += 1 if gm_cy > gm_py else 0
        
        at_cy = rev_cy / avg_assets_cy if avg_assets_cy else 0
        at_py = rev_py / avg_assets_py if avg_assets_py else 0
        score += 1 if at_cy > at_py else 0
        
        return score
        
    except Exception as e:
        logging.error(f"[{symbol}] Critical Calculation Error: {e}")
        return np.nan

def calculate_piotroski_parallel(ticker):
    """Wrapper for parallel execution."""
    time.sleep(random.uniform(0, 1.0)) 
    score = get_piotroski_score(ticker)
    return {'ticker': ticker, 'piotroski_f_score': score}

def main():
    print("Fetching all stocks from Screener...")
    screener = Screener()
    try:
        df = screener.stock(params={"exchangeName": "HOSE,HNX,UPCOM"}, limit=2000)
    except Exception as e:
        logging.error(f"Initial Screener Fetch Failed: {e}")
        return

    print(f"Fetched {len(df)} stocks.")
    
    # Filter for Value Stocks
    df_clean = df.dropna(subset=['pe', 'pb', 'roe', 'market_cap']).copy()
    df_clean = df_clean[(df_clean['pe'] > 0) & (df_clean['pb'] > 0)]
    
    # Calculate Initial Ranks
    df_clean['pe_rank'] = df_clean['pe'].rank(ascending=True)
    df_clean['pb_rank'] = df_clean['pb'].rank(ascending=True)
    df_clean['roe_rank'] = df_clean['roe'].rank(ascending=False)
    df_clean['composite_rank_score'] = df_clean['pe_rank'] + df_clean['pb_rank'] + df_clean['roe_rank']
    df_clean['initial_rank'] = df_clean['composite_rank_score'].rank(ascending=True)
    
    # Select Top 100 Candidates
    candidates = df_clean.sort_values('initial_rank').head(100).copy()
    tickers = candidates['ticker'].tolist()
    
    print(f"\n--- PROCESSING {len(tickers)} TICKERS ---")
    print(f"Logs are being written to 'extraction_debug.log'")
    
    piotroski_results = []
    
    # --- PARALLEL EXECUTION ---
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_ticker = {executor.submit(calculate_piotroski_parallel, t): t for t in tickers}
        
        # Use tqdm to show a progress bar
        for future in tqdm(as_completed(future_to_ticker), total=len(tickers), desc="Calculating Scores"):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                piotroski_results.append(result)
            except Exception as e:
                logging.error(f"[{ticker}] Thread crashed: {e}")
                piotroski_results.append({'ticker': ticker, 'piotroski_f_score': np.nan})

    # --- DEBUGGING: DATA AUDIT ---
    scores_df = pd.DataFrame(piotroski_results)
    
    # 1. Check for missing rows (should match len(tickers))
    if len(scores_df) != len(tickers):
        print(f"\nCRITICAL WARNING: Input {len(tickers)} tickers, but got results for {len(scores_df)}")
    
    # 2. Merge back
    candidates = pd.merge(candidates, scores_df, on='ticker', how='left')
    
    # 3. Identify Failures
    failed_stocks = candidates[candidates['piotroski_f_score'].isna()]
    success_stocks = candidates[~candidates['piotroski_f_score'].isna()]
    
    print("\n" + "="*40)
    print("       DATA INTEGRITY REPORT       ")
    print("="*40)
    print(f"Total Processed:   {len(candidates)}")
    print(f"Successful Scores: {len(success_stocks)}")
    print(f"Failed/Missing:    {len(failed_stocks)}")
    print("-" * 40)
    
    if not failed_stocks.empty:
        print("MISSING DATA FOR TICKERS:")
        print(failed_stocks['ticker'].tolist())
        print("Check 'extraction_debug.log' for details on why these failed.")
    else:
        print("PERFECT RUN! No missing data.")
    print("="*40 + "\n")

    # --- QUALITY FILTER & SAVE ---
    high_quality_value = candidates[candidates['piotroski_f_score'] >= 5].copy()
    
    high_quality_value['final_rank'] = high_quality_value['composite_rank_score'].rank(ascending=True)
    high_quality_value['sector_rank'] = high_quality_value.groupby('industry')['composite_rank_score'].rank(ascending=True)
    
    final_df = high_quality_value.sort_values('final_rank').copy()
    
    final_cols = ['ticker', 'exchange', 'industry', 'market_cap', 'pe', 'pb', 'roe', 'ev_ebitda', 'piotroski_f_score', 'sector_rank', 'final_rank']
    
    for col in final_cols:
        if col not in final_df.columns:
            final_df[col] = np.nan
        
    final_df = final_df[final_cols]
    
    output_file = 'data/top_quality_value_stocks.csv'
    if not os.path.exists('data'):
        os.makedirs('data')
        
    final_df.to_csv(output_file, index=False)
    print(f"Saved {len(final_df)} stocks to {output_file}")

if __name__ == "__main__":
    main()