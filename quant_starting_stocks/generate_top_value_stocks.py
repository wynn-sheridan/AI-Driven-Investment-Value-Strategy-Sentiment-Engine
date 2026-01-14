import pandas as pd
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import sys
import os
import numpy as np
from datetime import datetime

# --- IMPORTS ---
from data_adapter import DataProvider
from analysis_engine import AnalysisEngine
import warnings

# SILENCE PANDAS WARNINGS
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s', 
    handlers=[
        logging.FileHandler("extraction_debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- CONFIGURATION ---
BASE_FILE = 'data/market_fundamentals_base.csv'
BATCH_SIZE = 10 

# --- HELPER: WRAPPER FOR THREADING ---
def calculate_piotroski_parallel(ticker):
    # Now we just call the Engine
    score = AnalysisEngine.get_piotroski_score(ticker)
    return {'ticker': ticker, 'piotroski_f_score': score} 

def get_latest_deadline():
    """Returns the most recent official financial reporting deadline."""
    today = datetime.now()
    year = today.year
    deadlines = [
        datetime(year, 1, 30), datetime(year, 4, 30),
        datetime(year, 7, 30), datetime(year, 10, 30),
        datetime(year-1, 1, 30), datetime(year-1, 4, 30),
        datetime(year-1, 7, 30), datetime(year-1, 10, 30)
    ]
    past_deadlines = [d for d in deadlines if d < today]
    return max(past_deadlines) if past_deadlines else datetime(year-1, 10, 30)

def main():
    print("--- STARTING HYBRID PIPELINE (MODULAR) ---")
    start_time = time.time()
    
    # ---------------------------------------------------------
    # PHASE 1: THE "BASE" SCAN (Smart Expiration)
    # ---------------------------------------------------------
    base_df = pd.DataFrame()
    need_scan = True
    last_deadline = get_latest_deadline()
    
    if os.path.exists(BASE_FILE):
        file_date = datetime.fromtimestamp(os.path.getmtime(BASE_FILE))
        print(f"📅 Last Market Deadline: {last_deadline.strftime('%Y-%m-%d')}")
        
        if file_date < last_deadline:
            print(f"⚠️ FILE EXPIRED. Re-scanning for new season...")
            need_scan = True 
        else:
            print("✅ FILE VALID. Loading base data...")
            base_df = pd.read_csv(BASE_FILE, dtype={'ticker': str})
            
            all_tickers = DataProvider.get_all_tickers()
            existing_tickers = base_df['ticker'].tolist()
            tickers_to_scan = [t for t in all_tickers if t not in existing_tickers]
            
            if not tickers_to_scan:
                print("✅ Data complete.")
                need_scan = False
            else:
                print(f"⚠️ New IPOs detected. Scanning {len(tickers_to_scan)} new stocks...")
    else:
        print("🐢 BASE DATA MISSING: Starting Fresh Scan...")
        all_tickers = DataProvider.get_all_tickers()
        tickers_to_scan = all_tickers

    if need_scan:
        new_results = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_ticker = {
                executor.submit(DataProvider.fetch_single_stock_fundamentals, t): t 
                for t in tickers_to_scan
            }
            pbar = tqdm(as_completed(future_to_ticker), total=len(tickers_to_scan), desc="Fetching Fundamentals")
            
            for i, future in enumerate(pbar):
                res = future.result()
                if res: new_results.append(res)
                
                if len(new_results) >= BATCH_SIZE or (i == len(tickers_to_scan)-1 and new_results):
                    batch_df = pd.DataFrame(new_results)
                    hdr = not os.path.exists(BASE_FILE)
                    batch_df.to_csv(BASE_FILE, mode='a', header=hdr, index=False)
                    new_results = []
        
        print("\n📥 Reloading base data...")
        base_df = pd.read_csv(BASE_FILE, dtype={'ticker': str})

    if base_df.empty:
        print("CRITICAL: No base data available.")
        return

    # ---------------------------------------------------------
    # PHASE 2: THE "LIVE" UPDATE (Price)
    # ---------------------------------------------------------
    print(f"\n🚀 FETCHING LIVE PRICES for {len(base_df)} stocks...")
    tickers = base_df['ticker'].tolist()
    price_data = []
    
    for i in tqdm(range(0, len(tickers), 40), desc="Downloading Prices"):
        chunk = tickers[i:i+40]
        prices = DataProvider.fetch_live_price_batch(chunk)
        if prices: price_data.extend(prices)
        time.sleep(0.2) 

    price_df = pd.DataFrame(price_data)
    print(f"Got prices for {len(price_df)} stocks.")

    # ---------------------------------------------------------
    # PHASE 3: CALCULATE & FILTER (DELEGATED TO ENGINE)
    # ---------------------------------------------------------
    final_df = pd.merge(base_df, price_df, on='ticker', how='inner')
    final_df['pe'] = final_df['price'] / final_df['eps']
    final_df['pb'] = final_df['price'] / final_df['bvps']
    final_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    print(f"\nPhase 3: AnalysisEngine Filtering & Ranking...")
    
    # ONE LINE to handle all ranking logic!
    candidates = AnalysisEngine.rank_and_filter(final_df, top_n=50)
    
    target_tickers = candidates['ticker'].tolist()
    
    # ---------------------------------------------------------
    # PHASE 4: DEEP DIVE
    # ---------------------------------------------------------
    print(f"\nPhase 4: Deep Dive (Piotroski) on {len(target_tickers)} Candidates...")
    
    piotroski_results = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_ticker = {executor.submit(calculate_piotroski_parallel, t): t for t in target_tickers}
        for future in tqdm(as_completed(future_to_ticker), total=len(target_tickers), desc="Calculating F-Scores"):
            result = future.result()
            piotroski_results.append(result)

    scores_df = pd.DataFrame(piotroski_results)
    candidates = pd.merge(candidates, scores_df, on='ticker', how='left')
    
    high_quality_value = candidates[candidates['piotroski_f_score'] >= 5].copy()
    high_quality_value['final_rank'] = high_quality_value['composite_rank_score'].rank(ascending=True)
    
    output_file = 'data/top_quality_value_stocks.csv'
    candidates.to_csv(output_file, index=False)
    
    elapsed = time.time() - start_time
    print(f"\n✅ DONE! Saved results to {output_file}")
    print(f"⏱️ Session Runtime: {elapsed/60:.2f} minutes")

if __name__ == "__main__":
    main()