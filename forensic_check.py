import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from vnstock import Vnstock

# --- CONFIGURATION ---
TARGET_FILE = 'data/target_list_for_scrapers.csv'
CACHE_DIR = 'data/cache'
OUTPUT_FILE = 'data/target_list_with_forensics.csv'

# --- DATA HELPERS ---
def get_cached_data(symbol, report_type):
    """Retrieves cached data used by Piotroski score."""
    file_path = os.path.join(CACHE_DIR, f"{symbol}_{report_type}.json")
    if os.path.exists(file_path):
        try:
            return pd.read_json(file_path)
        except ValueError:
            return None
    # If not in cache, fetch it (Fallback)
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        if report_type == 'bs':
            df = stock.finance.balance_sheet(period='year', lang='en', dropna=True)
        elif report_type == 'is':
            df = stock.finance.income_statement(period='year', lang='en', dropna=True)
        else:
            return None
        return df
    except:
        return None

def get_val(df, idx, keywords):
    """Safely extracts a value from a dataframe column based on keywords."""
    if df is None or df.empty or idx >= len(df):
        return 0.0
    for col in df.columns:
        if any(k.lower() in col.lower() for k in keywords):
            try:
                # Handle cases where data might be a string or None
                val = df.iloc[idx][col]
                return float(val) if val is not None else 0.0
            except:
                return 0.0
    return 0.0

# --- BENEISH M-SCORE CALCULATOR ---
def calculate_m_score(symbol):
    bs = get_cached_data(symbol, 'bs')
    is_ = get_cached_data(symbol, 'is')
    
    # Needs at least 2 years of data
    if bs is None or is_ is None or len(bs) < 2 or len(is_) < 2:
        return np.nan

    # 1. DSRI: Days Sales in Receivables Index
    # (Are receivables growing faster than sales? = Channel Stuffing?)
    rec_cy = get_val(bs, 0, ['Receivables', 'Short-term receivables'])
    rec_py = get_val(bs, 1, ['Receivables', 'Short-term receivables'])
    rev_cy = get_val(is_, 0, ['Revenue', 'Net Revenue'])
    rev_py = get_val(is_, 1, ['Revenue', 'Net Revenue'])
    
    dsri = (rec_cy / rev_cy) / (rec_py / rev_py) if (rev_cy and rev_py and rec_py and rev_py != 0) else 1.0

    # 2. GMI: Gross Margin Index
    # (Is margin deteriorating? = Pressure to fake earnings?)
    cogs_cy = get_val(is_, 0, ['Cost of Goods Sold', 'Cost of Sales'])
    cogs_py = get_val(is_, 1, ['Cost of Goods Sold', 'Cost of Sales'])
    
    # Calculate Gross Profit
    gp_cy = rev_cy - abs(cogs_cy)
    gp_py = rev_py - abs(cogs_py)
    
    # Calculate Margins
    gm_cy = gp_cy / rev_cy if rev_cy else 0
    gm_py = gp_py / rev_py if rev_py else 0
    
    gmi = gm_py / gm_cy if gm_cy else 1.0

    # 3. AQI: Asset Quality Index
    # (Are they capitalizing costs to hide expenses?)
    ta_cy = get_val(bs, 0, ['Total Assets'])
    ta_py = get_val(bs, 1, ['Total Assets'])
    ca_cy = get_val(bs, 0, ['Current assets', 'Short-term assets'])
    ca_py = get_val(bs, 1, ['Current assets', 'Short-term assets'])
    ppe_cy = get_val(bs, 0, ['Fixed assets', 'Property, plant'])
    ppe_py = get_val(bs, 1, ['Fixed assets', 'Property, plant'])
    
    aq_cy = (1 - (ca_cy + ppe_cy) / ta_cy) if ta_cy else 0
    aq_py = (1 - (ca_py + ppe_py) / ta_py) if ta_py else 0
    
    aqi = aq_cy / aq_py if aq_py else 1.0

    # 4. SGI: Sales Growth Index
    # (Is growth impossibly high?)
    sgi = rev_cy / rev_py if rev_py else 1.0

    # 5. DEPI: Depreciation Index
    # (Did they slow down depreciation to boost income?)
    # We default to 1.0 as granular depreciation schedules are often missing in basic API feeds
    depi = 1.0 

    # 6. SGAI: Sales General & Admin Index
    # (Are overheads increasing disproportionately?)
    sga_cy = get_val(is_, 0, ['Selling expenses', 'Admin', 'Operating expenses'])
    sga_py = get_val(is_, 1, ['Selling expenses', 'Admin', 'Operating expenses'])
    
    sgai = (sga_cy / rev_cy) / (sga_py / rev_py) if (rev_cy and rev_py and sga_py) else 1.0

    # 7. LVGI: Leverage Index
    # (Is debt increasing to fund operations?)
    cl_cy = get_val(bs, 0, ['Current liabilities'])
    ltd_cy = get_val(bs, 0, ['Long-term liabilities', 'Non-current liabilities'])
    cl_py = get_val(bs, 1, ['Current liabilities'])
    ltd_py = get_val(bs, 1, ['Long-term liabilities', 'Non-current liabilities'])
    
    lev_cy = (cl_cy + ltd_cy) / ta_cy if ta_cy else 0
    lev_py = (cl_py + ltd_py) / ta_py if ta_py else 0
    
    lvgi = lev_cy / lev_py if lev_py else 1.0

    # 8. TATA: Total Accruals to Total Assets
    # (Is profit cash-based or accounting-magic based?)
    # This is a simplified proxy: (Net Income - Cash) / Assets
    net_income = get_val(is_, 0, ['Net Profit', 'Net Income'])
    cfo = get_val(bs, 0, ['Cash', 'Cash equivalents']) 
    
    tata = (net_income - cfo) / ta_cy if ta_cy else 0

    # --- FINAL FORMULA ---
    # Beneish M-Score Formula
    m_score = -4.84 + (0.92*dsri) + (0.528*gmi) + (0.404*aqi) + (0.892*sgi) + \
              (0.115*depi) - (0.172*sgai) + (4.679*tata) - (0.327*lvgi)
              
    return m_score

def run_forensic_check():
    print("--- 🕵️ FORENSIC CHECK (BENEISH M-SCORE) ---")
    
    if not os.path.exists(TARGET_FILE):
        print("Missing target list.")
        return

    df = pd.read_csv(TARGET_FILE)
    m_scores = []
    
    for ticker in tqdm(df['ticker'], desc="Auditing Books"):
        try:
            score = calculate_m_score(ticker)
            m_scores.append(score)
        except Exception as e:
            m_scores.append(np.nan)
            
    df['beneish_m_score'] = m_scores
    
    # RISK FLAG
    # M-Score > -2.22 suggests high risk of manipulation
    # (Note: -1.0 is GREATER than -2.22, so -1.0 is High Risk)
    
    df['accounting_risk'] = np.where(df['beneish_m_score'] > -2.22, 'HIGH RISK', 'SAFE')
    
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n[SUCCESS] Forensics complete. Saved to {OUTPUT_FILE}")
    
    risky = df[df['accounting_risk'] == 'HIGH RISK']
    if not risky.empty:
        print(f"\n⚠️ FOUND {len(risky)} POTENTIAL FRAUDS/MANIPULATORS:")
        print(risky[['ticker', 'beneish_m_score', 'pe']].head())
    else:
        print("\n✅ No high-risk accounting manipulation detected in the target list.")

if __name__ == "__main__":
    run_forensic_check()