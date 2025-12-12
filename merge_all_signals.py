import pandas as pd
import numpy as np
import os
import sys

# --- CONFIGURATION ---
ALPHA_FILE = 'Final_Investment_Report.csv'       # From Step 5
TECH_FILE = 'data/final_target_list.csv'         # From Step 7 (Forensics + Technicals)
OUTPUT_FILE = 'MASTER_INVESTMENT_DASHBOARD.csv'  # The Final Result

def determine_final_action(row):
    """
    The Master Decision Matrix.
    Combines Value, Sentiment, Risk, and Timing into one instruction.
    """
    alpha = row['ALPHA_SCORE']
    risk = row['accounting_risk']
    tech = row['technical_signal']
    
    # 1. THE KILL SWITCH (Forensics)
    if risk == 'HIGH RISK':
        return "AVOID (ACCOUNTING RED FLAG)"
    
    # 2. LOW CONVICTION (Low Alpha)
    if alpha < 50:
        return "PASS (LOW CONVICTION)"
        
    # 3. HIGH CONVICTION CANDIDATES
    
    # Scenario A: Good Value, Bad Timing
    if "DOWNTREND" in tech or "Falling Knife" in tech:
        return "WATCHLIST (WAIT FOR UPTREND)"
        
    # Scenario B: The Golden Setup
    if "STRONG BUY" in tech:
        return "STRONG BUY (VALUE + DIP)"
    
    if "UPTREND" in tech:
        return "BUY (MOMENTUM + VALUE)"
        
    return "HOLD / NEUTRAL"

def merge_signals():
    print("--- 🧠 GENERATING MASTER INVESTMENT DASHBOARD ---")
    
    # 1. Load Files
    if not os.path.exists(ALPHA_FILE) or not os.path.exists(TECH_FILE):
        print("❌ Missing input files. Make sure you ran final_ranking.py AND technical_analysis.py")
        return

    alpha_df = pd.read_csv(ALPHA_FILE)
    tech_df = pd.read_csv(TECH_FILE)
    
    print(f"Loaded Alpha Report ({len(alpha_df)} stocks) and Technical/Risk Report ({len(tech_df)} stocks).")

    # 2. Merge
    # We strip tech_df down to just the columns we need to avoid duplicates
    cols_to_use = ['ticker', 'accounting_risk', 'beneish_m_score', 
                   'technical_signal', 'RSI_14', 'current_price', 'SMA_200']
    
    # Left merge onto Alpha (Rank) to preserve the order
    master_df = pd.merge(alpha_df, tech_df[cols_to_use], on='ticker', how='left')
    
    # 3. Apply Decision Logic
    print("Applying Decision Matrix...")
    master_df['FINAL_ACTION'] = master_df.apply(determine_final_action, axis=1)
    
    # 4. Clean and Sort
    # We want the "STRONG BUY" and "BUY" actions at the top, sorted by Alpha Score
    
    # Create a custom sort order for Action
    action_order = {
        "STRONG BUY (VALUE + DIP)": 0,
        "BUY (MOMENTUM + VALUE)": 1,
        "WATCHLIST (WAIT FOR UPTREND)": 2,
        "HOLD / NEUTRAL": 3,
        "PASS (LOW CONVICTION)": 4,
        "AVOID (ACCOUNTING RED FLAG)": 5
    }
    
    master_df['action_rank'] = master_df['FINAL_ACTION'].map(action_order)
    
    # Sort by Action Rank (Ascending) then Alpha Score (Descending)
    master_df = master_df.sort_values(by=['action_rank', 'ALPHA_SCORE'], ascending=[True, False])
    
    # Select Final Columns for the Dashboard
    final_cols = [
        'ticker', 'FINAL_ACTION', 'action_rank', 'ALPHA_SCORE', 'current_price', 
        'technical_signal', 'accounting_risk', 'RSI_14', 
        'pe', 'sector_pe', 'final_sentiment'
    ]
    
    master_df = master_df[final_cols]
    
    # 5. Save
    master_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ DASHBOARD GENERATED: {OUTPUT_FILE}")
    
    # 6. Display the Winners
    winners = master_df[master_df['action_rank'] <= 1]
    
    print("\n" + "="*60)
    print("       🚀 TOP HIGH-CONVICTION TRADES FOR TODAY       ")
    print("="*60)
    
    if not winners.empty:
        print(winners.to_string(index=False))
    else:
        print("No 'Buy' signals found today. Market conditions may be poor.")
        print("Check 'WATCHLIST' candidates in the CSV.")

    print("\n" + "="*60)
    print("       ⚠️ FRAUD RISK ALERTS (DO NOT BUY)       ")
    print("="*60)
    frauds = master_df[master_df['accounting_risk'] == 'HIGH RISK']
    if not frauds.empty:
        print(frauds[['ticker', 'ALPHA_SCORE', 'accounting_risk']].head().to_string(index=False))
    else:
        print("Clean books! No immediate fraud risks detected.")

if __name__ == "__main__":
    merge_signals()