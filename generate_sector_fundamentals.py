import pandas as pd
from vnstock import Screener
import numpy as np
import os

def generate_sector_fundamentals():
    print("--- CALCULATING SECTOR FUNDAMENTALS ---")
    
    # 1. Load the Master List
    try:
        master_df = pd.read_csv('data/company_master_list.csv')
    except FileNotFoundError:
        print("❌ Error: 'data/company_master_list.csv' not found.")
        print("Please run get_master_industry_list.py first.")
        return

    # 2. Fetch Market Data
    print("Fetching market data for all stocks...")
    screener = Screener()
    try:
        market_data = screener.stock(params={"exchangeName": "HOSE,HNX,UPCOM"}, limit=3000)
    except Exception as e:
        print(f"❌ Error fetching screener data: {e}")
        return
        
    print(f"Fetched raw market data for {len(market_data)} tickers.")
    
    # --- FIX STARTS HERE ---
    # 3. Clean Market Data before Merge
    # If the screener returns an 'industry' column, drop it so it doesn't conflict 
    # with the translated 'industry' column in our master_df
    if 'industry' in market_data.columns:
        market_data = market_data.drop(columns=['industry'])
        
    # Standardize ticker name just in case
    if 'symbol' in market_data.columns and 'ticker' not in market_data.columns:
        market_data = market_data.rename(columns={'symbol': 'ticker'})
        
    # 4. Merge
    # Now valid_sectors will definitely have the correct 'industry' column
    merged_df = pd.merge(market_data, master_df[['ticker', 'industry']], on='ticker', how='inner')
    
    print(f"Successfully matched {len(merged_df)} stocks to their industries.")
    
    # 5. Clean Data for Aggregation
    numeric_cols = ['pe', 'pb', 'roe', 'market_cap']
    available_cols = [c for c in numeric_cols if c in merged_df.columns]
    
    for col in available_cols:
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
    
    # Filter out garbage
    clean_df = merged_df[
        (merged_df['pe'] > 0) & 
        (merged_df['pe'] < 200) & 
        (merged_df['market_cap'] > 0)
    ].copy()
    
    # 6. Calculate Sector Metrics
    # Now this groupby will work because 'industry' is guaranteed to exist
    sector_stats = clean_df.groupby('industry').agg({
        'pe': 'median',
        'pb': 'median',
        'roe': 'median',
        'market_cap': 'sum',
        'ticker': 'count'
    }).rename(columns={
        'pe': 'sector_pe', 
        'pb': 'sector_pb', 
        'roe': 'sector_roe',
        'market_cap': 'total_sector_mcap',
        'ticker': 'stock_count'
    }).reset_index()
    
    # 7. Save
    if not os.path.exists('data'):
        os.makedirs('data')
        
    output_file = 'data/sector_fundamentals.csv'
    sector_stats.to_csv(output_file, index=False)
    
    print(f"\n✅ [SUCCESS] Sector analysis complete.")
    print(f"Saved sector metrics to {output_file}")
    
    print("\n--- 📊 Top 5 Cheapest Sectors (Median P/E) ---")
    print(sector_stats[['industry', 'sector_pe', 'stock_count']].sort_values('sector_pe').head(5))

if __name__ == "__main__":
    generate_sector_fundamentals()