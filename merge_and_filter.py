import pandas as pd
import os

# --- CONFIGURATION ---
TOP_N = 50            # We will select the top 50 from your 73 survivors
MAX_SECTOR_PE = 25.0  # Avoid sectors that are in a bubble
MIN_SECTOR_ROE = 5.0  # Avoid dead sectors

def merge_and_filter_targets():
    print(f"--- MERGING DATA FOR TOP {TOP_N} TARGETS ---")
    
    # 1. Load Data
    try:
        stocks_df = pd.read_csv('data/top_quality_value_stocks.csv')
        sector_df = pd.read_csv('data/sector_fundamentals.csv')
        master_df = pd.read_csv('data/company_master_list.csv')
    except FileNotFoundError as e:
        print(f"❌ Missing file: {e}")
        return

    print(f"Loaded {len(stocks_df)} stock candidates (from your 73) and {len(sector_df)} sector profiles.")

    # 2. Merge Sector Data into Stock Data
    # Drop industry from stocks_df if it exists to ensure we use the Master List version
    if 'industry' in stocks_df.columns:
        stocks_df = stocks_df.drop(columns=['industry'])
        
    # Stock -> Master (get Industry) -> Sector Metrics
    merged = pd.merge(stocks_df, master_df[['ticker', 'industry']], on='ticker', how='left')
    final_df = pd.merge(merged, sector_df, on='industry', how='left')
    
    # 3. Apply Filters
    print("Applying Smart Filters...")
    
    # Filter A: Valid Sectors (Remove Bubble or Dead sectors)
    valid_sectors = final_df[
        (final_df['sector_pe'] < MAX_SECTOR_PE) & 
        (final_df['sector_roe'] > MIN_SECTOR_ROE)
    ].copy()
    
    # Filter B: Discount Check (Is Stock P/E < Sector P/E?)
    valid_sectors['pe_discount'] = (valid_sectors['pe'] - valid_sectors['sector_pe']) / valid_sectors['sector_pe']
    
    # 4. Final Scoring
    valid_sectors['score_piotroski'] = valid_sectors['piotroski_f_score'] / 9.0
    valid_sectors['score_discount'] = valid_sectors['pe_discount'] * -1.0 # Lower discount (negative) is better
    valid_sectors['score_sector'] = valid_sectors['sector_roe'] / 20.0 
    
    valid_sectors['final_conviction_score'] = (
        (valid_sectors['score_piotroski'] * 0.4) + 
        (valid_sectors['score_discount'] * 0.4) + 
        (valid_sectors['score_sector'] * 0.2)
    )
    
    # Sort
    top_targets = valid_sectors.sort_values('final_conviction_score', ascending=False).head(TOP_N)
    
    # 5. Save
    output_cols = ['ticker', 'industry', 'pe', 'sector_pe', 'piotroski_f_score', 'final_conviction_score']
    
    output_file = 'data/target_list_for_scrapers.csv'
    top_targets[output_cols].to_csv(output_file, index=False)
    
    print(f"\n✅ [SUCCESS] Generated target list of {len(top_targets)} stocks.")
    print(f"Saved to {output_file}")
    print("\n--- TOP 5 TARGETS ---")
    print(top_targets[output_cols].head(5))

if __name__ == "__main__":
    merge_and_filter_targets()