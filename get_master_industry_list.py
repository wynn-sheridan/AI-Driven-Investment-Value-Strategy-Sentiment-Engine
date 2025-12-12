import pandas as pd
from vnstock import Listing
from deep_translator import GoogleTranslator
from tqdm import tqdm
import os
import time
import json

# Cache file to avoid getting banned by Google Translate
TRANSLATION_CACHE_FILE = 'data/industry_translation_cache.json'

def load_translation_cache():
    if os.path.exists(TRANSLATION_CACHE_FILE):
        with open(TRANSLATION_CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_translation_cache(cache):
    with open(TRANSLATION_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=4)

def create_master_ticker_file():
    print("Fetching master ticker list from vnstock...")
    
    try:
        listing = Listing()
        
        # 1. Get all symbols
        master_df = listing.symbols_by_industries()
        print(f"Raw data fetched. Found {len(master_df)} tickers.")
        
        # 2. Select & Rename columns
        target_cols = ['symbol', 'organ_name', 'icb_name3']
        master_df = master_df[target_cols].copy()
        master_df.columns = ['ticker', 'company_name', 'industry']
        
        # Drop rows where industry is missing
        master_df = master_df.dropna(subset=['industry'])
        
        # --- OPTIMIZED TRANSLATION LOGIC ---
        print("Translating industry names to English...")
        
        unique_industries = master_df['industry'].unique()
        translation_map = load_translation_cache()
        translator = GoogleTranslator(source='auto', target='en')
        
        new_translations_count = 0
        
        for ind in tqdm(unique_industries, desc="Processing Industries"):
            # Skip if already in cache
            if ind in translation_map:
                continue
                
            try:
                # Retry logic for translator
                for attempt in range(3):
                    try:
                        trans_text = translator.translate(ind)
                        translation_map[ind] = trans_text
                        new_translations_count += 1
                        time.sleep(0.5) # Gentle delay
                        break
                    except Exception as e:
                        if attempt == 2:
                            translation_map[ind] = ind # Fallback to original
                        time.sleep(2)
            except Exception:
                translation_map[ind] = ind

        # Save the updated cache
        if new_translations_count > 0:
            save_translation_cache(translation_map)
            print(f"Cached {new_translations_count} new translations.")
        
        # Map the English translations back
        master_df['industry'] = master_df['industry'].map(translation_map)
        
        # 3. Save to CSV
        if not os.path.exists('data'):
            os.makedirs('data')
            
        output_file = 'data/company_master_list.csv'
        master_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n--- SUCCESS ---")
        print(f"Saved {len(master_df)} companies to {output_file}")
        
    except Exception as e:
        print(f"Error fetching master list: {e}")

if __name__ == "__main__":
    create_master_ticker_file()