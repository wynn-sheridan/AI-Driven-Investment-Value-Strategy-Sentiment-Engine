# import requests
# from bs4 import BeautifulSoup
# import time
# import pandas as pd
# import os
# from tqdm import tqdm 

# def scrape_f319_raw():
#     """
#     Scrapes 150 pages of F319's 'Main Stock Market Forum'.
#     This avoids the 5-page limit of the 'New Posts' search.
#     """
#     print("--- SCRAPING F319: EXTRACTING RAW VIETNAMESE HEADERS ---")
    
#     pages_to_scan = 150 
#     all_data = []
    
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#         'Referer': 'https://f319.com/'
#     }

#     # Loop through pages 1 to 150
#     for page in tqdm(range(1, pages_to_scan + 1), desc="Scraping F319 Pages"):
        
#         # URL for the Main Stock Market Box
#         url = f"https://f319.com/forums/thi-truong-chung-khoan.3/page-{page}"
        
#         try:
#             response = requests.get(url, headers=headers, timeout=10)
            
#             if response.status_code != 200:
#                 tqdm.write(f"  [ERROR] Page {page} failed. Status: {response.status_code}. Stopping.")
#                 break

#             soup = BeautifulSoup(response.text, 'html.parser')
            
#             # On the main forum list, titles are usually in 'h3.title' or 'a.PreviewTooltip'
#             titles = soup.find_all('h3', class_='title')
#             if not titles:
#                 titles = soup.find_all('a', class_='PreviewTooltip')
            
#             if titles:
#                 for title_tag in titles:
#                     link = title_tag.find('a') if title_tag.name == 'h3' else title_tag
#                     if link:
#                         all_data.append({
#                             'original_title': link.get_text(strip=True),
#                             'page': page,
#                             'source': 'F319_FORUM'
#                         })
#             else:
#                 tqdm.write(f"  [INFO] No threads found on page {page}. Stopping scan.")
#                 break
            
#             time.sleep(1.0) # Be polite!

#         except Exception as e:
#             tqdm.write(f"  [Error] Page {page}: {e}")
#             continue

#     # SAVE TO CSV
#     if all_data:
#         df = pd.DataFrame(all_data)
#         output_file = 'f319_raw_titles.csv'
        
#         if not os.path.exists('data'):
#             os.makedirs('data')
            
#         # Drop duplicates (sticky threads appear on every page)
#         df = df.drop_duplicates(subset=['original_title'])
            
#         df.to_csv(os.path.join('data', output_file), index=False, encoding='utf-8-sig')
        
#         print(f"\n[SUCCESS] Saved {len(df)} unique raw titles to {output_file}")
        
#     else:
#         print("\n[FAIL] No data was collected.")

# if __name__ == "__main__":
#     scrape_f319_raw()

import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import os
from tqdm import tqdm
import re  # <--- NEW: Needed for accurate word matching

# --- CONFIGURATION ---
TARGET_LIST_FILE = 'data/target_list_for_scrapers.csv'
OUTPUT_FILE = 'data/f319_smart_filtered.csv' # Changed name to reflect filtered status

def load_targets():
    """Loads the Top 50 tickers from the previous step."""
    try:
        if not os.path.exists(TARGET_LIST_FILE):
            print(f"⚠️ Warning: {TARGET_LIST_FILE} not found. Running in 'Capture All' mode.")
            return []
            
        df = pd.read_csv(TARGET_LIST_FILE)
        # Convert to a list of uppercase strings
        targets = df['ticker'].astype(str).str.upper().unique().tolist()
        print(f"🎯 Sniper Mode Activated: Tracking {len(targets)} tickers.")
        return targets
    except Exception as e:
        print(f"Error loading targets: {e}")
        return []

def scrape_f319_smart():
    """
    Scrapes F319 but ONLY keeps threads related to our Target List.
    """
    print("--- SCRAPING F319: EXTRACTING SMART SIGNALS ---")
    
    # 1. Load the Hit List
    target_tickers = load_targets()
    
    pages_to_scan = 150 
    all_data = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://f319.com/'
    }

    # Loop through pages 1 to 150
    for page in tqdm(range(1, pages_to_scan + 1), desc="Scanning F319 Pages"):
        
        url = f"https://f319.com/forums/thi-truong-chung-khoan.3/page-{page}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                tqdm.write(f"  [ERROR] Page {page} failed. Status: {response.status_code}. Stopping.")
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find titles
            titles = soup.find_all('h3', class_='title')
            if not titles:
                titles = soup.find_all('a', class_='PreviewTooltip')
            
            if titles:
                for title_tag in titles:
                    link = title_tag.find('a') if title_tag.name == 'h3' else title_tag
                    if link:
                        raw_title = link.get_text(strip=True)
                        title_upper = raw_title.upper()
                        
                        # --- THE SNIPER FILTER ---
                        matched_ticker = None
                        
                        if target_tickers:
                            # Check if any of our 50 tickers appear in this title
                            for ticker in target_tickers:
                                # Regex \b means "Word Boundary". 
                                # It ensures "VIX" matches "VIX" but NOT "VIXION"
                                if re.search(r'\b' + re.escape(ticker) + r'\b', title_upper):
                                    matched_ticker = ticker
                                    break # Found a match, stop checking other tickers
                            
                            # If we found a match (or if we have no targets), keep it
                            if matched_ticker:
                                all_data.append({
                                    'ticker': matched_ticker, # Store which ticker matched
                                    'original_title': raw_title,
                                    'page': page,
                                    'source': 'F319_FORUM'
                                })
                        else:
                            # Fallback: If no target file exists, keep everything (Original behavior)
                            all_data.append({
                                'ticker': 'UNKNOWN',
                                'original_title': raw_title,
                                'page': page,
                                'source': 'F319_FORUM'
                            })
                            
            else:
                tqdm.write(f"  [INFO] No threads found on page {page}. Stopping scan.")
                break
            
            time.sleep(1.0) 

        except Exception as e:
            tqdm.write(f"  [Error] Page {page}: {e}")
            continue

    # SAVE TO CSV
    if all_data:
        df = pd.DataFrame(all_data)
        
        if not os.path.exists('data'):
            os.makedirs('data')
            
        # Drop duplicates (sticky threads appear on every page)
        df = df.drop_duplicates(subset=['original_title'])
            
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
        print(f"\n[SUCCESS] Found {len(df)} relevant discussions.")
        print(f"Saved to {OUTPUT_FILE}")
        
        # Quick Stat
        if 'ticker' in df.columns and target_tickers:
            print("\n--- Top Buzzing Stocks ---")
            print(df['ticker'].value_counts().head(5))
        
    else:
        print("\n[INFO] No relevant discussions found for your target list.")

if __name__ == "__main__":
    scrape_f319_smart()