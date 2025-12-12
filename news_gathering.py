# import pandas as pd
# import requests
# import os
# import time
# from tqdm import tqdm
# import warnings
# from urllib3.exceptions import InsecureRequestWarning
# from bs4 import BeautifulSoup
# from datetime import datetime, timedelta
# from vnstock import Company 
# import random 
# import concurrent.futures

# # --- 1. SETUP ---
# warnings.simplefilter('ignore', InsecureRequestWarning)
# http_headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari=537.36',
#     'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' 
# }

# # --- 2. HELPER: SMART DATE CONVERSION (THE FIX) ---
# def clean_date(raw_date):
#     """Converts timestamp (seconds OR ms) or string to DD/MM/YYYY"""
#     if pd.isna(raw_date):
#         return 'N/A'
    
#     try:
#         str_date = str(raw_date)
#         # Check if it's a timestamp (digits)
#         if str_date.isdigit():
#             ts = int(str_date)
            
#             # SMART CHECK: 
#             # If the number is huge (> 1 trillion), it's Milliseconds (Vnstock).
#             # If it's smaller (10 digits), it's Seconds (HSX).
#             if ts > 1000000000000: 
#                 ts = ts / 1000  # Convert ms to seconds
            
#             dt_obj = datetime.fromtimestamp(ts)
#             return dt_obj.strftime('%d/%m/%Y')
            
#         # If it's already a string, return it
#         return str_date
#     except Exception:
#         return str(raw_date)

# # --- 3. SCRAPER FUNCTIONS ---

# def fetch_hsx_page(session, page, start_date_str, end_date_str):
#     """Helper function to fetch a single page safely."""
#     try:
#         # SAFETY: Random sleep to prevent exact simultaneous hits
#         time.sleep(random.uniform(0.5, 1.5))
        
#         api_url = f"https://api.hsx.vn/n/api/v1/1/news/securitiesType/1?pageIndex={page}&pageSize=50&startDate={start_date_str}&endDate={end_date_str}"
#         response = session.get(api_url, headers=http_headers, verify=False, timeout=30)
        
#         if response.status_code == 200:
#             data = response.json()
#             return data.get('data', {}).get('list', [])
#     except Exception:
#         pass
#     return []

# def get_hsx_news_general(target_tickers):
#     """
#     Fetches HOSE news using GENTLE PARALLEL PROCESSING (3 Workers).
#     """
#     filtered_articles = []
#     print(f"Fetching general HOSE news stream (Safe Parallel Scan)...")
    
#     # --- CHANGE 1: Start Timer & Counter ---
#     start_time = time.time()
#     total_found = 0
    
#     end_date = datetime.now()
#     start_date = end_date - timedelta(days=90)
#     start_str = start_date.strftime('%Y-%m-%d')
#     end_str = end_date.strftime('%Y-%m-%d')
    
#     with requests.Session() as session:
#         # REDUCED WORKERS TO 3 (Safe for home IP)
#         with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            
#             future_to_page = {
#                 executor.submit(fetch_hsx_page, session, page, start_str, end_str): page 
#                 for page in range(1, 101)
#             }
            
#             # --- CHANGE 2: Assign tqdm to 'pbar' variable ---
#             pbar = tqdm(concurrent.futures.as_completed(future_to_page), total=100, desc="Scanning HSX Pages")
            
#             for future in pbar:
#                 page_articles = future.result()
            
#             # for future in tqdm(concurrent.futures.as_completed(future_to_page), total=100, desc="Scanning HSX Pages"):
#             #     page_articles = future.result()
                
#                 if not page_articles: continue
                    
#                 for article in page_articles:
#                     original_title = article.get('title', '')
#                     extracted_ticker = "UNKNOWN"
#                     if ':' in original_title:
#                         extracted_ticker = original_title.split(':')[0].strip()
                    
#                     if extracted_ticker in target_tickers:
#                         filtered_articles.append({
#                             'ticker': extracted_ticker,
#                             'date': clean_date(article.get('postedDate')),
#                             'news_title': original_title,
#                             'source': 'HOSE_API'
#                         })
                        
#                         # --- CHANGE 3: Increment Count & Update Bar ---
#                         total_found += 1
#                         pbar.set_description(f"Scanning HSX Pages (Found {total_found} relevant)")

#     # --- CHANGE 4: Stop Timer & Print ---
#     end_time = time.time()
#     duration = end_time - start_time

#     print(f"  [HSX] Finished in {duration:.2f} seconds.")
#     print(f"  [HSX] Total relevant articles found: {len(filtered_articles)}")
#     return filtered_articles

# def get_cafef_news(ticker):
#     """Scrapes HNX/UPCoM news from CafeF Ajax API."""
#     all_articles = []
#     try:
#         api_url = f"https://cafef.vn/du-lieu//Ajax/Events_RelatedNews_New.aspx?symbol={ticker}&floorID=0&configID=0&PageIndex=1&PageSize=10&Type=2"
#         response = requests.get(api_url, headers=http_headers, verify=False, timeout=10)
#         response.raise_for_status()
        
#         soup = BeautifulSoup(response.text, 'lxml')
#         news_list_container = soup.find('ul', class_='News_Title_Link')
        
#         if news_list_container:
#             news_items = news_list_container.find_all('li')
#             for item in news_items:
#                 date_tag = item.find('span', class_='timeTitle')
#                 link = item.find('a', class_='docnhanhTitle')
#                 if link and date_tag:
#                     all_articles.append({
#                         'ticker': ticker,
#                         'date': date_tag.get_text(strip=True),
#                         'news_title': link.get_text(strip=True),
#                         'source': 'CAFEF_AJAX'
#                     })
#         time.sleep(0.5)
#     except Exception:
#         pass 
#     return all_articles

# def get_vnstock_news(ticker):
#     """Scrapes news using the vnstock library."""
#     all_articles = []
#     try:
#         company_news = Company(symbol=ticker)
#         news_df = company_news.news(page=1, page_size=10) 

#         if not news_df.empty:
#             for index, row in news_df.iterrows():
                
#                 formatted_date = clean_date(row.get('public_date'))
                
#                 # VNSTOCK returns English keys now
#                 original_title = row.get('news_title', row.get('title', 'N/A'))
                
#                 all_articles.append({
#                     'ticker': ticker,
#                     'date': formatted_date, 
#                     'news_title': original_title,
#                     'source': 'VNSTOCK_TCBS'
#                 })
#     except Exception:
#         pass 
#     return all_articles

# def run_data_gathering():
#     INPUT_FILE = 'top_value_stocks.csv'
#     OUTPUT_FILE = 'data/raw_news_data.csv'

#     try:
#         top_50_df = pd.read_csv(INPUT_FILE)
        
#         # Identify correct columns
#         ticker_col = 'ticker' if 'ticker' in top_50_df.columns else 'symbol'
#         exchange_col = 'exchange' if 'exchange' in top_50_df.columns else 'comGroupCode'
        
#         # Get list of all tickers for filtering HSX
#         all_target_tickers = top_50_df[ticker_col].tolist()

#         print(f"Loaded {len(top_50_df)} tickers. Starting news scrape...")
        
#         master_news_list = []

#         # --- 1. GET HOSE NEWS (Bulk Fetch) ---
#         # Only run this ONCE to get all HOSE news efficiently
#         master_news_list.extend(get_hsx_news_general(all_target_tickers))
        
#         # --- 2. LOOP FOR CAFEF & VNSTOCK ---
#         print("Starting ticker-specific scrape (CafeF & Vnstock)...")
        
#         # Counters for debugging
#         cafef_count = 0
#         vnstock_count = 0
        
#         for index, row in tqdm(top_50_df.iterrows(), total=top_50_df.shape[0]):
#             ticker = row[ticker_col]
#             exchange = row[exchange_col]
            
#             # CAFEF (Only non-HOSE)
#             if exchange in ['HNX', 'UPCOM']:
#                 new_cafef = get_cafef_news(ticker)
#                 master_news_list.extend(new_cafef)
#                 cafef_count += len(new_cafef)
            
#             # VNSTOCK (All)
#             new_vnstock = get_vnstock_news(ticker)
#             master_news_list.extend(new_vnstock)
#             vnstock_count += len(new_vnstock)
            
#             time.sleep(1.0) 
            
#         print(f"\n[SUMMARY] CafeF Articles: {cafef_count} | Vnstock Articles: {vnstock_count}")

#         if not master_news_list:
#             print("\nFATAL: No news was found.")
#             return

#         # Clean up duplicates
#         news_df = pd.DataFrame(master_news_list)
#         before_dedupe = len(news_df)
#         news_df = news_df.drop_duplicates(subset=['ticker', 'news_title'])
#         after_dedupe = len(news_df)
        
#         if not os.path.exists('data'):
#             os.makedirs('data')
            
#         news_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
#         print(f"\n--- SUCCESS ---")
#         print(f"Dropped {before_dedupe - after_dedupe} duplicates.")
#         print(f"Saved {after_dedupe} unique news items to {OUTPUT_FILE}")

#     except FileNotFoundError:
#         print(f"ERROR: Could not find {INPUT_FILE}.")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     run_data_gathering()

import pandas as pd
import requests
import os
import time
from tqdm import tqdm
import warnings
from urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from vnstock import Company 
import random 
import concurrent.futures

# --- 1. SETUP ---
warnings.simplefilter('ignore', InsecureRequestWarning)
http_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari=537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' 
}

# --- 2. HELPER: SMART DATE CONVERSION ---
def clean_date(raw_date):
    """Converts timestamp (seconds OR ms) or string to DD/MM/YYYY"""
    if pd.isna(raw_date):
        return 'N/A'
    
    try:
        str_date = str(raw_date)
        if str_date.isdigit():
            ts = int(str_date)
            # If > 1 trillion, it's ms. Else seconds.
            if ts > 1000000000000: 
                ts = ts / 1000 
            
            dt_obj = datetime.fromtimestamp(ts)
            return dt_obj.strftime('%d/%m/%Y')
        return str_date
    except Exception:
        return str(raw_date)

# --- 3. SCRAPER FUNCTIONS ---

def fetch_hsx_page(session, page, start_date_str, end_date_str):
    """Helper function to fetch a single page safely."""
    try:
        time.sleep(random.uniform(0.5, 1.5))
        api_url = f"https://api.hsx.vn/n/api/v1/1/news/securitiesType/1?pageIndex={page}&pageSize=50&startDate={start_date_str}&endDate={end_date_str}"
        response = session.get(api_url, headers=http_headers, verify=False, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('data', {}).get('list', [])
    except Exception:
        pass
    return []

def get_hsx_news_general(target_tickers):
    """Fetches HOSE news using GENTLE PARALLEL PROCESSING."""
    filtered_articles = []
    print(f"Fetching general HOSE news stream (Safe Parallel Scan)...")
    
    start_time = time.time()
    total_found = 0
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Create a SET for faster lookup
    target_set = set(target_tickers)

    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_page = {
                executor.submit(fetch_hsx_page, session, page, start_str, end_str): page 
                for page in range(1, 101)
            }
            
            pbar = tqdm(concurrent.futures.as_completed(future_to_page), total=100, desc="Scanning HSX Pages")
            
            for future in pbar:
                page_articles = future.result()
                if not page_articles: continue
                    
                for article in page_articles:
                    original_title = article.get('title', '')
                    extracted_ticker = "UNKNOWN"
                    if ':' in original_title:
                        extracted_ticker = original_title.split(':')[0].strip()
                    
                    if extracted_ticker in target_set:
                        filtered_articles.append({
                            'ticker': extracted_ticker,
                            'date': clean_date(article.get('postedDate')),
                            'news_title': original_title,
                            'source': 'HOSE_API'
                        })
                        total_found += 1
                        pbar.set_description(f"Scanning HSX Pages (Found {total_found} relevant)")

    end_time = time.time()
    duration = end_time - start_time
    print(f"  [HSX] Finished in {duration:.2f} seconds.")
    print(f"  [HSX] Total relevant articles found: {len(filtered_articles)}")
    return filtered_articles

def get_cafef_news(ticker):
    """Scrapes HNX/UPCoM news from CafeF Ajax API."""
    all_articles = []
    try:
        api_url = f"https://cafef.vn/du-lieu//Ajax/Events_RelatedNews_New.aspx?symbol={ticker}&floorID=0&configID=0&PageIndex=1&PageSize=10&Type=2"
        response = requests.get(api_url, headers=http_headers, verify=False, timeout=10)
        
        soup = BeautifulSoup(response.text, 'lxml')
        news_list_container = soup.find('ul', class_='News_Title_Link')
        
        if news_list_container:
            news_items = news_list_container.find_all('li')
            for item in news_items:
                date_tag = item.find('span', class_='timeTitle')
                link = item.find('a', class_='docnhanhTitle')
                if link and date_tag:
                    all_articles.append({
                        'ticker': ticker,
                        'date': date_tag.get_text(strip=True),
                        'news_title': link.get_text(strip=True),
                        'source': 'CAFEF_AJAX'
                    })
        time.sleep(0.5)
    except Exception:
        pass 
    return all_articles

def get_vnstock_news(ticker):
    """Scrapes news using the vnstock library."""
    all_articles = []
    try:
        company_news = Company(symbol=ticker)
        news_df = company_news.news(page=1, page_size=10) 

        if not news_df.empty:
            for index, row in news_df.iterrows():
                formatted_date = clean_date(row.get('public_date'))
                original_title = row.get('news_title', row.get('title', 'N/A'))
                all_articles.append({
                    'ticker': ticker,
                    'date': formatted_date, 
                    'news_title': original_title,
                    'source': 'VNSTOCK_TCBS'
                })
    except Exception:
        pass 
    return all_articles

def run_data_gathering():
    # --- CHANGED: Point to the new Target List ---
    TARGET_FILE = 'data/target_list_for_scrapers.csv'
    # We use the old file just to look up the 'Exchange' info (HOSE vs HNX)
    METADATA_FILE = 'data/top_quality_value_stocks.csv'
    OUTPUT_FILE = 'data/raw_news_data.csv'

    try:
        if not os.path.exists(TARGET_FILE):
            print(f"ERROR: {TARGET_FILE} not found. Run merge_and_filter.py first.")
            return

        print(f"Loading targets from {TARGET_FILE}...")
        targets_df = pd.read_csv(TARGET_FILE)
        
        # --- RESTORE EXCHANGE INFO ---
        # The target list might check missing 'exchange', so we merge with the metadata file
        if os.path.exists(METADATA_FILE):
            meta_df = pd.read_csv(METADATA_FILE)
            # Keep only ticker and exchange from metadata
            if 'exchange' in meta_df.columns:
                targets_df = pd.merge(targets_df, meta_df[['ticker', 'exchange']], on='ticker', how='left')
        
        # Fill missing exchange with 'Unknown' to avoid crashes
        if 'exchange' not in targets_df.columns:
            targets_df['exchange'] = 'Unknown'

        # Get list of all tickers for HSX filtering
        all_target_tickers = targets_df['ticker'].tolist()

        print(f"Loaded {len(targets_df)} targets. Starting news scrape...")
        
        master_news_list = []

        # --- 1. GET HOSE NEWS (Bulk Fetch) ---
        # Only run this once. It scans the whole HSX news feed and picks out ANY of our tickers.
        hsx_news = get_hsx_news_general(all_target_tickers)
        master_news_list.extend(hsx_news)
        
        # --- 2. LOOP FOR CAFEF & VNSTOCK ---
        print("Starting ticker-specific scrape (CafeF & Vnstock)...")
        
        cafef_count = 0
        vnstock_count = 0
        
        for index, row in tqdm(targets_df.iterrows(), total=targets_df.shape[0]):
            ticker = row['ticker']
            exchange = row['exchange']
            
            # Smart Logic: Only scrape CafeF if it's NOT on HOSE (HSX covers HOSE)
            # If exchange is unknown, we scrape CafeF just to be safe.
            if exchange in ['HNX', 'UPCOM', 'Unknown']:
                new_cafef = get_cafef_news(ticker)
                master_news_list.extend(new_cafef)
                cafef_count += len(new_cafef)
            
            # VNSTOCK (Scrape for everyone as a backup/second source)
            new_vnstock = get_vnstock_news(ticker)
            master_news_list.extend(new_vnstock)
            vnstock_count += len(new_vnstock)
            
            time.sleep(1.0) 
            
        print(f"\n[SUMMARY] HSX Articles: {len(hsx_news)} | CafeF Articles: {cafef_count} | Vnstock Articles: {vnstock_count}")

        if not master_news_list:
            print("\nFATAL: No news was found.")
            return

        # Clean up duplicates
        news_df = pd.DataFrame(master_news_list)
        before_dedupe = len(news_df)
        news_df = news_df.drop_duplicates(subset=['ticker', 'news_title'])
        after_dedupe = len(news_df)
        
        if not os.path.exists('data'):
            os.makedirs('data')
            
        news_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
        print(f"\n--- SUCCESS ---")
        print(f"Dropped {before_dedupe - after_dedupe} duplicates.")
        print(f"Saved {after_dedupe} unique news items to {OUTPUT_FILE}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_data_gathering()