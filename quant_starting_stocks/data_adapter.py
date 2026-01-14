import pandas as pd
from vnstock import Listing, Vnstock
import logging
import time
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
METRIC_MAP = {
    'pe': ('Chỉ tiêu định giá', 'P/E'),
    'pb': ('Chỉ tiêu định giá', 'P/B'),
    'roe': ('Chỉ tiêu khả năng sinh lợi', 'ROE (%)'),
    'market_cap': ('Chỉ tiêu định giá', 'Market Capital (Bn. VND)'),
    'eps': ('Chỉ tiêu định giá', 'EPS (VND)'),
    'bvps': ('Chỉ tiêu định giá', 'BVPS (VND)')
}

class DataProvider:
    
    @staticmethod
    def _safe_api_call(func, *args, **kwargs):
        """Retries API calls if server is busy."""
        max_retries = 3
        base_wait = 2 
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "502" in error_msg or "quá nhiều request" in error_msg:
                    wait_time = base_wait * (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                else:
                    return None
        return None

    @staticmethod
    def get_all_tickers(limit=None):
        try:
            listing = Listing(source='VCI') 
            universe = listing.all_symbols()
            if isinstance(universe, pd.DataFrame):
                if 'ticker' in universe.columns: tickers = universe['ticker'].tolist()
                elif 'symbol' in universe.columns: tickers = universe['symbol'].tolist()
                else: return []
            else: tickers = universe
            return tickers[:limit] if limit else tickers
        except Exception:
            return []

    @staticmethod
    def fetch_single_stock_fundamentals(symbol):
        """Phase 1: Fetch EPS/BVPS (Quarterly)"""
        def _work():
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            df = stock.finance.ratio(period='year', lang='en', dropna=True)
            if df is None or df.empty: return None
            latest = df.iloc[0]
            try:
                return {
                    'ticker': symbol, 
                    'eps': float(latest.get(METRIC_MAP['eps'], 0)), 
                    'bvps': float(latest.get(METRIC_MAP['bvps'], 0)), 
                    'roe': float(latest.get(METRIC_MAP['roe'], 0)) / 100.0
                }
            except Exception: return None

        time.sleep(random.uniform(0.5, 1.0))
        return DataProvider._safe_api_call(_work)

    # --- NEW: Helper for individual price fetching ---
    @staticmethod
    def _fetch_single_price_history(symbol):
        """Fetches the latest closing price using the History API."""
        try:
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            
            # Get last 7 days to ensure we find a trading day
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            
            df = stock.quote.history(start=start_date, end=end_date)
            
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                # 'close' is standard, but we use .get just in case
                price = latest.get('close') or latest.get('Close')
                if price and price > 0:
                    return {'ticker': symbol, 'price': float(price)}
            return None
        except Exception:
            return None

    @staticmethod
    def fetch_live_price_batch(tickers):
        """
        Phase 2: Live Price Update.
        Since the 'Batch' API is blocked, we simulate it by running 
        parallel requests to the 'History' API.
        """
        if isinstance(tickers, str): tickers = tickers.split(',')
        
        results = []
        # We use a mini-threadpool here to keep it fast
        # 10 workers is safe for the History endpoint
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_ticker = {
                executor.submit(DataProvider._fetch_single_price_history, t): t 
                for t in tickers
            }
            
            for future in as_completed(future_to_ticker):
                res = future.result()
                if res: results.append(res)
        
        return results