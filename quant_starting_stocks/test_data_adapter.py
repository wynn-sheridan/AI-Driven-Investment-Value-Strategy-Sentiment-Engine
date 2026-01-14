from quant_starting_stocks.data_adapter import DataProvider
import time

print("--- TESTING DATA ADAPTER (HYBRID VERSION) ---\n")

# ---------------------------------------------------------
# TEST 1: GET UNIVERSE
# ---------------------------------------------------------
print("1. Testing get_all_tickers()...")
try:
    tickers = DataProvider.get_all_tickers(limit=5)
    if tickers and len(tickers) > 0:
        print(f"✅ SUCCESS: Fetched {len(tickers)} tickers: {tickers}")
    else:
        print("❌ FAILURE: Returned empty list.")
except Exception as e:
    print(f"❌ CRITICAL ERROR: {e}")

print("-" * 30)

# ---------------------------------------------------------
# TEST 2: FETCH FUNDAMENTALS (Static Data)
# ---------------------------------------------------------
symbol = "VCB"
print(f"2. Testing fetch_single_stock_fundamentals('{symbol}')...")
try:
    data = DataProvider.fetch_single_stock_fundamentals(symbol)
    
    if data:
        print("✅ SUCCESS: Fundamentals fetched!")
        print(f"   Data: {data}")
        
        # Check for specific keys required by the Base Scan
        required = ['eps', 'bvps', 'roe']
        missing = [k for k in required if k not in data]
        
        if not missing:
            print("   ✅ Keys Check: EPS and BVPS present.")
        else:
            print(f"   ❌ Keys Check: Missing {missing}")
    else:
        print("❌ FAILURE: Returned None.")

except Exception as e:
    print(f"❌ CRITICAL ERROR: {e}")

print("-" * 30)

# ---------------------------------------------------------
# TEST 3: LIVE PRICE BATCH (The Fix)
# ---------------------------------------------------------
# We are testing if the parallel 'History' fetch works
batch_symbols = ['VCB', 'FPT', 'HPG']
print(f"3. Testing fetch_live_price_batch({batch_symbols})...")
print("   (This now runs 3 parallel threads to 'fake' a batch request)")

start = time.time()
try:
    prices = DataProvider.fetch_live_price_batch(batch_symbols)
    elapsed = time.time() - start
    
    if prices and len(prices) > 0:
        print(f"✅ SUCCESS! Fetched {len(prices)} prices in {elapsed:.2f}s.")
        print(f"   Data: {prices}")
        
        # Validation
        for p in prices:
            if p['price'] > 0:
                print(f"   ✅ {p['ticker']}: {p['price']}")
            else:
                print(f"   ⚠️ {p['ticker']}: Price is 0/Invalid")
    else:
        print("❌ FAILURE: Returned empty list.")

except Exception as e:
    print(f"❌ CRITICAL ERROR: {e}")

print("\n--- TEST COMPLETE ---")