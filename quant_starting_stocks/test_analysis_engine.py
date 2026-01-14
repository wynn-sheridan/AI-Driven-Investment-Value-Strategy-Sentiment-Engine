import pandas as pd
from analysis_engine import AnalysisEngine
import time

print("--- TESTING ANALYSIS ENGINE (THE BRAIN) ---\n")

# ---------------------------------------------------------
# TEST 1: RANKING LOGIC (Using Fake Data)
# ---------------------------------------------------------
print("1. Testing Ranking & Filtering Logic...")

# Create a dummy dataset
# Stock A: Cheap and Profitable (The Winner)
# Stock B: Expensive and Low Profit (The Loser)
# Stock C: Negative P/E (The Garbage - Should be removed)
mock_data = {
    'ticker': ['STOCK_A', 'STOCK_B', 'STOCK_C'],
    'pe': [5.0, 20.0, -5.0],      # A is cheap, B is expensive, C is invalid
    'pb': [1.0, 3.0, 1.0],        # A is cheap
    'roe': [0.25, 0.05, 0.10]     # A is high return (25%), B is low (5%)
}
df_mock = pd.DataFrame(mock_data)

try:
    results = AnalysisEngine.rank_and_filter(df_mock, top_n=10)
    
    print("   Input Data:")
    print(df_mock.to_string(index=False))
    print("\n   Output (Ranked):")
    print(results[['ticker', 'pe', 'initial_rank']].to_string(index=False))

    # Assertions
    if 'STOCK_C' not in results['ticker'].values:
        print("\n   ✅ PASS: 'STOCK_C' (Negative P/E) was correctly filtered out.")
    else:
        print("\n   ❌ FAIL: 'STOCK_C' was not removed.")

    if results.iloc[0]['ticker'] == 'STOCK_A':
        print("   ✅ PASS: 'STOCK_A' (The Value Stock) is Ranked #1.")
    else:
        print("   ❌ FAIL: Ranking logic is wrong.")

except Exception as e:
    print(f"❌ CRITICAL ERROR IN RANKING: {e}")

print("-" * 30)

# ---------------------------------------------------------
# TEST 2: PIOTROSKI SCORE (Using Real Data)
# ---------------------------------------------------------
symbol = "VCB"
print(f"2. Testing Piotroski Calculation for {symbol}...")
print("   (This tests if the engine can fetch BS/IS/CF reports and do the math)")

try:
    start_time = time.time()
    score = AnalysisEngine.get_piotroski_score(symbol)
    elapsed = time.time() - start_time
    
    print(f"   Calculated F-Score: {score}/9")
    print(f"   Time Taken: {elapsed:.2f}s")
    
    if isinstance(score, int) and 0 <= score <= 9:
        print(f"   ✅ PASS: Score is a valid integer (0-9).")
    elif score == 0:
        print(f"   ⚠️ WARNING: Score is 0. This might be correct, or it might mean data fetching failed.")
    else:
        print(f"   ❌ FAIL: Invalid score returned.")

except Exception as e:
    print(f"❌ CRITICAL ERROR IN PIOTROSKI: {e}")

print("\n--- TEST COMPLETE ---")