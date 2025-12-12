import pandas as pd
from vnstock import Vnstock
from datetime import datetime, timedelta
import os
from tqdm import tqdm
import time
import random
import numpy as np

# --- CONFIGURATION ---
INPUT_FILE = 'data/target_list_with_forensics.csv'
OUTPUT_FILE = 'data/final_target_list.csv'

# --- 1. MANUAL TECHNICAL INDICATOR FUNCTIONS ---
def calculate_sma(series, window):
    """Calculates Simple Moving Average."""
    return series.rolling(window=window).mean()

def calculate_rsi(series, window=14):
    """
    Calculates RSI manually using Pandas (Wilder's Smoothing).
    """
    delta = series.diff()
    gain = (delta.where(delta > 0, 0))
    loss = (-delta.where(delta < 0, 0))

    # Use Exponential Weighted Moving Average (EWM) for Wilder's method
    avg_gain = gain.ewm(com=window - 1, min_periods=window).mean()
    avg_loss = loss.ewm(com=window - 1, min_periods=window).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# --- 2. MAIN LOGIC ---

def get_technical_indicators(ticker):
    """
    Fetches 1 year of history using the Object-Oriented Vnstock method.
    """
    try:
        # Define Dates (Last 365 days)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # --- FIX: Use the Vnstock Class directly (Same as Step 1) ---
        # We use source='TCBS' which is the standard for price history
        stock = Vnstock().stock(symbol=ticker, source='VCI')
        
        # Fetch history (The method might be .history or .quote.history depending on version)
        # We try the standard quote history first
        try:
            df = stock.quote.history(start=start_date, end=end_date, interval='1D')
        except:
            # Fallback for older versions or different API structures
            return None
        
        if df is None or df.empty or len(df) < 200:
            return None # Not enough data for SMA 200

        # Clean Data (Ensure 'close' is numeric)
        # VNStock usually returns 'close' or 'Close'
        if 'close' in df.columns:
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
        elif 'Close' in df.columns:
            df['close'] = pd.to_numeric(df['Close'], errors='coerce')
        
        # --- CALCULATIONS ---
        
        # RSI (14 period)
        df['RSI'] = calculate_rsi(df['close'], window=14)
        
        # SMA (50 and 200 period)
        df['SMA_50'] = calculate_sma(df['close'], window=50)
        df['SMA_200'] = calculate_sma(df['close'], window=200)
        
        # Get the LATEST values
        latest = df.iloc[-1]
        
        result = {
            'current_price': latest['close'],
            'RSI_14': round(latest['RSI'], 2) if not pd.isna(latest['RSI']) else 50,
            'SMA_50': round(latest['SMA_50'], 2),
            'SMA_200': round(latest['SMA_200'], 2)
        }
        
        return result

    except Exception as e:
        # Debugging: Uncomment if you need to see specific errors
        # print(f"Error fetching TA for {ticker}: {e}")
        return None

def determine_signal(row):
    """
    Logic for the final 'Technical Signal'
    """
    price = row['current_price']
    sma200 = row['SMA_200']
    rsi = row['RSI_14']
    
    if pd.isna(price) or pd.isna(sma200) or pd.isna(rsi):
        return "NO DATA"

    # 1. Trend Check (Are we in an Uptrend?)
    # Price > SMA200 is the gold standard for "Long Term Uptrend"
    is_uptrend = price > sma200
    
    # 2. Momentum Check (Is it a good entry?)
    is_oversold = rsi < 40   # Cheap (Buy zone)
    is_overbought = rsi > 70 # Expensive (Sell zone)
    
    if is_uptrend and is_oversold:
        return "STRONG BUY THE DIP"
    elif is_uptrend:
        return "UPTREND (HOLD/BUY)"
    elif not is_uptrend and is_oversold:
        return "Falling Knife (High Risk Value)"
    else:
        return "DOWNTREND (AVOID)"

def run_technical_analysis():
    print("--- 📉 TECHNICAL ANALYSIS: TIMING THE ENTRY ---")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Missing {INPUT_FILE}. Run forensic_check.py first.")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} stocks. Fetching price history...")

    prices = []
    rsis = []
    sma50s = []
    sma200s = []

    for ticker in tqdm(df['ticker'], desc="Calculating Indicators"):
        data = get_technical_indicators(ticker)
        
        if data:
            prices.append(data['current_price'])
            rsis.append(data['RSI_14'])
            sma50s.append(data['SMA_50'])
            sma200s.append(data['SMA_200'])
        else:
            prices.append(np.nan)
            rsis.append(np.nan)
            sma50s.append(np.nan)
            sma200s.append(np.nan)
        
        # Jitter to avoid rate limits
        time.sleep(random.uniform(0.2, 0.5))

    # Append columns
    df['current_price'] = prices
    df['RSI_14'] = rsis
    df['SMA_50'] = sma50s
    df['SMA_200'] = sma200s
    
    print("Generating Trading Signals...")
    df['technical_signal'] = df.apply(determine_signal, axis=1)
    
    df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n✅ [SUCCESS] Analysis Complete. Saved to {OUTPUT_FILE}")
    
    # Filter: Must be SAFE accounting AND in an UPTREND
    perfect_setup = df[
        (df['accounting_risk'] == 'SAFE') & 
        (df['technical_signal'].str.contains('UPTREND') | df['technical_signal'].str.contains('BUY'))
    ]
    
    if not perfect_setup.empty:
        print(f"\n💎 {len(perfect_setup)} STOCKS IN 'GOLDEN' SETUP (Safe + Uptrend):")
        print(perfect_setup[['ticker', 'final_conviction_score', 'RSI_14', 'technical_signal']].head(10))
    else:
        print("\nNo stocks met the perfect 'Golden Setup' criteria today.")

if __name__ == "__main__":
    run_technical_analysis()