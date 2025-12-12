import pandas as pd
import numpy as np

# --- CONFIGURATION ---
TARGET_LIST = 'data/target_list_for_scrapers.csv'
SENTIMENT_DATA = 'data/processed_sentiment.csv'
FINAL_REPORT = 'Final_Investment_Report.csv'

# Weights (Adjust these based on your preference)
W_FUNDAMENTALS = 0.40  # 40% (PE, Piotroski)
W_SECTOR = 0.30        # 30% (Sector Health)
W_SENTIMENT = 0.30     # 30% (News + Forum)

def generate_final_report():
    try:
        targets_df = pd.read_csv(TARGET_LIST)
        sentiment_df = pd.read_csv(SENTIMENT_DATA)
    except FileNotFoundError:
        print("❌ Missing input files. Run previous steps first.")
        return

    print("--- GENERATING FINAL INVESTMENT CONVICTION ---")

    # 1. AGGREGATE SENTIMENT BY TICKER
    # We separate News vs Forum to weight them differently
    news_sent = sentiment_df[sentiment_df['type'] == 'News'].groupby('ticker')['sentiment_score'].mean()
    forum_sent = sentiment_df[sentiment_df['type'] == 'Forum'].groupby('ticker')['sentiment_score'].mean()
    
    # Volume counts (Confidence check)
    news_count = sentiment_df[sentiment_df['type'] == 'News'].groupby('ticker').size()
    forum_count = sentiment_df[sentiment_df['type'] == 'Forum'].groupby('ticker').size()

    # 2. MERGE INTO MASTER DATAFRAME
    # Map the series to the dataframe
    targets_df['sent_news_score'] = targets_df['ticker'].map(news_sent).fillna(0)
    targets_df['sent_forum_score'] = targets_df['ticker'].map(forum_sent).fillna(0)
    
    targets_df['news_count'] = targets_df['ticker'].map(news_count).fillna(0)
    targets_df['forum_count'] = targets_df['ticker'].map(forum_count).fillna(0)

    # 3. CALCULATE COMPOSITE SENTIMENT SCORE
    # Logic: News is worth 70% of the sentiment score, Forum is worth 30%
    # If a stock has 0 news, we rely 100% on Forum (and vice versa)
    
    def calc_combined_sentiment(row):
        n_score = row['sent_news_score']
        f_score = row['sent_forum_score']
        n_count = row['news_count']
        f_count = row['forum_count']
        
        if n_count > 0 and f_count > 0:
            return (n_score * 0.7) + (f_score * 0.3)
        elif n_count > 0:
            return n_score
        elif f_count > 0:
            return f_score
        else:
            return 0  # No info = Neutral

    targets_df['final_sentiment'] = targets_df.apply(calc_combined_sentiment, axis=1)

    # 4. CALCULATE FINAL CONVICTION
    # Normalize previous scores to 0-100 scale for readability
    
    # Fundamental Score (already calculated in merge step, usually 0-1 scale)
    # We assume 'final_conviction_score' from step 2 was roughly -1 to 1.
    # Let's normalize it to 0-100.
    
    # Creating a cleaner display score
    # Piotroski (0-9) -> normalized to 0-1
    fund_norm = targets_df['piotroski_f_score'] / 9.0
    
    # Sector PE Discount (Positive is good)
    # If Discount is -0.2 (20% cheaper), we want that to be positive score.
    sector_norm = (targets_df['sector_pe'] - targets_df['pe']) / targets_df['sector_pe']
    # Cap it at 1.0 (100% discount) to avoid outliers
    sector_norm = sector_norm.clip(-1, 1) 
    
    # Sentiment (-1 to 1) -> already normalized
    sent_norm = targets_df['final_sentiment']
    
    # THE FORMULA
    targets_df['ALPHA_SCORE'] = (
        (fund_norm * W_FUNDAMENTALS) + 
        (sector_norm * W_SECTOR) + 
        (sent_norm * W_SENTIMENT)
    ) * 100

    # 5. FORMATTING & RANKING
    final_cols = [
        'ticker', 'industry', 'ALPHA_SCORE', 
        'pe', 'sector_pe', 'piotroski_f_score',
        'news_count', 'forum_count', 'final_sentiment'
    ]
    
    report = targets_df.sort_values('ALPHA_SCORE', ascending=False)[final_cols]
    
    report.to_csv(FINAL_REPORT, index=False)
    
    print(f"\n🏆 TOP 10 HIDDEN GEMS 🏆")
    print(report.head(10).to_string(index=False))
    print(f"\nFull report saved to {FINAL_REPORT}")

if __name__ == "__main__":
    generate_final_report()