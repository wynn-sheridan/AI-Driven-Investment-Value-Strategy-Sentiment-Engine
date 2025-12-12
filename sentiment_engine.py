import pandas as pd
from transformers import pipeline
from deep_translator import GoogleTranslator
from tqdm import tqdm
import torch
import os
import time

# --- CONFIGURATION ---
NEWS_FILE = 'data/raw_news_data.csv'
FORUM_FILE = 'data/f319_smart_filtered.csv'
OUTPUT_FILE = 'data/processed_sentiment.csv'

# Setup FinBERT (The Financial Brain)
print("Loading FinBERT Model (this might take a moment)...")
device = 0 if torch.cuda.is_available() else -1
classifier = pipeline('sentiment-analysis', model='ProsusAI/finbert', device=device)

def translate_and_score(text_list, source_type):
    """
    1. Translates Vietnamese -> English
    2. Runs FinBERT
    3. Returns list of scores (-1 to 1)
    """
    translator = GoogleTranslator(source='auto', target='en')
    scores = []
    
    # We batch process to be polite to Google Translate API
    for text in tqdm(text_list, desc=f"Processing {source_type}"):
        try:
            # 1. Translate
            # Simple caching could go here, but for 50 stocks, direct is usually fine
            eng_text = translator.translate(text)
            
            # 2. Analyze (FinBERT returns {label: 'positive', score: 0.99})
            result = classifier(eng_text)[0]
            
            # 3. Convert to Number (-1, 0, 1) * Confidence
            val = 0
            if result['label'] == 'positive':
                val = 1 * result['score']
            elif result['label'] == 'negative':
                val = -1 * result['score']
            else: # Neutral
                val = 0 
                
            scores.append(val)
            
            # Slight delay to avoid IP ban from Translator
            time.sleep(0.3)
            
        except Exception as e:
            # If translation fails, assume Neutral (0)
            scores.append(0)
            
    return scores

def run_sentiment_analysis():
    all_data = []

    # --- 1. PROCESS NEWS (Professional Sentiment) ---
    if os.path.exists(NEWS_FILE):
        news_df = pd.read_csv(NEWS_FILE)
        print(f"Loaded {len(news_df)} news articles.")
        
        # Filter for only relevant columns
        scores = translate_and_score(news_df['news_title'].tolist(), "News")
        news_df['sentiment_score'] = scores
        news_df['type'] = 'News'
        
        # Normalize columns
        all_data.append(news_df[['ticker', 'sentiment_score', 'type', 'date']])
    else:
        print("⚠️ No News Data Found.")

    # --- 2. PROCESS FORUM (Retail Sentiment) ---
    if os.path.exists(FORUM_FILE):
        forum_df = pd.read_csv(FORUM_FILE)
        print(f"Loaded {len(forum_df)} forum discussions.")
        
        scores = translate_and_score(forum_df['original_title'].tolist(), "Forum")
        forum_df['sentiment_score'] = scores
        forum_df['type'] = 'Forum'
        
        # Normalize columns (Forum doesn't have date usually, so we fill N/A)
        # Or we can assume 'Latest' since we just scraped it
        forum_df['date'] = pd.Timestamp.now().strftime('%d/%m/%Y')
        
        all_data.append(forum_df[['ticker', 'sentiment_score', 'type', 'date']])
    else:
        print("⚠️ No Forum Data Found.")

    # --- 3. COMBINE & SAVE ---
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        full_df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ [SUCCESS] Processed sentiment for {len(full_df)} items.")
        print(f"Saved to {OUTPUT_FILE}")
    else:
        print("❌ No data to process.")

if __name__ == "__main__":
    run_sentiment_analysis()