import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- PAGE CONFIG ---
st.set_page_config(page_title="Quant Value Dashboard", layout="wide")

# --- LOAD DATA ---
FILE_PATH = 'MASTER_INVESTMENT_DASHBOARD.csv'

@st.cache_data
def load_data():
    if not os.path.exists(FILE_PATH):
        return None
    df = pd.read_csv(FILE_PATH)
    return df

df = load_data()

# --- TITLE & HEADER ---
st.title("🇻🇳 AI-Driven Value Strategy: Execution Dashboard")
st.markdown("### Focus: Vietnamese Equities (HOSE/HNX)")

if df is None:
    st.error(f"❌ Could not find {FILE_PATH}. Please run merge_all_signals.py first.")
    st.stop()

# --- SIDEBAR FILTERS ---
st.sidebar.header("Filter Results")
min_alpha = st.sidebar.slider("Min Alpha Score", 0, 100, 50)
show_frauds = st.sidebar.checkbox("Show Accounting Risks", value=False)

# Filter Logic
filtered_df = df[df['ALPHA_SCORE'] >= min_alpha]
if not show_frauds:
    filtered_df = filtered_df[filtered_df['accounting_risk'] == 'SAFE']

# --- TOP LEVEL METRICS ---
col1, col2, col3, col4 = st.columns(4)

total_scanned = len(df)
strong_buys = len(df[df['FINAL_ACTION'].str.contains("STRONG BUY")])
buys = len(df[df['FINAL_ACTION'] == "BUY (MOMENTUM + VALUE)"])
frauds = len(df[df['accounting_risk'] == 'HIGH RISK'])

col1.metric("Total Candidates Scanned", total_scanned)
col2.metric("🚀 Strong Buy Signals", strong_buys)
col3.metric("📈 Momentum Buys", buys)
col4.metric("⚠️ Accounting Risks", frauds, delta_color="inverse")

st.markdown("---")

# --- SECTION 1: THE WINNERS (Actionable Trades) ---
st.header("🏆 High-Conviction Opportunities")

# Filter for Buys
buy_list = filtered_df[filtered_df['FINAL_ACTION'].str.contains("BUY")]

if not buy_list.empty:
    # Stylized Dataframe
    st.dataframe(
        buy_list[['ticker', 'FINAL_ACTION', 'ALPHA_SCORE', 'current_price', 'technical_signal', 'pe', 'final_sentiment']],
        column_config={
            "ALPHA_SCORE": st.column_config.ProgressColumn("Alpha Score", format="%f", min_value=0, max_value=100),
            "final_sentiment": st.column_config.NumberColumn("Sentiment Score", format="%.2f"),
        },
        width='stretch',
        hide_index=True
    )
else:
    st.info("No 'Buy' signals found matching current filters.")

# --- SECTION 2: VISUALIZATION ---
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("The 'Golden Quadrant' (Value vs. Sentiment)")
    # Scatter Plot: Alpha Score vs Sentiment
    fig = px.scatter(
        filtered_df, 
        x="sector_pe", 
        y="ALPHA_SCORE", 
        size="ALPHA_SCORE",
        color="FINAL_ACTION",
        hover_name="ticker",
        title="Alpha Score vs. Sector P/E Context",
        color_discrete_map={
            "STRONG BUY (VALUE + DIP)": "#00CC96",  # Green
            "BUY (MOMENTUM + VALUE)": "#636EFA",    # Blue
            "WATCHLIST (WAIT FOR UPTREND)": "#EF553B", # Red
            "HOLD / NEUTRAL": "#FECB52" # Yellow
        }
    )
    st.plotly_chart(fig, width='stretch')

with col_right:
    st.subheader("Top 5 by Alpha Score")
    top_5 = filtered_df.sort_values('ALPHA_SCORE', ascending=False).head(5)
    st.dataframe(top_5[['ticker', 'ALPHA_SCORE', 'accounting_risk']], width='stretch', hide_index=True)

# --- SECTION 3: WATCHLIST (Good Value, Bad Timing) ---
with st.expander("👀 View Watchlist (High Value / Downtrend)"):
    watchlist = filtered_df[filtered_df['FINAL_ACTION'].str.contains("WATCHLIST")]
    st.dataframe(watchlist, width='stretch', hide_index=True)

# --- SECTION 4: THE TRAP LIST (Fraud Risks) ---
if show_frauds:
    with st.expander("🚨 FRAUD RISK AUDIT (Beneish M-Score Failures)", expanded=True):
        risky = df[df['accounting_risk'] == 'HIGH RISK']
        st.dataframe(risky, width='stretch', hide_index=True)