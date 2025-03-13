import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from index_construction import EqualWeightedIndex

# Initialize the index
index = EqualWeightedIndex()

# Fetch performance data
performance_data = index.track_index_performance()
if not performance_data:
    st.error("No performance data available.")
    index.close_connection()
    st.stop()

# Convert to DataFrame for visualization
performance_df = pd.DataFrame(performance_data.items(), columns=["Date", "Index Value"])

# Streamlit App
st.title("Equal Weighted Index Dashboard")

# Performance Chart
st.header("Index Performance Over the Past Month")
st.line_chart(performance_df.set_index("Date"))

# Index Composition
st.header("Index Composition on Selected Date")
selected_date = st.date_input("Select a date", value=performance_df["Date"].max())

# Fetch and display top 100 stocks
top_100_stocks = index.fetch_top_100_stocks(selected_date)
if top_100_stocks:
    composition_df = pd.DataFrame(top_100_stocks, columns=["Ticker", "Market Cap"])
    st.dataframe(composition_df)
else:
    st.warning("No data available for the selected date.")

# Composition Changes Log
st.header("Index Composition Changes")
changes_df = pd.read_sql("SELECT * FROM index_composition_log;", index.conn)
if not changes_df.empty:
    st.dataframe(changes_df)
else:
    st.warning("No composition changes recorded.")

# Summary Metrics
st.header("Summary Metrics")
cumulative_return = (performance_df["Index Value"].iloc[-1] - performance_df["Index Value"].iloc[0]) / performance_df["Index Value"].iloc[0]
daily_returns = performance_df["Index Value"].pct_change().dropna()
num_changes = len(changes_df)

st.metric("Cumulative Return", f"{cumulative_return:.2%}")
st.metric("Average Daily Return", f"{daily_returns.mean():.2%}")
st.metric("Number of Composition Changes", num_changes)

# Close database connection
index.close_connection()
