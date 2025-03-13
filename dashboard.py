import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from index_construction import EqualWeightedIndex

# Initialize the index
index = EqualWeightedIndex()

# Fetch performance data
performance_data = index.track_index_performance()

# Convert performance data to a DataFrame for easier visualization
performance_df = pd.DataFrame(list(performance_data.items()), columns=["Date", "Index Value"])

# Streamlit app
st.title("Equal Weighted Index Dashboard")

# Performance Visualization
st.header("Index Performance Over the Past Month")
st.line_chart(performance_df.set_index("Date"))

# Composition Visualization
st.header("Index Composition on Selected Day")
selected_date = st.date_input("Select a date", value=performance_df["Date"].max())
top_100_stocks = index.fetch_top_100_stocks(selected_date)
composition_df = pd.DataFrame(top_100_stocks, columns=["Ticker", "Market Cap"])
st.write(composition_df)

# Composition Changes Visualization
st.header("Index Composition Changes")
log_query = "SELECT * FROM index_composition_log;"
composition_changes = index.conn.execute(log_query).fetchall()
changes_df = pd.DataFrame(composition_changes, columns=["Date", "Ticker", "Action"])
st.write(changes_df)

# Summary Metrics (Bonus)
st.header("Summary Metrics")
cumulative_return = (performance_df["Index Value"].iloc[-1] - performance_df["Index Value"].iloc[0]) / performance_df["Index Value"].iloc[0]
daily_returns = performance_df["Index Value"].pct_change().dropna()
num_changes = len(changes_df)

st.metric("Cumulative Return", f"{cumulative_return:.2%}")
st.metric("Average Daily Return", f"{daily_returns.mean():.2%}")
st.metric("Number of Composition Changes", num_changes)

# Close the database connection
index.close_connection()