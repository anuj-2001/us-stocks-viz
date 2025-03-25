import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta

# Initialize DuckDB connection
conn = duckdb.connect(database='stocks.db')

# Page configuration
st.set_page_config(page_title="Equal Weight Index Dashboard", layout="wide")

# Custom CSS for metrics
st.markdown("""
<style>
div[data-testid="metric-container"] {
    background-color: rgba(28, 131, 225, 0.1);
    border: 1px solid rgba(28, 131, 225, 0.1);
    padding: 5% 5% 5% 10%;
    border-radius: 5px;
    color: rgb(30, 103, 119);
    overflow-wrap: break-word;
}

div[data-testid="metric-container"] > label {
    color: rgba(35, 86, 131);
}
</style>
""", unsafe_allow_html=True)

def load_performance_data():
    return conn.execute("""
        SELECT date, index_value, daily_return 
        FROM index.performance 
        WHERE date >= CURRENT_DATE - INTERVAL '30 DAYS'
        ORDER BY date
    """).fetchdf()

def load_constituents(date):
    return conn.execute(f"""
        SELECT ticker, weight, notional 
        FROM index.constituents 
        WHERE date = '{date}'
    """).fetchdf()

def load_composition_changes():
    return conn.execute("""
        SELECT date, ticker, action_type, notional_change 
        FROM index.rebalancing_log 
        ORDER BY date DESC
    """).fetchdf()

def calculate_metrics():
    metrics = conn.execute("""
        WITH changes AS (
            SELECT COUNT(DISTINCT date) AS change_days 
            FROM index.rebalancing_log
        ),
        returns AS (
            SELECT 
                FIRST(index_value) AS start_value,
                LAST(index_value) AS end_value,
                AVG(daily_return) AS avg_return
            FROM index.performance
            WHERE date >= CURRENT_DATE - INTERVAL '30 DAYS'
        )
        SELECT 
            (end_value/start_value - 1) * 100 AS cumulative_return,
            avg_return * 100 AS avg_daily_return,
            change_days
        FROM returns, changes
    """).fetchone()
    
    return metrics

# Main dashboard
st.title("Equal Weight Index Dashboard")

# Load data
performance_df = load_performance_data()

changes_df = load_composition_changes()
cumulative_return, avg_daily_return, change_days = calculate_metrics()

# Summary Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Cumulative Return", f"{cumulative_return:.2f}%")
col2.metric("Avg Daily Return", f"{avg_daily_return:.2f}%")
col3.metric("Composition Changes", change_days)

# Performance Chart
st.subheader("Index Performance (30 Days)")
fig = px.line(performance_df, x='date', y='index_value',
              labels={'index_value': 'Index Value', 'date': 'Date'})

# Add composition change markers
change_dates = changes_df['date'].unique()
for date in change_dates:
    fig.add_vline(x=date, line_width=1, line_dash="dash", line_color="red")

st.plotly_chart(fig, use_container_width=True)

# Composition Section
st.subheader("Index Composition")

# Date selector
min_date = performance_df['date'].min().date()
max_date = performance_df['date'].max().date()
selected_date = st.slider("Select Date", 
                         min_value=min_date,
                         max_value=max_date,
                         value=max_date)

# Load composition data
composition_df = load_constituents(selected_date)

# Composition Charts
col1, col2 = st.columns(2)

with col1:
    st.markdown("**Constituents Table**")
    st.dataframe(composition_df.style.format({
        'weight': '{:.2%}',
        'notional': '${:,.2f}'
    }), use_container_width=True)

with col2:
    st.markdown("**Weight Distribution**")
    fig = px.bar(composition_df, x='ticker', y='notional',
                 labels={'notional': 'Notional Value', 'ticker': 'Ticker'})
    st.plotly_chart(fig, use_container_width=True)

# Composition Changes
st.subheader("Recent Composition Changes")
changes_display = changes_df.copy()
changes_display['date'] = pd.to_datetime(changes_display['date']).dt.date
changes_display['notional_change'] = changes_display['notional_change'].apply(
    lambda x: f"${x:,.2f}" if x else "-"
)

st.dataframe(
    changes_display.style.applymap(
        lambda x: 'color: green' if x == 'ADD' else 'color: red' if x == 'REMOVE' else '',
        subset=['action_type']
    ),
    use_container_width=True,
    column_order=['date', 'ticker', 'action_type', 'notional_change']
)

# Run with: streamlit run dashboard.py