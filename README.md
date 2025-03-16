# Equal Weighted Index Tracker

This project implements an Equal Weighted Index tracker that fetches stock data, processes it, and visualizes index performance using Streamlit.

## Features

- Fetch stock data using Yahoo Finance API
- Store and manage data using DuckDB
- Construct an Equal Weighted Index
- Track index performance and composition changes
- Visualize stock index trends using Streamlit

## File Structure

- `dashboard.py`: Streamlit dashboard for visualizing index performance.
- `fetch_data.py`: Fetches stock data and stores it in DuckDB.
- `index_construction.py`: Constructs and tracks the equal-weighted index.
- `duckdb_utils.py`: Utility functions for storing data in DuckDB.

## Requirements

Ensure you have the following dependencies installed:

```bash
pip install streamlit pandas matplotlib duckdb pyspark requests yfinance
```

## Running the Application

To start the Streamlit dashboard, run the following command:

```bash
streamlit run dashboard.py
```

## How It Works

1. **Fetching Data**

   - `fetch_data.py` retrieves S&P 500 stock data from Yahoo Finance.
   - The data is processed and stored in a DuckDB database (`stocks.db`).

2. **Index Construction**

   - `index_construction.py` constructs an Equal Weighted Index by selecting the top 100 stocks by market capitalization.
   - It tracks daily performance and logs composition changes.

3. **Visualization**

   - `dashboard.py` provides a Streamlit-based dashboard for viewing index performance, composition, and summary metrics.

## License

This project is licensed under the MIT License.

Give me a little more 
