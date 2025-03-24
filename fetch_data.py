import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import ssl
from datetime import datetime, timedelta
import time
from duckdb_utils import store_in_duckdb  
import os
import duckdb
# from index_construction import IndexConstructor
import yfinance as yf

# Ensure secure HTTPS context
ssl._create_default_https_context = ssl._create_stdlib_context

class StockDataFetcher:
    def __init__(self):
        pass  
        
    @staticmethod
    def fetch_stock_data(tickers, period):
        """
        Fetches stock data for the past month with daily intervals using Yahoo Finance API.
        Returns a processed DataFrame with relevant stock metrics.
        """
        data = yf.download(tickers, period=period, interval="1d").reset_index()
        date_col = data.columns[0]  # Extract the date column name
        
        # Transform data from wide to long format for better structuring
        data = data.melt(id_vars=[date_col], var_name=['Price', 'Ticker'], value_name='Value')
        data.rename(columns={date_col: 'Date'}, inplace=True)
        
        # Pivot data to structure it properly
        data = data.pivot(index=['Date', 'Ticker'], columns='Price', values='Value').reset_index()
        data.columns.name = None  # Remove multi-index column names
        
        # Rename columns to standard format
        data.rename(columns={
            'Date': 'date',
            'Ticker': 'ticker',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)
        
        # Calculate market capitalization
        data['market_cap'] = data['close'] * data['volume']
        data['date'] = pd.to_datetime(data['date']).dt.date
        data['market_cap'] = pd.to_numeric(data['market_cap'], errors='coerce')
        
        # Remove adjusted close column if present
        if 'Adj Close' in data.columns:
            data = data.drop(columns=['Adj Close'])
        
        return data
    
    @staticmethod
    def fetch_sp500_tickers():
        """
        Retrieves the list of S&P 500 tickers from Wikipedia.
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500_df = tables[0]  
        return sp500_df['Symbol'].tolist()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("StockIndexTracker").getOrCreate()
    
    print("Application started!")
    con = duckdb.connect(database="stocks.db")
    first_fetch = False

    # Fetch S&P 500 tickers
    tickers_today_500 = StockDataFetcher.fetch_sp500_tickers()
    
    if first_fetch:
        print("Running first fetch!")
        df = StockDataFetcher.fetch_stock_data(tickers_today_500, period = "1mo")
        store_in_duckdb(df)
    else:
        today = datetime.today().date()
        print("Running first fetch!")
        existing_tickers = [row[0] for row in con.execute("SELECT DISTINCT ticker FROM stock_data").fetchall()]
        missing_tickers = list(set(tickers_today_500) - set(existing_tickers))
        if missing_tickers:
            df = StockDataFetcher.fetch_stock_data(missing_tickers, period = "1mo")
            store_in_duckdb(df)
        
        # Fetch Daily
        df = StockDataFetcher.fetch_stock_data(tickers_today_500, period = "1d")
        store_in_duckdb(df)

        # # Perform index construction
        # IndexConstructor.index_construction()
        
    # Stop Spark session
    spark.stop()
