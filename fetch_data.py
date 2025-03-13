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
from index_construction import IndexConstructor
import yfinance as yf

# Ensure secure HTTPS context
ssl._create_default_https_context = ssl._create_stdlib_context

class StockDataFetcher:
    def __init__(self):
        pass  # Constructor is currently empty
        
    @staticmethod
    def fetch_stock_data(tickers):
        """
        Fetches stock data for the past month with daily intervals using Yahoo Finance API.
        Returns a processed DataFrame with relevant stock metrics.
        """
        data = yf.download(tickers, period="1mo", interval="1d").reset_index()
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
    def fetch_daily_data(tickers):
        """
        Fetches daily stock data for given tickers.
        """
        data = yf.download(tickers, period="1d", interval="1d")
        data['Date'] = pd.to_datetime(data['Date']).dt.date
        
        # Rename columns for consistency
        data.rename(columns={
            'Date': 'date',
            'Symbol': 'ticker',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)
    
    @staticmethod
    def fetch_sp500_tickers():
        """
        Retrieves the list of S&P 500 tickers from Wikipedia.
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500_df = tables[0]  
        return sp500_df['Symbol'].tolist()

class DataProcessor:
    def __init__(self, spark_session):
        """
        Initializes the DataProcessor with a Spark session.
        """
        self.spark = spark_session
        self.fetcher = StockDataFetcher()
    
    def process_stock_data(self, ticker, stock_data):
        """
        Converts raw stock data into a structured Spark DataFrame with the necessary schema.
        """
        schema = StructType([
            StructField("ticker", StringType(), nullable=False),
            StructField("open", FloatType(), nullable=False),
            StructField("high", FloatType(), nullable=False),
            StructField("low", FloatType(), nullable=False),
            StructField("close", FloatType(), nullable=False),
            StructField("volume", FloatType(), nullable=False),
            StructField("market_cap", FloatType(), nullable=False),
            StructField("date", DateType(), nullable=False),
        ])
        
        # Convert raw JSON data into a list of records
        records = []
        for entry in stock_data["results"]:
            timestamp_ms = entry["t"]
            record = {
                "ticker": ticker,
                "open": float(entry["o"]),
                "high": float(entry["h"]),
                "low": float(entry["l"]),
                "close": float(entry["c"]),
                "volume": float(entry["v"]),
                "market_cap": float(entry["c"]) * int(entry["v"]),
                "date": datetime.utcfromtimestamp(timestamp_ms / 1000).date()
            }
            records.append(record)
        
        return self.spark.createDataFrame(records, schema)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("StockIndexTracker").getOrCreate()
    
    print("Application started!")
    
    # Fetch S&P 500 tickers
    tickers = StockDataFetcher.fetch_sp500_tickers()
    today = datetime.today().date()
    month_ago = today - timedelta(days=31)
    
    # Fetch and process stock data
    df = StockDataFetcher.fetch_stock_data(tickers)
    print(df)
    
    # Connect to DuckDB and store data
    con = duckdb.connect(database="stocks.db")
    con.execute("DROP TABLE IF EXISTS stock_data")
    store_in_duckdb(df)
    
    # Perform index construction
    IndexConstructor.index_construction()
    
    # Stop Spark session
    spark.stop()
