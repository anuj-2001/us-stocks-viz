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

ssl._create_default_https_context = ssl._create_stdlib_context

class StockDataFetcher:
    def __init__(self):
        # TODO document why this method is empty
        pass
        
    def fetch_stock_data(tickers):
        data = yf.download(tickers, period="1mo", interval="1d")
        data = data.reset_index()
        date_col = data.columns[0] 

        data = data.melt(id_vars=[date_col], var_name=['Price', 'Ticker'], value_name='Value')


        data.rename(columns={date_col: 'Date'}, inplace=True)

        data = data.pivot(index=['Date', 'Ticker'], columns='Price', values='Value').reset_index()
                
                
        # Drop the 'Price' column from the multi-index column names
        data.columns.name = None  # Remove hierarchical column name
        data = data.reset_index(drop=True)  # Reset index
        data.rename(columns={
            'Date': 'date',
            'Ticker': 'ticker',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)
        data['market_cap'] = data['close'] * data['volume']

        data['date'] = pd.to_datetime(data['date']).dt.date

        data['market_cap'] = pd.to_numeric(data['market_cap'], errors='coerce')
        print(data)
        
        data = data.drop(columns = ['Adj Close'])
        
        return data
    
    def fetch_daily_data(self, tickers):
        data = yf.download(tickers, period="1d", interval="1d")
        data['Date'] = pd.to_datetime(data['Date']).dt.date



        data.rename(columns={
            'Date': 'date',
            'Symbol': 'ticker',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)
        
        
    
    def calculate_market_cap(data):
        """
        Calculate market cap for MultiIndex columns structure
        Returns DataFrame with date index and ticker columns
        """
        # Extract closing prices and volumes
        close_prices = data.xs('Close', level='Price', axis=1)
        volumes = data.xs('Volume', level='Price', axis=1)
        
        # Calculate market cap for each ticker
        market_caps = close_prices * volumes
        
        # Rename columns for clarity
        market_caps.columns = pd.MultiIndex.from_tuples(
            [('Market Cap', col) for col in market_caps.columns],
            names=['Metric', 'Ticker']
        )
        
        # Combine with original data
        return pd.concat([data, market_caps], axis=1)

    
    def fetch_sp500_tickers():
        """
        Fetch the list of S&P 500 tickers from Wikipedia.
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500_df = tables[0]  
        return sp500_df['Symbol'].tolist()
    

class DataProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.fetcher = StockDataFetcher()

    def process_stock_data(self, ticker, stock_data):
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
        df = spark.createDataFrame(records,schema)
        
        return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("StockIndexTracker").getOrCreate()

    fetcher = StockDataFetcher()

    print("App starts!")
    
    tickers = StockDataFetcher.fetch_sp500_tickers()
    
    today = datetime.today().date()
    print(today)
    month_ago = today - timedelta(days=31)
    
    df = StockDataFetcher.fetch_stock_data(tickers)
    print(df)
    
    
    con = duckdb.connect(database="stocks.db")

    
    con.execute("DROP TABLE IF EXISTS stock_data")
    store_in_duckdb(df)
    
    IndexConstructor.index_construction()

    # con.close()
    spark.stop()