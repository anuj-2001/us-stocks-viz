import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import ssl
from datetime import datetime
import time
from duckdb_utils import store_in_duckdb  
import os

ssl._create_default_https_context = ssl._create_stdlib_context

class StockDataFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v3/reference/tickers/"
        # AAPL?apiKey=ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK
        
    def fetch_stock_data(self, ticker):
        # params = {
        #     "function": "TIME_SERIES_DAILY",
        #     "symbol": symbol,
        #     "apikey": "MF6VVSLMHZJXVHYA"   ,
        #     "outputsize": "compact"
        # }
        response = requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2025-02-01/2025-02-28?adjusted=true&sort=asc&apiKey=ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK")
        data = response.json()
        return data
    
    def fetch_sp500_tickers(self):
        """
        Fetch the list of S&P 500 tickers from Wikipedia.
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500_df = tables[0]  # The first table contains the S&P 500 tickers
        return sp500_df['Symbol'].tolist()

    def fetch_market_cap(self, ticker):
        url =  f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey=ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK"

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data['results']['market_cap']
        else:
            raise Exception(f"Failed to fetch market cap: {response.status_code}")

    def get_top_100_stocks_by_market_cap(self, spark):
        """
        Get the top 100 US stocks by market cap.
        """
        # Fetch S&P 500 tickers
        print("Fetching sp500 stocks...")
        sp500_tickers = self.fetch_sp500_tickers()
        output_file="top_100_stocks.csv"
        
        with open(output_file, "w") as file:
            file.write("Ticker, MarketCap\n")  # Add a header
        # Fetch market cap for each ticker
        market_caps = []
        i = 0
        with open(output_file, "a") as file:
            for ticker in sp500_tickers:
                if i != 5:
                    i+=1
                    market_cap = self.fetch_market_cap(ticker)
                    print(f"Processing {ticker}")
                    if market_cap != "N/A":
                        print(f"{ticker}: {market_cap}")
                        market_caps.append({"Ticker": ticker, "MarketCap": float(market_cap)})
                        # Write the data to the file immediately
                        file.write(f"{ticker}, {market_cap}\n")
                        file.flush()  # Ensure data is written to the file
                        time.sleep(12)  # Respect API rate limits
                else:
                    break

        # Sort by market cap in descending order
        sorted_market_caps = sorted(market_caps, key=lambda x: x["MarketCap"], reverse=True)

        # Select the top 100 stocks
        top_100_stocks = [stock["Ticker"] for stock in sorted_market_caps[:100]]
        
        self.save_top_100_stocks(top_100_stocks, spark)
    
    def save_top_100_stocks(self, top_100_stocks, spark, file_path="top_100_stocks.csv"):
        """
        Save the top 100 stocks to a CSV file.
        """ 
        df = spark.createDataFrame(top_100_stocks, "string").toDF("Ticker")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(file_path)
        
        print(f"Top 100 stocks saved to {file_path}")

    def load_top_100_stocks(self, spark, file_path="top_100_stocks.csv"):
        """
        Load the top 100 stocks from a CSV file.
        """
        try:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            top_100_stocks = [row.Ticker for row in df.collect()]
            return top_100_stocks
        except FileNotFoundError:
            print(f"File {file_path} not found. Returning an empty list.")
            return []

class DataProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session

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
        

        
        # Convert data into records
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
                "date": datetime.utcfromtimestamp(timestamp_ms / 1000).date(),  # Convert milliseconds to date
                "market_cap": float(entry["c"]) * int(entry["v"])  # Calculate market cap
            }
            records.append(record)
        # Create a DataFrame
        df = spark.createDataFrame(records,schema)
        return df

    def filter_top_100_stocks(self, stock_data):
        top_100_stocks = stock_data.orderBy(col("MarketCap").desc()).limit(100)
        return top_100_stocks

if __name__ == "__main__":
    spark = SparkSession.builder.appName("StockIndexTracker").getOrCreate()

    api_key = "MF6VVSLMHZJXVHYA"  
    fetcher = StockDataFetcher(api_key)

    print("App starts!")
    
    # fetcher.get_top_100_stocks_by_market_cap(spark)

    top_100_stocks = fetcher.load_top_100_stocks(spark)

    

    processor = DataProcessor(spark)
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

    # Create an empty DataFrame with the defined schema
    # stock_df = spark.createDataFrame([], schema)
    # output_file = "stock_data.csv"
    # header = not os.path.exists(output_file)  # Check if file exists to write the header only once
    # for ticker in top_100_stocks:
    #     print(f"Fetching data for {ticker}")
    #     ticker_data = fetcher.fetch_stock_data(ticker)
    #     market_cap = fetcher.fetch_market_cap(ticker)
    #     processed_df = processor.process_stock_data(ticker, ticker_data)
        
    #     processed_df.write.csv(output_file, mode="append", header=header)
    #     stock_df = stock_df.union(processed_df)
    #     header = False
    #     time.sleep(12)

    # stock_df.show()
    
    store_in_duckdb(spark,csv_file = "stock_data.csv")

    # # Combine all stock data into a single DataFrame
    # combined_df = all_stock_data[0]
    # for df in all_stock_data[1:]:
    #     combined_df = combined_df.union(df)

    # # Filter top 100 stocks by market cap
    # top_100_df = processor.filter_top_100_stocks(combined_df)

    # # Show the result
    # top_100_df.show()

    # Stop Spark session
    spark.stop()
    
    
    # _65JCzpRj9osp7TC_7Uu