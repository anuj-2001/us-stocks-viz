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

ssl._create_default_https_context = ssl._create_stdlib_context

class StockDataFetcher:
    def __init__(self):
        self.base_url = "https://api.polygon.io/v3/reference/tickers/"
        # AAPL?apiKey=ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK
        
    def fetch_stock_data(self, ticker, start_dt, end_dt):
        response = requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_dt}/{end_dt}?adjusted=true&sort=asc&apiKey=ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK")
        data = response.json()
        return data
    
    def fetch_sp500_tickers(self):
        """
        Fetch the list of S&P 500 tickers from Wikipedia.
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500_df = tables[0]  
        return sp500_df['Symbol'].tolist()

    def fetch_market_cap(self, ticker):
        url =  f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey=ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK"

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            print(data)  
            
            # Ensure 'results' exist and contains 'market_cap'
            if 'results' in data and 'market_cap' in data['results']:
                return data['results']['market_cap']
            else:
                print("Market cap not found, skipping this stock.")
                return None  # Return None or handle appropriately
        else:
            raise Exception(f"Failed to fetch market cap: {response.status_code}")

    def get_top_100_stocks_by_market_cap(self):
        """
        Get the top 100 US stocks by market cap.
        """
        print("Fetching sp500 stocks...")
        sp500_tickers = self.fetch_sp500_tickers()
        output_file="top_100_stocks.csv"
        
        with open(output_file, "w") as file:
            file.write("Ticker, MarketCap\n")  
            market_caps = []
            with open(output_file, "a") as file:
                for i in range(443,len(sp500_tickers)):
                    print(i)
                    market_cap = self.fetch_market_cap(sp500_tickers[i])
                    print(f"Processing {sp500_tickers[i]}")
                    if market_cap != "N/A" and market_cap!=None:
                        print(f"{sp500_tickers[i]}: {market_cap}")
                        market_caps.append({"Ticker": sp500_tickers[i], "MarketCap": float(market_cap)})
                        file.write(f"{sp500_tickers[i]}, {market_cap}\n")
                        file.flush() 
                        time.sleep(12) 
        
        df = pd.read_csv(output_file)
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
        df_sorted = df.sort_values(by="market_cap", ascending=False)
        df_sorted.to_csv(output_file, index=False)
        
    
    # def save_top_100_stocks(self, top_100_stocks, spark, file_path="top_100_stocks.csv"):
    #     """
    #     Save the top 100 stocks to a CSV file.
    #     """ 
    #     df = spark.createDataFrame(top_100_stocks, "string").toDF("Ticker")
    #     df.coalesce(1).write.mode("overwrite").option("header", "true").csv(file_path)
        
    #     print(f"Top 100 stocks saved to {file_path}")

    def load_top_100_stocks(self, spark, file_path="top_100_stocks.csv"):
        """
        Load the top 100 stocks from a CSV file.
        """
        try:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            top_100_stocks = [row.Ticker for row in df.limit(100).collect()]
            return top_100_stocks
        except FileNotFoundError:
            print(f"File {file_path} not found. Returning an empty list.")
            return []

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
            StructField("market_cap_api", FloatType(), nullable=False),
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
                "market_cap_api":float(self.fetcher.fetch_market_cap(ticker)),
                "date": datetime.utcfromtimestamp(timestamp_ms / 1000).date()
            }
            records.append(record)
        df = spark.createDataFrame(records,schema)
        return df

    def filter_top_100_stocks(self, stock_data):
        top_100_stocks = stock_data.orderBy(col("MarketCap").desc()).limit(100)
        return top_100_stocks

if __name__ == "__main__":
    spark = SparkSession.builder.appName("StockIndexTracker").getOrCreate()

    api_key = "MF6VVSLMHZJXVHYA"  
    fetcher = StockDataFetcher()

    print("App starts!")
    
    fetcher.get_top_100_stocks_by_market_cap()

    top_100_stocks = fetcher.load_top_100_stocks(spark)
    
    con = duckdb.connect(database="stocks.db")
    tickers_in_db = []
    tickers_in_db = con.execute("SELECT distinct ticker from stock_data").fetchall()
    tickers_in_db = [t[0] for t in tickers_in_db]
    print(tickers_in_db)
    


    

    processor = DataProcessor(spark)
    schema = StructType([
        StructField("ticker", StringType(), nullable=False),
        StructField("open", FloatType(), nullable=False),
        StructField("high", FloatType(), nullable=False),
        StructField("low", FloatType(), nullable=False),
        StructField("close", FloatType(), nullable=False),
        StructField("volume", FloatType(), nullable=False),
        StructField("market_cap", FloatType(), nullable=False),
        StructField("market_cap_api", FloatType(), nullable=False),
        StructField("date", DateType(), nullable=False),
    ])

    stock_df = spark.createDataFrame([], schema)
    output_file = "stock_data.csv"
    header = not os.path.exists(output_file) 
    
    today = datetime.today().date() 
    month_ago = today - timedelta(days=31)

    
    for ticker in top_100_stocks:
        if ticker in tickers_in_db:
            print(f"Fetching today's data for {ticker}")
            ticker_data = fetcher.fetch_stock_data(ticker, today, today)
            # market_cap = fetcher.fetch_market_cap(ticker)
            processed_df = processor.process_stock_data(ticker, ticker_data)
            
            processed_df.write.csv(output_file, mode="append", header=header)
            stock_df = stock_df.union(processed_df)
            header = False
            time.sleep(12)
        else:
            print(f"Fetching historical data for {ticker}")
            ticker_data = fetcher.fetch_stock_data(ticker, month_ago, today)
            # market_cap = fetcher.fetch_market_cap(ticker)
            processed_df = processor.process_stock_data(ticker, ticker_data)
            
            processed_df.write.csv(output_file, mode="append", header=header)
            stock_df = stock_df.union(processed_df)
            header = False
            time.sleep(12)
            

    stock_df.show()
    
    store_in_duckdb(spark,csv_file = "stock_data.csv")

    con.close()
    spark.stop()