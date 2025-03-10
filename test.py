import requests
import time
from polygon import RESTClient

api_key = "ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK"
client = RESTClient(api_key)

def fetch_daily_data(ticker, date):
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}"
        params = {
            "apiKey": "ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK"
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Failed to fetch data for {ticker}: {response.status_code}")
        
def fetch_top_stocks(ticker, limit=100):
        url =  f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey=ASaIGjQ3FG9CA1BppBRjTguyRwtKAfGK"

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data['results']
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")
        time.sleep(12)
        
def fetch_stock_data(ticker):
    data = client.stocks_equities_daily_open_close(ticker, "2023-09-01", "2023-09-30")
    return {
        "ticker": ticker,
        "date": data.from_,
        "open": data.open,
        "close": data.close,
        "market_cap": data.market_cap  # Assuming market cap is available
    }

def fetch_stock_data(symbol):
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": "P86DB3846ZQPMKS2" ,
            "outputsize": "compact"
        }
        response = requests.get("https://www.alphavantage.co/query", params=params)
        data = response.json()
        return data

# List of US stock tickers (example)
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

print(fetch_top_stocks("AAPL"))

# Fetch data for all tickers
# stock_data = [fetch_stock_data(ticker) for ticker in tickers]
# print(stock_data)

# print(fetch_top_stocks("AAPL"))