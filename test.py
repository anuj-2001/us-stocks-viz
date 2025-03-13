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

import yfinance as yf
dat = yf.Ticker("MSFT")
dat = yf.Ticker("MSFT")
print(f"Info: {dat.info}")
print(f"Calendar: {dat.calendar}")
print(f"Price Targets: {dat.analyst_price_targets}")
print(f"Quaterly: {dat.quarterly_income_stmt}")
print(f"History: {dat.history(period='1mo')}")
print(f"Option chain: {dat.option_chain(dat.options[0]).calls}")
data = yf.download(tickers, period="1mo", interval="1d")
print(data)
print(data.columns)

# Reset index to bring the date column into the DataFrame
data = data.reset_index()

# Ensure the correct date column name (check what it's called after reset_index)
date_col = data.columns[0]  # This will pick the first column (which should be the date)

# Melt the DataFrame to bring multi-index columns into rows
df_melted = data.melt(id_vars=[date_col], var_name=['Price', 'Ticker'], value_name='Value')

# Rename the date column
df_melted.rename(columns={date_col: 'Date'}, inplace=True)

# Pivot to get the required format
df_final = df_melted.pivot(index=['Date', 'Ticker'], columns='Price', values='Value').reset_index()

# Display the transformed DataFrame
print(df_final.columns)
print(type(df_final['Ticker'][0]))

# print(data['Close']['AAPL']['2025-02-13'])
# print(fetch_top_stocks("AAPL"))

# Fetch data for all tickers
# stock_data = [fetch_stock_data(ticker) for ticker in tickers]
# print(stock_data)

# print(fetch_top_stocks("AAPL"))