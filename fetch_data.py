import yfinance as yf
import sqlite3

def fetch_stock_data(ticker_list, start_date, end_date):
    data = yf.download(tickers=ticker_list, start=start_date, end=end_date)
    return data

def initialize_database():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE stocks (ticker TEXT PRIMARY KEY, name TEXT, market_cap REAL);")
    cursor.execute("CREATE TABLE prices (ticker TEXT, date DATE, price REAL, FOREIGN KEY(ticker) REFERENCES stocks(ticker));")
    conn.commit()
    return conn

if __name__ == '__main__':
    # Example usage
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    data = fetch_stock_data(tickers, "2023-12-01", "2023-12-31")
    print(data)
