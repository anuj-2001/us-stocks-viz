import duckdb
from datetime import datetime, timedelta

class EqualWeightedIndex:
    def __init__(self, db_path="stocks.db"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.conn.execute("DROP TABLE IF EXISTS index_composition_log; DROP TABLE IF EXISTS index_performance;")
        self._create_log_table()
        self._create_performance_table()

    def _create_log_table(self):
        """Tracks additions/removals in the index."""
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS index_composition_log (
            date DATE,
            ticker VARCHAR,
            action VARCHAR
        );
        """)

    def _create_performance_table(self):
        """Stores daily index performance."""
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS index_performance (
            date DATE PRIMARY KEY,
            index_value FLOAT
        );
        """)

    def get_dates(self):
        """Returns sorted list of unique dates from stock data."""
        dates = self.conn.execute("SELECT DISTINCT date FROM stock_data ORDER BY date;").fetchall()
        return [row[0] for row in dates]

    def fetch_top_100_stocks(self, date):
        """Gets the top 100 stocks by market cap for a given date."""
        return self.conn.execute(f"""
        SELECT ticker, market_cap
        FROM stock_data
        WHERE date = '{date}'
        ORDER BY market_cap DESC
        LIMIT 100;
        """).fetchall()

    def calculate_index_performance(self, date):
        """Computes the average closing price of the top 100 stocks."""
        top_100_stocks = self.fetch_top_100_stocks(date)
        if not top_100_stocks:
            return None

        tickers = [f"'{stock[0]}'" for stock in top_100_stocks]
        query = f"""
        SELECT AVG(close) AS index_value
        FROM stock_data
        WHERE date = '{date}' AND ticker IN ({','.join(tickers)});
        """
        return self.conn.execute(query).fetchone()[0]

    def rebalance_index(self, date):
        """Logs changes in index composition if stocks are added or removed."""
        current_tickers = {stock[0] for stock in self.fetch_top_100_stocks(date)}
        previous_tickers = {stock[0] for stock in self.fetch_top_100_stocks(date - timedelta(days=1))}

        additions = current_tickers - previous_tickers
        removals = previous_tickers - current_tickers

        if additions or removals:
            for ticker in additions:
                self.conn.execute(f"""
                INSERT INTO index_composition_log (date, ticker, action)
                VALUES ('{date}', '{ticker}', 'added');
                """)
            for ticker in removals:
                self.conn.execute(f"""
                INSERT INTO index_composition_log (date, ticker, action)
                VALUES ('{date}', '{ticker}', 'removed');
                """)

            print(f"Index updated on {date}: {len(additions)} added, {len(removals)} removed.")
        else:
            print(f"No changes in index composition on {date}.")

    def track_index_performance(self):
        """Tracks performance over the past month."""
        dates = self.get_dates()
        if not dates:
            raise ValueError("No stock data available.")

        start_date = dates[-1] - timedelta(days=28)
        past_month_dates = [date for date in dates if start_date <= date <= dates[-1]]

        performance_data = {}
        for date in past_month_dates:
            index_value = self.calculate_index_performance(date)
            if index_value is not None:
                self.conn.execute(f"""
                INSERT OR REPLACE INTO index_performance (date, index_value)
                VALUES ('{date}', {index_value});
                """)
                performance_data[date] = index_value
                self.rebalance_index(date)

        return performance_data

    def close_connection(self):
        self.conn.close()


if __name__ == "__main__":
    index = EqualWeightedIndex()
    performance = index.track_index_performance()
    print("Index Performance:", performance)
    index.close_connection()
