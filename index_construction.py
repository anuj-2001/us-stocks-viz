import duckdb
from datetime import datetime, timedelta

class EqualWeightedIndex:
    def __init__(self, db_path="stocks.db"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.conn.execute("DROP TABLE index_composition_log;DROP TABLE index_performance; ")
        self._create_log_table()
        self._create_performance_table()

    def _create_log_table(self):
        """
        Create a log table to track changes in index composition.
        """
        query = """
        CREATE TABLE IF NOT EXISTS index_composition_log (
            date DATE,
            ticker VARCHAR,
            action VARCHAR
        );
        """
        self.conn.execute(query)

    def get_dates(self):
        """
        Retrieve sorted list of unique dates from stock_data.
        """
        query = "SELECT DISTINCT date FROM stock_data ORDER BY date;"
        dates = self.conn.execute(query).fetchall()
        return [row[0] for row in dates]

    def fetch_top_100_stocks(self, date):
        """
        Fetch the top 100 stocks by market cap for a given date.
        """
        query = f"""
        SELECT ticker, market_cap
        FROM stock_data
        WHERE date = '{date}'
        ORDER BY market_cap DESC
        LIMIT 100;
        """
        return self.conn.execute(query).fetchall()

    def calculate_index_performance(self, date):
        """
        Calculate the index performance for a given date using closing prices.
        """
        # Fetch the top 100 stocks for the given date
        top_100_stocks = self.fetch_top_100_stocks(date)
        tickers = [stock[0] for stock in top_100_stocks]

        # Calculate the equal-weighted index performance
        query = f"""
        SELECT AVG(close) as index_value
        FROM stock_data
        WHERE date = '{date}' AND ticker IN ({','.join([f"'{ticker}'" for ticker in tickers])});
        """
        index_value = self.conn.execute(query).fetchone()[0]
        return index_value

    def rebalance_index(self, date):
        """
        Rebalance the index composition if the top 100 stocks change.
        Log changes in the index_composition_log table.
        """
        # Fetch the current top 100 stocks
        current_top_100 = self.fetch_top_100_stocks(date)
        current_tickers = [stock[0] for stock in current_top_100]

        # Fetch the previous day's top 100 stocks
        previous_date = date - timedelta(days=1)
        previous_top_100 = self.fetch_top_100_stocks(previous_date)
        previous_tickers = [stock[0] for stock in previous_top_100]

        # Check for additions to the index
        additions = set(current_tickers) - set(previous_tickers)
        for ticker in additions:
            self.conn.execute(f"""
            INSERT INTO index_composition_log (date, ticker, action)
            VALUES ('{date}', '{ticker}', 'added');
            """)

        # Check for removals from the index
        removals = set(previous_tickers) - set(current_tickers)
        for ticker in removals:
            self.conn.execute(f"""
            INSERT INTO index_composition_log (date, ticker, action)
            VALUES ('{date}', '{ticker}', 'removed');
            """)

        if additions or removals:
            print(f"Index composition changed on {date}. Rebalancing...")
        else:
            print(f"No change in index composition on {date}.")
    
    def _create_performance_table(self):
        """
        Create a table to store index performance data.
        """
        query = """
        CREATE TABLE IF NOT EXISTS index_performance (
            date DATE PRIMARY KEY,
            index_value FLOAT
        );
        """
        self.conn.execute(query)
        
    def track_index_performance(self):
        """
        Track the index composition and performance over the past month.
        """
        # Get all unique dates from the database
        dates = self.get_dates()
        if not dates:
            raise ValueError("No dates found in the database.")

        # Determine the start and end dates for the past month
        end_date = dates[-1]  # Most recent date
        start_date = end_date - timedelta(days=28)

        # Filter dates to include only those within the past month
        past_month_dates = [date for date in dates if start_date <= date <= end_date]

        # Track performance for each date in the past month
        performance_data = {}
        for date in past_month_dates:
            # Calculate index performance for the current date
            index_value = self.calculate_index_performance(date)
            performance_data[date] = index_value
            
            # Insert performance data into the index_performance table
            self.conn.execute(f"""
            INSERT OR REPLACE INTO index_performance (date, index_value)
            VALUES ('{date}', {index_value});
            """)

            # Rebalance the index if necessary
            self.rebalance_index(date)

        return performance_data

    def close_connection(self):
        """
        Close the database connection.
        """
        self.conn.close()


# Example usage
if __name__ == "__main__":
    # Initialize the index
    index = EqualWeightedIndex()

    # Track index performance over the past month
    performance = index.track_index_performance()
    print("Index Performance:", performance)

    # Close the database connection
    index.close_connection()