import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

pd.options.mode.chained_assignment = None  

class EqualWeightIndex:
    def __init__(self, db_path='stocks.db'):
        self.conn = duckdb.connect(db_path)
        self._create_tables()
        self.base_index_value = 1000
        self.initial_notional = 1e6
        self.current_date = None

    def _create_tables(self):
        """Create all required database tables"""
        # Index management tables
        self.conn.execute("""
        CREATE SCHEMA IF NOT EXISTS index;
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS index.constituents (
            date DATE,
            ticker TEXT,
            weight FLOAT,
            shares_held FLOAT,
            price FLOAT,
            notional FLOAT,
            PRIMARY KEY (date, ticker)
        );
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS index.performance (
            date DATE PRIMARY KEY,
            index_value FLOAT,
            total_notional FLOAT,
            daily_return FLOAT
        );
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS index.rebalancing_log (
            date DATE,
            ticker TEXT,
            action_type TEXT,
            shares_change FLOAT,
            notional_change FLOAT,
            price FLOAT
        );
        """)

    def get_dates(self):
        """Returns sorted list of unique dates from stock data."""
        dates = self.conn.execute("SELECT DISTINCT date FROM stock_data ORDER BY date;").fetchall()
        return [row[0] for row in dates]
    
    def fetch_top_100_stocks(self, date):
        """Retrieve top 100 stocks by market cap for a given date"""
        return self.conn.execute(f"""
            SELECT ticker, close as price, market_cap
            FROM stock_data
            WHERE date = '{date}'
            ORDER BY market_cap DESC
            LIMIT 100;
        """).fetchdf()
        
    def fetch_current_price(self, date):
        """Retrieve stocks by market cap for a given date"""
        return self.conn.execute(f"""
            SELECT ticker, close as price, market_cap
            FROM stock_data
            WHERE date = '{date}'
            ORDER BY market_cap DESC
        """).fetchdf()
    

    def calculate_index(self, start_date, end_date):
        """Main calculation loop for date range"""
        current_date = start_date
        dates = self.get_dates()
        prev_date = start_date
        while current_date <= end_date:
            print(current_date)
            if current_date in dates:
                self._process_day(current_date, prev_date)
                prev_date = current_date
            current_date += timedelta(days=1)

    def _process_day(self, date, prev_date):
        """Process a single trading day"""
        self.current_date = date
        top100 = self.fetch_top_100_stocks(date)
        current_df = self.fetch_current_price(date)
        
        if self._is_first_day():
            self._initialize_index(top100)
        else:
            self._rebalance_index(top100, current_df, prev_date)
            
        self._update_performance(prev_date)
        
        self._cleanup()

    def _is_first_day(self):
        """Check if this is the first day of index calculation"""
        return not self.conn.execute("""
            SELECT EXISTS(SELECT 1 FROM index.performance)
        """).fetchone()[0]

    def _initialize_index(self, constituents):
        """Initialize the index on first day"""
        num_stocks = len(constituents)
        weight = 1 / num_stocks
        total_notional = self.initial_notional
        constituents['shares_held'] = (total_notional * weight) / constituents['price']
        constituents['weight'] = weight
        constituents['notional'] = constituents['shares_held'] * constituents['price']
        constituents['date'] = self.current_date

        # Insert initial constituents
        self.conn.register('temp_constituents', constituents)
        self.conn.execute("""
            INSERT INTO index.constituents
            SELECT date, ticker, weight, shares_held, price, notional
            FROM temp_constituents
        """)

        # Initialize performance
        # self.conn.execute(f"""
        #     INSERT INTO index.performance (date, index_value, total_notional, daily_return)
        #     VALUES ('{self.current_date}', {self.base_index_value}, {self.initial_notional}, 0.0)
        # """)

    def _rebalance_index(self, new_constituents, current_df, prev_date):
        """Full rebalancing logic with constituent changes"""
        # Get previous day's holdings
        prev_holdings = self.conn.execute(f"""
            SELECT ticker, shares_held, price 
            FROM index.constituents 
            WHERE date = '{prev_date}'
        """).fetchdf()

        # Calculate current value of previous holdings
        
        current_prices = current_df.set_index('ticker')['price']
        prev_holdings['current_price'] = prev_holdings['ticker'].map(current_prices)
        prev_holdings['current_value'] = prev_holdings['shares_held'] * prev_holdings['current_price']
        total_value = prev_holdings['current_value'].sum()
        
        
        
        # Identify constituent changes
        current_tickers = set(new_constituents['ticker'])
        prev_tickers = set(prev_holdings['ticker'])
        
        added_tickers = current_tickers - prev_tickers
        removed_tickers = prev_tickers - current_tickers
        common_tickers = current_tickers & prev_tickers

        # Remove dropped constituents
        
        if removed_tickers:
            self.conn.execute(f"""
                DELETE FROM index.constituents 
                WHERE date = '{self.current_date}'
                AND ticker IN ({','.join([f"'{t}'" for t in removed_tickers])})
            """)
            
            # Log removals
            removed = prev_holdings[prev_holdings['ticker'].isin(removed_tickers)]
            for _, row in removed.iterrows():
                self.conn.execute(f"""
                    INSERT INTO index.rebalancing_log
                    VALUES (
                        '{self.current_date}',
                        '{row['ticker']}',
                        'REMOVE',
                        {-row['shares_held']},
                        {-row['current_value']},
                        {row['current_price']}
                    )
                """)

        # Add new constituents
        new_stocks = new_constituents[new_constituents['ticker'].isin(added_tickers)]
        if not new_stocks.empty:
            weight = 1 / len(new_constituents)
            total_notional = total_value
            new_stocks.loc[:, 'shares_held'] = (total_notional * weight) / new_stocks['price']
            new_stocks['weight'] = weight
            new_stocks['notional'] = new_stocks['shares_held'] * new_stocks['price']
            new_stocks['date'] = self.current_date

            self.conn.register('temp_new', new_stocks)
            self.conn.execute("""
                INSERT INTO index.constituents
                SELECT date, ticker, weight, shares_held, price, notional
                FROM temp_new
            """)

            # Log additions
            for _, row in new_stocks.iterrows():
                self.conn.execute(f"""
                    INSERT INTO index.rebalancing_log
                    VALUES (
                        '{self.current_date}',
                        '{row['ticker']}',
                        'ADD',
                        {row['shares_held']},
                        {row['notional']},
                        {row['price']}
                    )
                """)

        # Rebalance existing constituents
        target_weight = 1 / len(new_constituents)
        target_notional = total_value * target_weight
        
        print(f"Target notional: {target_notional} and total notional : {total_value}")
        
        existing = new_constituents[new_constituents['ticker'].isin(common_tickers)]
        existing['current_value'] = existing['ticker'].map(
            prev_holdings.set_index('ticker')['current_value']
        )
        existing['target_shares'] = target_notional / existing['price']
        existing['shares_change'] = existing['target_shares'] - existing['ticker'].map(
            prev_holdings.set_index('ticker')['shares_held']
        )
        existing['notional_change'] = existing['shares_change'] * existing['price']
        existing['date'] = self.current_date

        # Update existing constituents
        update_cols = ['date', 'ticker', 'weight', 'shares_held', 'price', 'notional']
        existing['weight'] = target_weight
        existing.loc[:, 'shares_held'] = existing['target_shares']
        existing['notional'] = target_notional
        
        print(f"Existing stocks:\n {existing}")
        
        self.conn.register('temp_existing', existing[update_cols])
        self.conn.execute("""
            INSERT INTO index.constituents
            SELECT * FROM temp_existing
        """)

        # Log rebalancing trades
        for _, row in existing.iterrows():
            self.conn.execute(f"""
                INSERT INTO index.rebalancing_log
                VALUES (
                    '{self.current_date}',
                    '{row['ticker']}',
                    'REBALANCE',
                    {row['shares_change']},
                    {row['notional_change']},
                    {row['price']}
                )
            """)

    def _update_performance(self, prev_date):
        """Update performance metrics and daily returns"""
        # Get current total notional
        total_notional = self.conn.execute(f"""
            SELECT SUM(notional) 
            FROM index.constituents 
            WHERE date = '{self.current_date}'
        """).fetchone()[0]

        # Get previous performance
        prev_perf = self.conn.execute(f"""
            SELECT index_value, total_notional 
            FROM index.performance 
            WHERE date = '{prev_date}'
        """).fetchone()
        print(f"Previous performance : {prev_perf}")
        if prev_perf:
            prev_value = prev_perf[0]
            daily_return = (total_notional / prev_perf[1]) - 1
            index_value = prev_value * (1 + daily_return)
        else:
            daily_return = 0.0
            index_value = self.base_index_value
        print(f"Daily Return : {daily_return} and index value: {index_value}")
        # Update performance table
        # con = duckdb.connect(database="stocks.db")
        # print(con.execute("select * from index.performance;").fetchall())
        self.conn.execute(f"""
            INSERT INTO index.performance (date, index_value, total_notional, daily_return)
            VALUES (
                '{self.current_date}',
                {index_value},
                {total_notional},
                {daily_return}
            )
        """)

    def _cleanup(self):
        """Clean up temporary resources"""
        self.conn.unregister('temp_constituents')
        self.conn.unregister('temp_new')
        self.conn.unregister('temp_existing')


# Example Usage
if __name__ == "__main__":
    # Initialize index and generate test data
    index = EqualWeightIndex()
    
    # Generate 30 days of test data
    end_date = datetime(2025, 3, 24)
    start_date = datetime(2025, 2, 25)
    
    # Run index calculation
    index.calculate_index(start_date.date(), end_date.date())
    
    # Query results
    print("Performance Data:")
    print(index.conn.execute("SELECT * FROM index.performance").fetchdf())
    
    print("\nRebalancing Log:")
    print(index.conn.execute("SELECT * FROM index.rebalancing_log LIMIT 10").fetchdf())
    
    print("\nCurrent Constituents:")
    print(index.conn.execute(f"""
        SELECT * FROM index.constituents 
        WHERE date = '{end_date.date()}'
    """).fetchdf())