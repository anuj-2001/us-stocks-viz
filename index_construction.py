import duckdb
import pandas as pd

class IndexConstructor:
    def __init__(self, db_connection):
        self.conn = db_connection
        self._create_tables()
        
    def _create_tables(self):
        """Create necessary tables for index tracking"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_composition (
                date DATE,
                ticker VARCHAR(10),
                weight DECIMAL(5,4),
                PRIMARY KEY (date, ticker)
            );
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_performance (
                date DATE PRIMARY KEY,
                index_value DECIMAL(12,4),
                num_constituents INTEGER,
                composition_changes INTEGER
            );
        """)

    def calculate_daily_index(self):
        """
        Calculate daily index composition and performance for the past month
        Implements equal-weighting logic for top 100 stocks by market cap
        """
        # Calculate daily rankings and store index composition
        self.conn.execute("""
            INSERT INTO index_composition
            WITH ranked_stocks AS (
                SELECT 
                    date,
                    ticker,
                    market_cap,
                    ROW_NUMBER() OVER (
                        PARTITION BY date 
                        ORDER BY market_cap DESC
                    ) as market_cap_rank
                FROM stocks
            )
            SELECT 
                date,
                ticker,
                0.01 as weight  -- Equal weighting (1/100 = 0.01)
            FROM ranked_stocks
            WHERE market_cap_rank <= 100;
        """)
        
        # Calculate index performance metrics
        self.conn.execute("""
            INSERT INTO index_performance
            SELECT 
                ic.date,
                SUM(s.close * ic.weight) as index_value,
                COUNT(DISTINCT ic.ticker) as num_constituents,
                COALESCE(changes.num_changes, 0) as composition_changes
            FROM index_composition ic
            JOIN stocks s 
                ON ic.date = s.date 
                AND ic.ticker = s.ticker
            LEFT JOIN (
                SELECT
                    current.date,
                    COUNT(*) as num_changes
                FROM index_composition current
                LEFT JOIN index_composition previous
                    ON current.ticker = previous.ticker
                    AND current.date = previous.date + INTERVAL 1 DAY
                WHERE previous.ticker IS NULL
                GROUP BY current.date
            ) changes ON ic.date = changes.date
            GROUP BY ic.date, changes.num_changes;
        """)
    
    def rebalance_index(self):
        """
        Track composition changes between trading days
        Identifies added and removed tickers between consecutive days
        """
        self.conn.execute("""
            CREATE OR REPLACE TABLE composition_changes AS
            WITH previous_composition AS (
                SELECT 
                    date + INTERVAL 1 DAY as next_date,
                    ticker
                FROM index_composition
            )
            SELECT
                curr.date,
                LIST(curr.ticker) FILTER (WHERE prev.ticker IS NULL) as added,
                LIST(prev.ticker) FILTER (WHERE curr.ticker IS NULL) as removed
            FROM index_composition curr
            FULL OUTER JOIN previous_composition prev
                ON curr.date = prev.next_date
                AND curr.ticker = prev.ticker
            GROUP BY curr.date;
        """)
    
    def get_index_performance(self):
        """Retrieve complete index performance data"""
        return self.conn.execute("""
            SELECT 
                date,
                index_value,
                LAG(index_value) OVER (ORDER BY date) as previous_value,
                (index_value - LAG(index_value) OVER (ORDER BY date)) / 
                LAG(index_value) OVER (ORDER BY date) as daily_return,
                composition_changes
            FROM index_performance
            ORDER BY date;
        """).df()
