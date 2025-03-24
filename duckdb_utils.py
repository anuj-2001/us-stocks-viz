import duckdb
from pyspark.sql import SparkSession
import pandas as pd


def store_in_duckdb(df, table_name="stock_data", database_file="stocks.db"):
    """
    Stores a DataFrame into a DuckDB table, handling duplicates.
    - Creates the table if it does not exist.
    - Inserts new records while avoiding duplicates.
    - Updates existing records if they already exist.
    """
    
    con = duckdb.connect(database=database_file)
    # con.execute("DROP TABLE IF EXISTS stock_data")
    # Ensure table exists (Creates it only if not present)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            ticker TEXT,
            open FLOAT,
            high FLOAT, 
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            market_cap FLOAT,
            PRIMARY KEY (date, ticker)  -- Prevents duplicate inserts
        )
    """)

    # Create a temporary table to insert new data
    con.execute("CREATE TEMP TABLE temp_data AS SELECT * FROM df")

    # Merge new data with existing table (UPSERT operation)
    con.execute(f"""
        INSERT INTO {table_name}
        SELECT * FROM temp_data
        ON CONFLICT (date, ticker) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            market_cap = excluded.market_cap
    """)
    
    # print(con.execute("select * from stock_data;").fetchall())
    print(f"Data successfully stored in {table_name}")

    con.close()