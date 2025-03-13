import duckdb
from pyspark.sql import SparkSession
import pandas as pd

def store_in_duckdb(df):
    
    # Initialize DuckDB connection (in-memory)
    database_file = "stocks.db"
    con = duckdb.connect(database=database_file)

    # Store DataFrame in DuckDB (overwrite if exists)
    con.execute("CREATE TABLE IF NOT EXISTS stock_data AS SELECT * FROM df")

    print("Data successfully stored in DuckDB!")



    # Close DuckDB connection
    con.close()
