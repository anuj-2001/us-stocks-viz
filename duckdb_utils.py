import duckdb
from pyspark.sql import SparkSession
import pandas as pd

def store_in_duckdb(df):
    
    database_file = "stocks.db"
    con = duckdb.connect(database=database_file)

    con.execute("CREATE TABLE IF NOT EXISTS stock_data AS SELECT * FROM df")

    print("Data successfully stored in DuckDB!")

    con.close()
