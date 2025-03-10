import duckdb
from pyspark.sql import SparkSession
import pandas as pd

def store_in_duckdb(spark, csv_file):
    """
    Reads the CSV file into PySpark, then stores it in an in-memory DuckDB database.
    """

    # Read the CSV file into a PySpark DataFrame
    stocks_df = spark.read.csv(csv_file, header=True, inferSchema=True)
    
    df = stocks_df.toPandas()
    
    print(df)
    # Initialize DuckDB connection (in-memory)
    con = duckdb.connect(database=":memory:")

    # Store DataFrame in DuckDB (overwrite if exists)
    con.execute("CREATE TABLE IF NOT EXISTS stock_data AS SELECT * FROM df")

    print("Data successfully stored in DuckDB!")

    # Query the stored data for verification
    results = con.execute("SELECT * FROM stock_data").fetchall()
    print("Sample Data from DuckDB:", results)

    # Close DuckDB connection
    con.close()
