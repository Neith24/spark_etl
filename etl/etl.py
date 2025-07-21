import os
import sys

# Fix  PySpark python3 not found error
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Set  Hadoop home for Window
os.environ['HADOOP_HOME'] = 'C:\\winutils'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def calculate_longest_streak(df):
    def longest_streak_udf(pdf):
        import pandas as pd
        
        # Get unique customer ID 
        cust_id = pdf["custId"].iloc[0]
        
        # Get unique dates and sort 
        dates = sorted(set(pdf["transactionDate"].dropna()))
        
        if not dates:
            max_streak = 0
        else:
            max_streak = cur_streak = 1
            for i in range(1, len(dates)):
                if (dates[i] - dates[i-1]).days == 1:
                    cur_streak += 1
                    max_streak = max(max_streak, cur_streak)
                else:
                    cur_streak = 1
        
        return pd.DataFrame({
            "custId": [cust_id],
            "longest_streak": [max_streak]
        })
    
    # Define  return schema
    return_schema = StructType([
        StructField("custId", StringType(), True),
        StructField("longest_streak", IntegerType(), True)
    ])
    
    # make sure that transactionDate is correctly formatted
    df = df.withColumn("transactionDate", F.to_date("transactionDate"))
    
    # Use applyInPandas 
    return df.groupBy("custId").applyInPandas(longest_streak_udf, return_schema)


def run_etl(source_path, destination_path=None, db_url=None, db_table=None):
    # use Spark with JDBC driver
    spark = SparkSession.builder \
        .appName("ETL Pipeline") \
        .config("spark.jars", "/tmp/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/tmp/postgresql-42.6.0.jar") \
        .config("spark.executor.extraClassPath", "/tmp/postgresql-42.6.0.jar") \
        .getOrCreate()
    
    df = spark.read.option("header", True).option("delimiter", "|").csv(source_path)
    df = df.withColumn("unitsSold", F.col("unitsSold").cast("int"))
    df = df.withColumn("transactionDate", F.to_date("transactionDate", "yyyy-MM-dd"))
    
    # Favourite product
    prod_agg = (
        df.groupBy("custId", "productSold")
        .agg(F.sum("unitsSold").alias("total_units"))
    )
    win = Window.partitionBy("custId").orderBy(F.desc("total_units"), F.asc("productSold"))
    
    fav_prod = (
        prod_agg.withColumn("rank", F.row_number().over(win))
        .filter("rank = 1")
        .select("custId", F.col("productSold").alias("favourite_product"))
    )
    
    # Longest streak
    streak_df = calculate_longest_streak(df)
    
    result = (
        fav_prod.join(streak_df, on="custId")
        .select(
            F.col("custId").alias("customer_id"),
            "favourite_product",
            "longest_streak"
        )
    )
    
    print("Rows to be written:", result.count())
    result.show()
    
    if destination_path:
        # Convert to Pandas and save to 
        pandas_df = result.toPandas()
        
        # make sure that output directory exists and can be created
        import os
        output_dir = os.path.dirname(destination_path) if os.path.dirname(destination_path) else '.'
        os.makedirs(output_dir, exist_ok=True)
        
        if destination_path.endswith('.parquet'):
            #  Parquet first, back to CSV if any issues
            try:
                pandas_df.to_parquet(destination_path, index=False)
                print(f"Output written to {destination_path} (Parquet format)")
            except Exception as e:
                print(f"Parquet write failed: {e}")
                # back to CSV
                csv_path = destination_path.replace('.parquet', '.csv')
                pandas_df.to_csv(csv_path, index=False)
                print(f"Output written to {csv_path} (CSV format - fallback)")
        else:
            pandas_df.to_csv(destination_path, index=False)
            print(f"Output written to {destination_path}")
    elif db_url and db_table:
        # Write to PostgreSQL using JDBC
        try:
            result.write \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", db_table) \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            print(f"Data successfully written to database table: {db_table}")
        except Exception as e:
            print(f"Database write failed: {e}")
            raise
    
    spark.stop()