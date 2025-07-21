import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from etl.etl import calculate_longest_streak
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def test_longest_streak():
    # test Spark session
    spark = SparkSession.builder.appName("Test").getOrCreate()
    
    # Create test data
    schema = StructType([
        StructField("custId", StringType(), True),
        StructField("transactionDate", StringType(), True)
    ])
    
    data = [
        ("1", "2023-01-01"),
        ("1", "2023-01-02"),
        ("1", "2023-01-04")
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Test  function
    result = calculate_longest_streak(df)
    result_pandas = result.toPandas()
    
    # Check  result
    assert result_pandas["longest_streak"][0] == 2
    print("Test passed!")
    
    spark.stop()

if __name__ == "__main__":
    test_longest_streak()