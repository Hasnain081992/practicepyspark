from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_data():
    spark = SparkSession.builder .appName("Basic PySpark Example with One Function") .getOrCreate()
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29), ("David", 19)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    filtered_df = df.filter(col("Age") > 30)
    filtered_df.show()
    spark.stop()




    if __name__ == "__main__":
        process_data()
      





