from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, format_number

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Transform") \
    .master("local[*]") \
    .getOrCreate()

# Load data
path = "/tmp/bigdata_nov_2024/Hasnain/sales.csv"
df = spark.read.csv(path, header=True, inferSchema=True)

# Rename columns
df = df.withColumnRenamed('Order Date', 'Purchase Date')

# Filter Data
household_df = df.filter(col('Item Type') == 'Household')

# Rename multiple columns
df = df.withColumnRenamed('Total Revenue', 'Total_Sales') \
       .withColumnRenamed('Total Cost', 'Total_Purchase')

# Add a new column
df = df.withColumn('Computed_Sales', col('Unit Price') * col('Units Sold'))

# Drop unnecessary columns
df = df.drop('Order ID', 'Total Revenue')

# Group by and aggregate
grouped_priority_df = df.groupBy('Order Priority') \
    .agg(sum('Computed_Sales').alias('Total_Sales_Sum')) \
    .withColumn('Formatted_Sales', format_number('Total_Sales_Sum', 2))

# Group by region and sum
revenue_df = df.groupBy('Region') \
    .agg(sum('Total_Sales').alias('Total_Sales_Sum')) \
    .withColumn('Formatted_Sales', format_number('Total_Sales_Sum', 2))

# Sort data by Order Priority
sorted_df = df.orderBy('Order Priority')

# Replace values in a column
replaced_df = df.withColumn('Sales Channel', when(col('Sales Channel') == 'online', 1).otherwise(0))

# Group by Sales Channel
grouped_channel_df = df.groupBy('Sales Channel') \
    .agg(sum('Total_Sales').alias('Total_Sales_Sum')) \
    .withColumn('Formatted_Sales', format_number('Total_Sales_Sum', 2))

# Show results
print("Filtered Household Data:")
household_df.show(5)

print("Grouped by Order Priority:")
grouped_priority_df.show()

print("Grouped by Region:")
revenue_df.show()

print("Sorted by Order Priority:")
sorted_df.show()

print("Sales Channel Replaced:")
replaced_df.show()

print("Grouped by Sales Channel:")
grouped_channel_df.show()
