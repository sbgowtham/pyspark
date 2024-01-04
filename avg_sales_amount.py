#Problem: Given a dataset of sales records, identify and replace all missing values in the 'amount' column with the average sales amount.

from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col

# Initialize Spark Session
#spark = SparkSession.builder.appName("HandleMissingValues").getOrCreate()

# Sample data for sales - id, amount (with missing values represented by None)
sales_data = [("1", 100), ("2", 150), ("3", None), ("4", 200), ("5", None)]

# Creating DataFrame
sales_df = spark.createDataFrame(sales_data, ["sale_id", "amount"])

# Calculate the average sales amount
avg_amount = sales_df.na.drop().agg(mean(col("amount"))).first()[0]

# Replace missing values with the average amount
sales_df_filled = sales_df.na.fill(avg_amount)

# Show the result
sales_df_filled.show()

# Stop Spark Session
#spark.stop()
