#Problem: Given a dataset of sales records with monthly sales per product, reshape the data to have one row per product-month combination.

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark Session
#spark = SparkSession.builder.appName("DataReshaping").getOrCreate()

# Sample data: Product sales per month
data = [("Product1", 100, 150, 200),
        ("Product2", 200, 250, 300),
        ("Product3", 300, 350, 400)]

# Columns: Product, Sales_Jan, Sales_Feb, Sales_Mar
columns = ["Product", "Sales_Jan", "Sales_Feb", "Sales_Mar"]

# Creating DataFrame
df = spark.createDataFrame(data, columns)

# Pivoting the DataFrame
# This step transforms the data into a long format: Product, Month, Sales
pivoted_df = df.selectExpr("Product", 
                           "stack(3, 'Jan', Sales_Jan, 'Feb', Sales_Feb, 'Mar', Sales_Mar) as (Month, Sales)")

# Show the result
pivoted_df.show()

# Stop Spark Session
#spark.stop()
