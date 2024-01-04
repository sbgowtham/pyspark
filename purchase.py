# Determine the first purchase date for each user.


from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import min

# Initialize Spark session
spark = SparkSession.builder.appName("FirstPurchaseDate").getOrCreate()

# Sample data
purchase_data = [
    Row(UserID=1, PurchaseDate='2023-01-05'),
    Row(UserID=1, PurchaseDate='2023-01-10'),
    Row(UserID=2, PurchaseDate='2023-01-03'),
    Row(UserID=3, PurchaseDate='2023-01-12')
]

# Create DataFrame
df_purchases = spark.createDataFrame(purchase_data)

# Convert PurchaseDate to date type
df_purchases = df_purchases.withColumn("PurchaseDate", col("PurchaseDate").cast("date"))

# Find first purchase date for each user
first_purchase = df_purchases.groupBy("UserID").agg(min("PurchaseDate").alias("FirstPurchaseDate"))

# Show results
first_purchase.show()
