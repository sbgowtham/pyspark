# In a DataFrame df_sales with columns Date, ProductID, and QuantitySold, how would you calculate a 7-day rolling average of QuantitySold for each product?

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("RollingAverageCalculation").getOrCreate()

# Sample data
data = [Row(Date='2023-01-01', ProductID=100, QuantitySold=10),
        Row(Date='2023-01-02', ProductID=100, QuantitySold=15),
        Row(Date='2023-01-03', ProductID=100, QuantitySold=20),
        Row(Date='2023-01-04', ProductID=100, QuantitySold=25),
        Row(Date='2023-01-05', ProductID=100, QuantitySold=30),
        Row(Date='2023-01-06', ProductID=100, QuantitySold=35),
        Row(Date='2023-01-07', ProductID=100, QuantitySold=40),
        Row(Date='2023-01-08', ProductID=100, QuantitySold=45)]

# Create DataFrame
df_sales = spark.createDataFrame(data)

# Convert Date string to Date type
df_sales = df_sales.withColumn("Date", F.to_date(F.col("Date")))

# Window specification for 7-day rolling average
windowSpec = Window.partitionBy('ProductID').orderBy('Date').rowsBetween(-6, 0)

# Calculating the rolling average
rollingAvg = df_sales.withColumn('7DayAvg', F.avg('QuantitySold').over(windowSpec))

# Show results
rollingAvg.show()
