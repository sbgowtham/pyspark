# Find the count of unique visitors to a website per day.

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName("UniqueVisitorsPerDay").getOrCreate()

# Sample data
visitor_data = [Row(Date='2023-01-01', VisitorID=101),
                Row(Date='2023-01-01', VisitorID=102),
                Row(Date='2023-01-01', VisitorID=101),
                Row(Date='2023-01-02', VisitorID=103),
                Row(Date='2023-01-02', VisitorID=101)]

# Create DataFrame
df_visitors = spark.createDataFrame(visitor_data)

# Count unique visitors per day
unique_visitors = df_visitors.groupBy('Date').agg(countDistinct('VisitorID').alias('UniqueVisitors'))

# Show results
unique_visitors.show()
