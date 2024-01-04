# Generate a sequential number for each row within each group, ordered by date.


from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("RowNumberPerGroup").getOrCreate()

# Sample data
group_data = [
    Row(GroupID='A', Date='2023-01-01'),
    Row(GroupID='A', Date='2023-01-02'),
    Row(GroupID='B', Date='2023-01-01'),
    Row(GroupID='B', Date='2023-01-03')
]

# Create DataFrame
df_group = spark.createDataFrame(group_data)

# Convert Date to date type
df_group = df_group.withColumn("Date", col("Date").cast("date"))

# Define window spec
windowSpec = Window.partitionBy("GroupID").orderBy("Date")

# Generate sequential number
df_group = df_group.withColumn("SeqNum", row_number().over(windowSpec))

# Show results
df_group.show()
