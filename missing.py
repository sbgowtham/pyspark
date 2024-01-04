#find the missing value from the list 


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Find Missing Numbers").getOrCreate()

# Sample data
data = [(1,), (2,), (4,), (5,), (7,), (8,), (10,)]
df_numbers = spark.createDataFrame(data, ["Number"])

# Generating a complete sequence DataFrame
full_range = spark.range(1, 11).toDF("Number")

# Finding missing numbers
missing_numbers = full_range.join(df_numbers, "Number", "left_anti")
missing_numbers.show()
