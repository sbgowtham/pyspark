# different delimiter in a file pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Initialize Spark Session
#spark = SparkSession.builder.appName("mixedDelimiterExample").getOrCreate()

# Sample data
data = ["1,Alice\t30|New York"]

# Creating a DataFrame with a single column
df = spark.createDataFrame(data, "string")

# Custom logic to split the mixed delimiter row
split_col = split(df['value'], ',|\t|\|')

# Creating new columns for each split part
df = df.withColumn('id', split_col.getItem(0))\
       .withColumn('name', split_col.getItem(1))\
       .withColumn('age', split_col.getItem(2))\
       .withColumn('city', split_col.getItem(3))

# Selecting and showing the result
df.select('id', 'name', 'age', 'city').show()

# Stop Spark Session
#spark.stop()
