# How can you use UDFs (User Defined Functions) in PySpark to apply a complex transformation, say, categorizing ages into groups ('Youth', 'Adult', 'Senior')?

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("AgeCategorization").getOrCreate()

# Sample data
data = [Row(UserID=4001, Age=17),
        Row(UserID=4002, Age=45),
        Row(UserID=4003, Age=65),
        Row(UserID=4004, Age=30),
        Row(UserID=4005, Age=80)]

# Create DataFrame
df = spark.createDataFrame(data)

# Define UDF to categorize age
def categorize_age(age):
    if age < 18:
        return 'Youth'
    elif age < 60:
        return 'Adult'
    else:
        return 'Senior'

age_udf = udf(categorize_age, StringType())

# Apply UDF to categorize ages
df = df.withColumn('AgeGroup', age_udf(df['Age']))

# Show results
df.show()
