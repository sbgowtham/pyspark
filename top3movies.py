#find top 3 movie based on the rating

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize Spark Session
#spark = SparkSession.builder.appName("TopMovies").getOrCreate()

# Sample DataFrames
data_movies = [(1, "Movie A"), (2, "Movie B"), (3, "Movie C"), (4, "Movie D"), (5, "Movie E")]

data_ratings = [(1, 101, 4.5), (1, 102, 4.0), (2, 103, 5.0), 
                (2, 104, 3.5), (3, 105, 4.0), (3, 106, 4.0), 
                (4, 107, 3.0), (5, 108, 2.5), (5, 109, 3.0)]

columns_movies = ["MovieID", "MovieName"]
columns_ratings = ["MovieID", "UserID", "Rating"]

# Creating DataFrames
df_movies = spark.createDataFrame(data_movies, columns_movies)
df_ratings = spark.createDataFrame(data_ratings, columns_ratings)

# Calculating average ratings
avg_ratings = df_ratings.groupBy('MovieID').agg(avg('Rating').alias('AvgRating'))

# Joining with df_movies to get movie names
top_movies = avg_ratings.join(df_movies, 'MovieID').orderBy('AvgRating', ascending=False).limit(3)

# Showing the top 3 movies
top_movies.show()
