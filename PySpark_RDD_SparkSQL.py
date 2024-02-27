# -*- coding: utf-8 -*-
!pip install pyspark

# create spark context and spark session
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PySpark_RDD_SQL_conf")
sc = SparkContext(conf = conf)

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark_RDD_SQL_spark").getOrCreate()

# download the yelp review dataset
from urllib.request import urlretrieve
urlretrieve('https://drive.google.com/uc?export=download&id=1AV5z7EcHoMabL5yijp_jRwcqzbHNhxGp',
            'user_reviews.csv')
urlretrieve('https://drive.google.com/uc?export=download&id=1D1BKYITdAsy82UVcbO8bTL7oDY-flHId',
            'text_reviews.txt')

# Load the text_reviews.txt file as an RDD and split each line by tab to
# create fields
text_reviews = sc.textFile("/content/text_reviews.txt")
text_reviews_fields = text_reviews.map(lambda line: line.split('\t'))

# Calculate the total number of unique users
uniqueUsersCount = text_reviews_fields.map(lambda line:
                                           line[0]).distinct().count()
print("Total number of unique users:", uniqueUsersCount)

# Count the number of reviews for each user and sort the results in descending
# order
userReviewsCount = text_reviews_fields.map(lambda line:
 (line[0], 1)).reduceByKey(lambda count1, count2: count1 + count2)
sortedUserReviewsCount = userReviewsCount.sortBy(lambda x:
                                                 x[1], ascending=False)
print("Top 10 users with the highest number of reviews:",
      sortedUserReviewsCount.take(10))

# Calculate the total length of reviews for each user and sort the results in
# ascending order
userReviewsLength = text_reviews_fields.map(lambda line:
 (line[0], len(line[1]))).reduceByKey(lambda length1, length2:
                                      length1 + length2)
sortedUserReviewsLength = userReviewsLength.sortBy(lambda x: x[1])
print("Top 10 users with the shortest review length:",
      sortedUserReviewsLength.take(10))

# Importing pandas and Spark SQL libraries
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, length, round, sum, avg

# Reading CSV into a dataframe
userReviewsDFOld = spark.read.csv('/content/user_reviews.csv')
userReviewsDFOld.printSchema()

# Defining schema for the reviewsSchema dataframe
reviewsSchema = StructType([
    StructField('review_id', StringType(), True),
    StructField('user_id', StringType(), True),
    StructField('business_id', StringType(), True),
    StructField('review_stars', FloatType(), True),
    StructField('useful', IntegerType(), True),
    StructField('review_text', StringType(), True)])

# Reading the CSV file with the defined schema
userReviewsDF = spark.read.csv('/content/user_reviews.csv', header=True, schema = reviewsSchema)
userReviewsDF.printSchema()
userReviewsDF.show(truncate=False)

# Removing the missing values
print("Number of rows before removing missing values:", userReviewsDF.count())
userReviewsDF = userReviewsDF.fillna("missing", subset=["business_id", "review_id"])
userReviewsDF.show(truncate=False)
userReviewsDF = userReviewsDF.filter((userReviewsDF.review_id != 'missing') | (userReviewsDF.business_id != 'missing'))
print("Number of rows after removing missing values:", userReviewsDF.count())

# Adding new column for the length of the review text
userReviewsDF = userReviewsDF.withColumn('review_text_length', length(col('review_text')))

# Grouping the DataFrame by user_id and calculating statistics
userReviewStatistics = userReviewsDF.groupBy('user_id').agg(
    round(avg('review_stars'), 2).alias('avg_review_stars'),
    round(avg('review_text_length'), 2).alias('avg_review_length'),
    round(sum('useful'), 2).alias('useful_votes')
)

# Sorting the statistics DataFrame by useful votes and average review stars
userReviewStatistics = userReviewStatistics.orderBy(['useful_votes', 'avg_review_stars'], ascending=[False, True])
userReviewStatistics.show()

# Stopping the Spark session
spark.stop()
