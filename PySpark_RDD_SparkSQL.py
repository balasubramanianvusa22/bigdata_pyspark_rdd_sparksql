# -*- coding: utf-8 -*-
"""
## Learning Objectives
* Demonstrate the use of PySpark RDD for basic distributed computing
* Demonstrate the use of PySpark SQL for structured data analysis

## Part 1: Configuring Spark and Downloading Data
"""

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

"""## Part 2: PySpark RDD Operations

This section demonstrates the utilization of PySpark RDD techniques for reading and processing an external file containing users' text reviews on businesses from Tucson.

### External File Description
The external file, `text_reviews.txt`, comprises two columns separated by tabs (`\t`). The first column stores each review's associated user's ID, while the second column stores each review's text information.

### Tasks:
1. **Read and Extract:**
  * Utilize Spark's `textFile` function to read the text file into an RDD.
  * Extract each line's user ID and review text by creating a customized mapper function and applying the `map` transformation on the RDD.
2. **Unique Users Count:**
  * Determine the total number of unique users in the dataset by applying the `distinct` action and print the count.
3. **Total Reviews per User:**
  * Calculate the total number of reviews each user has posted using the `reduceByKey` transformation.
  * Sort the transformed RDD by values (i.e., total number of reviews) in descending order using the `sortBy` transformation.
  * Display the top 10 users with the highest number of reviews.
4. **Total Length of Reviews:**
  * Compute the total length of reviews (i.e., the total number of characters) each user has posted using the `reduceByKey` transformation.
  * Sort the transformed RDD by values (i.e., total length of reviews) in ascending order with the `sortBy` transformation.
  * Show the 10 users with the least amount of reviews (i.e., the lowest total length of reviews).

"""

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

"""## Part 3: PySpark SQL Operations
This section demonstrates the utilization of Spark SQL techniques to read and process an external file ('user_reviews.csv') that contains users' individual reviews on businesses from Tucson.

### File Attributes:
The external file (`user_reviews.csv`) consists of the following six attributes:
* `review_id`: a string indicating the review's ID
* `user_id`: a string indicating the reviewer's ID
* `business_id`: a string indicating the ID of the reviewed business
* `review_stars`: a float indicating the review's star rating
* `useful`: an integer indicating the number of useful votes received by the review
* `review_text`: a string storing the review's text

###Tasks:
1. Read and Inspect Data:
  * Utilize Spark SQL's 'read.csv' function to read the csv file into a Spark DataFrame.
  * Customize the schema based on the provided information.
  * Print the DataFrame's schema and display the first 20 rows.
2. Handle Missing Data:
  * Fill missing values in the DataFrame's review_id or business_id attributes with the string "missing" using the fillna transformation.
  * Drop rows with missing values from the DataFrame.
  * Print the remaining row count after removing rows with missing values.
3. Creating Additional column:
  * Create a new column named review_text_length using the withColumn transformation to store the length of each review.
  * Utilize the length function from Spark SQL's functions library.
4. Aggregate Data:
  * Group the DataFrame by user_id to calculate the following values for each user:
    * Average star rating given by the user (rounded to 2 decimals).
    * Average review length given by the user (rounded to 2 decimals).
    * Total useful votes received by the user.
  * Sort the grouped DataFrame by total useful votes (in descending order) and average star rating (in ascending order).
  * Display the first 20 rows of the sorted DataFrame.
"""

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
