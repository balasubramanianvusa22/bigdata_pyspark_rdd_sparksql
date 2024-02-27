# bigdata_pyspark_rdd_sparksql
This repository contains Python code demonstrating the use of PySpark RDD and SQL operations for distributed computing and structured data analysis.

## Learning Objectives
  * Demonstrate the use of PySpark RDD for basic distributed computing.
  * Demonstrate the use of PySpark SQL for structured data analysis.

### Part 1: Configuring Spark and Downloading Data
In this part, we set up the Spark environment and download the Yelp review dataset for further analysis.

### Part 2: PySpark RDD Operations
This section focuses on PySpark RDD techniques for processing an external file that contains users' text reviews on businesses from Tucson.

### External File Description
The external file, text_reviews.txt, comprises two columns separated by tabs (\t). The first column stores each review's associated user's ID, while the second column stores each review's text information.

### Tasks
1. Read and Extract
    * Utilize Spark's textFile function to read the text file into an RDD.
    * Extract each line's user ID and review text by creating a customized mapper function and applying the map transformation on the RDD.
2. Unique Users Count
    * Determine the total number of unique users in the dataset by applying the distinct action and print the count.
3. Total Reviews per User
    * Calculate the total number of reviews each user has posted using the reduceByKey transformation.
    * Sort the transformed RDD by values (total number of reviews) in descending order using the sortBy transformation.
    * Display the top 10 users with the highest number of reviews.
4. Total Length of Reviews
    * Compute the total length of reviews (total number of characters) each user has posted using the reduceByKey transformation.
    * Sort the transformed RDD by values (total length of reviews) in ascending order with the sortBy transformation.
    * Show the 10 users with the least amount of reviews (lowest total length of reviews).

### Part 3: PySpark SQL Operations
This section demonstrates the utilization of Spark SQL techniques to read and process an external file (user_reviews.csv) containing users' individual reviews on businesses from Tucson.

### File Attributes
The external file, user_reviews.csv, consists of the following six attributes:

* review_id: a string indicating the review's ID
* user_id: a string indicating the reviewer's ID
* business_id: a string indicating the ID of the reviewed business
* review_stars: a float indicating the review's star rating
* useful: an integer indicating the number of useful votes received by the review
* review_text: a string storing the review's text

### Tasks
1. Read and Inspect Data
    * Utilize Spark SQL's read.csv function to read the csv file into a Spark DataFrame.
    * Customize the schema based on the provided information.
    * Print the DataFrame's schema and display the first 20 rows.
2. Handle Missing Data
    * Fill missing values in the DataFrame's review_id or business_id attributes with the string "missing" using the fillna transformation.
    * Drop rows with missing values from the DataFrame.
    * Print the remaining row count after removing rows with missing values.
3. Creating Additional Column
    * Create a new column named review_text_length using the withColumn transformation to store the length of each review.
    * Utilize the length function from Spark SQL's functions library.
4. Aggregate Data
    * Group the DataFrame by user_id to calculate the following values for each user:
      * Average star rating given by the user (rounded to 2 decimals).
      * Average review length given by the user (rounded to 2 decimals).
      * Total useful votes received by the user.
    * Sort the grouped DataFrame by total useful votes (in descending order) and average star rating (in ascending order).
    * Display the first 20 rows of the sorted DataFrame.
