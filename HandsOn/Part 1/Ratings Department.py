# Databricks notebook source
from pyspark.sql import functions as F
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer Ratings Processing
# MAGIC
# MAGIC In this notebook, we will perform data processing tasks on the Customer Ratings dataset. The dataset is loaded as a PySpark DataFrame. We will ensure the 'timestamp' column is of type timestamp, create a new column indicating the month of the rating, and conduct simple Data Quality checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load Data
# MAGIC
# MAGIC We start by loading the ratings table data into a PySpark DataFrame from the table location.

# COMMAND ----------

# Load the ratings table as a PySpark DataFrame
# ratings_df = spark.read.table("ratings")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Timestamp Conversion
# MAGIC
# MAGIC We convert the 'timestamp' column to the timestamp data type using the `to_timestamp` function.

# COMMAND ----------

# Ensure the 'timestamp' column is of type timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create 'Month' and 'Year' Columns
# MAGIC
# MAGIC We create two new columns 'month' and 'year', extracting the month and year from the 'timestamp' column using the `month`/`year`  function.
# MAGIC
# MAGIC Output schema:
# MAGIC |   Column    |   Data Type    |
# MAGIC |-------------|-----------------|
# MAGIC | userId      | LongType       |
# MAGIC | movieId     | LongType       |
# MAGIC | timestamp   | TimestampType  |
# MAGIC | rating      | DoubleType     |
# MAGIC | month       | IntegerType    |
# MAGIC | year       | IntegerType    |
# MAGIC

# COMMAND ----------

# Create a new column 'month' indicating the month of the rating


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Data Quality Checks
# MAGIC
# MAGIC We perform simple Data Quality checks, counting null values in important fields ('user_id', 'product_id', 'rating'). Results are displayed.

# COMMAND ----------

# Perform simple Data Quality checks
null_check_result = {
    "user_id": ratings_df.filter(F.col("userId").isNull()).count(),
    "product_id": ratings_df.filter(F.col("movieId").isNull()).count(),
    "rating": ratings_df.filter(F.col("rating").isNull()).count()
}

# Display null value checks results
null_check_result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Output Schema and Primary Key
# MAGIC
# MAGIC We define the expected output schema and specify the primary key fields essential for data integrity.

# COMMAND ----------

# Run this cell to import dq funtions from utils

# COMMAND ----------

# MAGIC %run Repos/Shared/gtc_data_mesh/Utils/dq_checks

# COMMAND ----------

# Define the expected output schema and primary key
expected_schema = T.StructType([
    T.StructField("userId", T.LongType(), True),
    T.StructField("movieId", T.LongType(), True),
    T.StructField("timestamp", T.TimestampType(), True),
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("month", T.IntegerType(), True),
    T.StructField("year", T.IntegerType(), True)
])

primary_key = ["userId", "movieId", "timestamp"]

compare_schema(ratings_df, expected_schema)

# Primary Key (movieId) Uniqueness Check
primary_key_check(ratings_df, primary_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Save Processed Data
# MAGIC
# MAGIC We save the processed data into a table in a subfolder based on the user's name.
# MAGIC

# COMMAND ----------

import re

# Get the current username
user_id = spark.sql("select current_user() as user").collect()[0]['user']
user_id = re.sub(r'@.+$', "", user_id).replace(".", "_")
# Define the output path for the processed data
processed_data_path = f"{user_id}_processed_ratings"

# Write the processed DataFrame into a table
ratings_df.write.mode("overwrite").saveAsTable(processed_data_path)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC This notebook performs the necessary preprocessing steps, ensuring the data is ready for further analysis and visualization. The output schema and primary key are defined to maintain data consistency and integrity.  
