# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer Ratings Processing
# MAGIC
# MAGIC In this notebook, we will perform data processing tasks on the Customer Ratings dataset. The dataset is loaded as a PySpark DataFrame. We will ensure the 'timestamp' column is of type timestamp, create a new column indicating the month of the rating, and conduct simple Data Quality checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load Data
# MAGIC
# MAGIC We start by loading the ratings table data into a PySpark DataFrame from the specified CSV file path.

# COMMAND ----------

# Load the ratings table as a PySpark DataFrame
# ratings_df = 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Timestamp Conversion
# MAGIC
# MAGIC We convert the 'timestamp' column to the timestamp data type using the `to_timestamp` function.

# COMMAND ----------

# Ensure the 'timestamp' column is of type timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create 'Month' Column
# MAGIC
# MAGIC We create a new column 'month', extracting the month from the 'timestamp' column using the `month` function.

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

# Define the expected output schema and primary key
output_schema = {
    "userId": "integer",
    "movieId": "integer",
    "timestamp": "timestamp",
    "rating": "integer",
    "month": "integer"
}

primary_key = ["user_id", "product_id", "timestamp"]

# Display the output schema and primary key
output_schema, primary_key

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
processed_data_path = f"{user_id}_ratings_table"

# Write the processed DataFrame into a table
ratings_df.write.mode("overwrite").saveAsTable(processed_data_path)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC This notebook performs the necessary preprocessing steps, ensuring the data is ready for further analysis and visualization. The output schema and primary key are defined to maintain data consistency and integrity.  