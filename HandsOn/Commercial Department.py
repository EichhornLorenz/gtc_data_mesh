# Databricks notebook source
from pyspark.sql import functions as F
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Movie Dataset Processing
# MAGIC In this notebook, our focus is on processing the Movies dataset using PySpark. The dataset is loaded into a PySpark DataFrame, and several essential tasks are performed to enhance the dataset's quality and usability. We start by performing Data Quality checks on key fields, ensuring the dataset's integrity. Following that, an additional column is created by extracting the movie year from the titles, enriching the dataset with valuable temporal information. Lastly, the titles are modified to align with the required output schema and Primary Key. These steps are crucial for ensuring data accuracy, enabling precise analysis, and facilitating meaningful insights from the Movies dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Importing the Movies Table
# MAGIC
# MAGIC In this step, we read the Movies dataset from a table in the Catalog into a Spark DataFrame. The dataset will be processed using Spark functionalities.

# COMMAND ----------

# Read the Movies dataset from a table
# movies_df = 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Extracting Movie Year from the Title and modifying the Title
# MAGIC
# MAGIC In this step, we extract the movie year from the 'title' column using a regular expression and create a new column called 'year'. We then strip the movie year from the 'title' column
# MAGIC
# MAGIC ** Hint: First remove string in brackets

# COMMAND ----------

# Extract movie year from title (assuming year is in parentheses at the end)

# Strip movie year from title


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Health Checks
# MAGIC In this step, we conduct preliminary health checks on the dataset to ensure its quality and integrity. The checks include verifying data types, schema integrity, and the existence of the primary key field ('movieId'). These checks are essential for identifying any discrepancies in the data early in the processing pipeline.
# MAGIC

# COMMAND ----------

expected_schema = T.StructType([
    T.StructField("movieId", T.IntegerType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("genres", T.StringType(), True),
    T.StructField("year", T.IntegerType(), True)
])

if movies_df.schema == expected_schema:
    print("Schema Integrity: Passed")
else:
    print("Schema Integrity: Failed")

# Primary Key (movieId) Uniqueness Check
pk_check = movies_df.groupBy("movieId").agg(F.count("movieId").alias("count"))
non_unique_pks = pk_check.filter(F.col("count") > 1).count()

if non_unique_pks == 0:
    print("Primary Key (movieId) Uniqueness: Passed")
else:
    print(f"Primary Key (movieId) Uniqueness: Failed for {non_unique_pks} records")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Save Processed Data
# MAGIC
# MAGIC We save the processed data into a table in a subfolder based on the user's name.
# MAGIC

# COMMAND ----------

import re

# Get the current username
user_id = spark.sql("select current_user() as user").collect()[0]['user']
user_id = re.sub(r'@.+$', "", user_id).replace(".", "_")
# Define the output path for the processed data
processed_data_path = f"{user_id}_movies_table"

# Write the processed DataFrame into a table
movies_df.write.mode("overwrite").saveAsTable(processed_data_path)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC This notebook performs the necessary preprocessing steps, ensuring the data is ready for further analysis and visualization. The output schema and primary key are defined to maintain data consistency and integrity.
