# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Genomics Datasets Processing
# MAGIC
# MAGIC In this notebook, we will process genomics datasets from the Genomics Department. The datasets are located under the 'genomes datasets' tag and will be imported into PySpark DataFrames. The primary goal is to enrich the 'genome_scores' dataset with tag names, create relevance buckets, and conduct essential Data Quality checks.
# MAGIC
# MAGIC ### Step 1: Import Genomics Datasets
# MAGIC We start by locating and importing tables genome_scores and genome_tags. The tables will be loaded into PySpark DataFrames for further processing.
# MAGIC

# COMMAND ----------

# genome_scores_df = 
# genome_tags_df = 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Enrich Genome Scores with Tag Names
# MAGIC We enrich the 'genome_scores' dataset by joining it with the 'genome_tags' dataset based on the 'tagId' column and rename the 'tag' column to 'tag_names'.

# COMMAND ----------

# Enrich genome_scores with tag names


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create Relevance Buckets
# MAGIC We create buckets for the 'relevance' column, categorizing values into intervals [0.0-0.1, 0.1-0.2, 0.3-0.4, ...].

# COMMAND ----------

from pyspark.ml.feature import Bucketizer

# Create relevance buckets and name them

# Define bucket names
bucket_names = ["Low", "Moderate", "High", "Very High", "Excellent"]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Data Quality Checks
# MAGIC We perform Data Quality checks on important fields, validate the output schema, and ensure Primary Key (PK) uniqueness.

# COMMAND ----------

# Data Quality Checks
important_fields_check = {
    "movieId": enriched_genome_scores_df.filter(F.col("movieId").isNull()).count(),
    "tag_names": enriched_genome_scores_df.filter(F.col("tag_names").isNull()).count(),
    "relevance": enriched_genome_scores_df.filter(F.col("relevance").isNull()).count()
}

# Display null value checks results
print(important_fields_check)

# Expected Output Schema
expected_schema = {
    "movieId": "integer",
    "tagId": "integer",
    "tag_names": "string",
    "relevance": "double",
    "relevance_bucket": "string"
}

if enriched_genome_scores_df.schema == expected_schema:
    print("Schema Integrity: Passed")
else:
    print("Schema Integrity: Failed")

# Primary Key (PK) Uniqueness Check
non_unique_pks = enriched_genome_scores_df.groupBy("movieId", "tagId").agg(F.count("tagId").alias("count")).filter(F.col("count") > 1).count()

if non_unique_pks == 0:
    print("Primary Key (movieId, tagId) Uniqueness: Passed")
else:
    print(f"Primary Key (movieId, tagId) Uniqueness: Failed for {non_unique_pks} records")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Save Processed Data
# MAGIC Finally, we save the processed data into a table under the user's subfolder, ensuring organized and accessible storage.

# COMMAND ----------

import re

# Get the current username
user_id = spark.sql("select current_user() as user").collect()[0]['user']
user_id = re.sub(r'@.+$', "", user_id).replace(".", "_")
# Define the output path for the processed data
processed_data_path = f"{user_id}_processed_genome_scores"

# Write the processed DataFrame into a table
enriched_genome_scores_df.write.mode("overwrite").saveAsTable(processed_data_path)
