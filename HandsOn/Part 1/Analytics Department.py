# Databricks notebook source
from pyspark.sql import functions as F
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %md
# MAGIC # Genomics Datasets Processing
# MAGIC
# MAGIC In this notebook, we will process genomics datasets from the Analytics Department. The datasets are located under the 'genomes datasets' tag and will be imported into PySpark DataFrames. The primary goal is to enrich the 'genome_scores' dataset with tag names, create relevance buckets, and conduct essential Data Quality checks.
# MAGIC
# MAGIC ### Step 1: Import Genomics Datasets
# MAGIC We start by locating and importing tables genome_scores and genome_tags. The tables will be loaded into PySpark DataFrames for further processing.
# MAGIC

# COMMAND ----------

# genome_scores_df = spark.read.table("genome_scores")
# genome_tags_df = spark.read.table("genome_tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Enrich Genome Scores with Tag Names
# MAGIC We enrich the 'genome_scores' dataset by joining it with the 'genome_tags' dataset based on the 'tagId' column and rename the 'tag' column to 'tag_names'.
# MAGIC
# MAGIC Output schema:
# MAGIC |   Column    |   Data Type   |
# MAGIC |-------------|---------------|
# MAGIC | movieId     | LongType      |
# MAGIC | tagId       | LongType      |
# MAGIC | tag_names   | StringType    |
# MAGIC | relevance   | DoubleType    |
# MAGIC

# COMMAND ----------

# Enrich genome_scores with tag names


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create Relevance Buckets
# MAGIC We create buckets for the 'relevance' column, categorizing values into intervals [0.0-0.2, 0.2-0.4, 0.4-0.6, 0.6-0.8, 0.8-1.0].
# MAGIC
# MAGIC Documentation Bucketizer: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.Bucketizer.html
# MAGIC
# MAGIC Output schema:
# MAGIC |   Column    |   Data Type   |
# MAGIC |-------------|---------------|
# MAGIC | movieId     | LongType      |
# MAGIC | tagId       | LongType      |
# MAGIC | tag_names   | StringType    |
# MAGIC | relevance   | DoubleType    |
# MAGIC | relevance_bucket   | StringType    |
# MAGIC

# COMMAND ----------

from pyspark.ml.feature import Bucketizer

# Create relevance buckets and name them
bucket_splits = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]

# Define bucket names
bucket_names = ["Low", "Moderate", "High", "Very High", "Excellent"]

# Create Bucketizer
# bucketizer = Bucketizer()

# Map to bucket name and create relevance_bucket column

# Transform dataframe using the bucketizer
# enriched_genome_scores_df =


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Data Quality Checks
# MAGIC We perform Data Quality checks on important fields, validate the output schema, and ensure Primary Key (PK) uniqueness.

# COMMAND ----------

# Run this cell to import dq funtions from utils
%run Repos/Shared/gtc_data_mesh/Utils/dq_checks

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
expected_schema = T.StructType([
    T.StructField("movieId", T.LongType(), True),
    T.StructField("tagId", T.LongType(), True),
    T.StructField("tag_names", T.StringType(), True),
    T.StructField("relevance", T.DoubleType(), True),
    T.StructField("relevance_bucket", T.StringType(), True)
])

compare_schema(enriched_genome_scores_df, expected_schema)

# Primary Key (movieId) Uniqueness Check
primary_key_check(enriched_genome_scores_df, ["movieId", "tagId"])

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
processed_data_path = f"{user_id}_processed_genomes"

# Write the processed DataFrame into a table
enriched_genome_scores_df.write.mode("overwrite").saveAsTable(processed_data_path)
