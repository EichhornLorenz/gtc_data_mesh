# Databricks notebook source
# MAGIC %sh ls

# COMMAND ----------

# MAGIC %run "./Solution Customers Department"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
movies_df = spark.read.table("movies")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Extracting Movie Year from the Title and modifying the Title
# MAGIC
# MAGIC In this step, we extract the movie year from the 'title' column using a regular expression and create a new column called 'year'. We then strip the movie year from the 'title' column
# MAGIC
# MAGIC ** Hint: First remove string in brackets

# COMMAND ----------

# Extract movie year from title (assuming year is in parentheses at the end)
movies_df = movies_df.withColumn("title", F.regexp_replace(F.col("title"), r'\([a-zA-Z]+\)', ''))
movies_df = movies_df.withColumn("year", F.regexp_extract("title", r"\((\d{4})\)$", 1))
movies_df = movies_df.withColumn("year", F.col("year").cast("integer"))

# Strip movie year from title
movies_df = movies_df.withColumn("title", F.regexp_replace(F.col("title"), r'\(.*\)', ''))

# Display the modified dataset
movies_df.show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Health Checks
# MAGIC In this step, we conduct preliminary health checks on the dataset to ensure its quality and integrity. The checks include verifying data types, schema integrity, and the existence of the primary key field ('movieId'). These checks are essential for identifying any discrepancies in the data early in the processing pipeline.
# MAGIC

# COMMAND ----------

def compare_schema(df, predefined_schema):
    """
    Compare DataFrame schema with a predefined schema and return mismatches or success message.
    
    Args:
        df (DataFrame): PySpark DataFrame to be compared.
        predefined_schema (StructType): Predefined schema to compare with.
        
    Returns:
        str: Mismatches if any, otherwise a success message.
    """
    # Get the schema of the DataFrame
    df_schema = df.schema
    
    # Compare schemas
    mismatched_fields = []
    for field in predefined_schema.fields:
        if field.name not in [f.name for f in df_schema.fields]:
            mismatched_fields.append(f"Missing field: {field.name} - {field.dataType}")
        else:
            df_field = next(f for f in df_schema.fields if f.name == field.name)
            if df_field.dataType != field.dataType:
                mismatched_fields.append(f"Field {field.name} - Expected: {field.dataType}, Actual: {df_field.dataType}")
    
    # Return result message
    if mismatched_fields:
        return "\n".join(mismatched_fields)
    else:
        return "Schema matches the predefined schema."

# COMMAND ----------

def primary_key_check(df, pk_column):
    """
    Check for duplicate values in the specified primary key column of the DataFrame.

    Args:
        df (DataFrame): PySpark DataFrame to perform the primary key check on.
        pk_column (str): Name of the primary key column.

    Returns:
        dict: A dictionary containing the check result and number of rows that failed.
              Example: {"result": "Pass", "failed_rows": 0} or {"result": "Fail", "failed_rows": 10}
    """
    # Define a window specification over the primary key column
    window_spec = Window.partitionBy(pk_column).orderBy(F.col(pk_column))

    # Calculate the count of rows with the same primary key
    pk_check_df = df.withColumn("count", F.count(pk_column).over(window_spec))

    # Filter rows where count is greater than 1 (indicating duplicates)
    duplicates_df = pk_check_df.filter(pk_check_df["count"] > 1)

    # Count the number of duplicates
    num_duplicates = duplicates_df.count()

    # Prepare the result dictionary
    result = {
        "result": "Pass" if num_duplicates == 0 else "Fail",
        "failed_rows": num_duplicates
    }

    return result

# COMMAND ----------

expected_schema = T.StructType([
    T.StructField("movieId", T.LongType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("genres", T.StringType(), True),
    T.StructField("year", T.IntegerType(), True)
])

print(compare_schema(movies_df, expected_schema))

# Primary Key (movieId) Uniqueness Check
print(primary_key_check(movies_df, "movieId"))


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
