# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

    for field in df_schema.fields:
        if field.name not in [f.name for f in predefined_schema.fields]:
            mismatched_fields.append(f"Additional field which is not in schema: {field.name} - {field.dataType}")
    
    # Return result message
    if mismatched_fields:
        print("\n".join(mismatched_fields))
    else:
        print("Schema matches the predefined schema.")



# COMMAND ----------

def primary_key_check(df, pk_columns):
    """
    Check for duplicate values in the specified primary key columns of the DataFrame.

    Args:
        df (DataFrame): PySpark DataFrame to perform the primary key check on.
        pk_columns (list): List of primary key column names.

    Returns:
        dict: A dictionary containing the check result and number of rows that failed.
              Example: {"result": "Pass", "failed_rows": 0} or {"result": "Fail", "failed_rows": 10}
    """
    # Define a window specification over the primary key columns
    window_spec = Window.partitionBy(*pk_columns).orderBy(*[F.col(col) for col in pk_columns])

    # Calculate the count of rows with the same primary key columns
    pk_check_df = df.withColumn("count", F.count(pk_columns[0]).over(window_spec))

    # Filter rows where count is greater than 1 (indicating duplicates)
    duplicates_df = pk_check_df.filter(pk_check_df["count"] > 1)

    # Count the number of duplicates
    num_duplicates = duplicates_df.count()

    # Prepare the result dictionary
    result = {
        "result": "Pass" if num_duplicates == 0 else "Fail",
        "failed_rows": num_duplicates
    }

    print(result)
