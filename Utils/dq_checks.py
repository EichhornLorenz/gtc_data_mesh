# Databricks notebook source
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

# MAGIC %run pwd

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------


