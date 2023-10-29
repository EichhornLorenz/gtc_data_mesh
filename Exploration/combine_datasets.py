# Databricks notebook source
import pyspark.sql.functions as F
from functools import reduce

# COMMAND ----------

dfs = []
part = 1
parts = 3
while part <= parts:
  dfs.append(spark.read.table(f"default.ratings_{part}"))
  part += 1

# COMMAND ----------

result_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)

# COMMAND ----------

result_df.write.saveAsTable("default.ratings",  mode="overwrite")

# COMMAND ----------

df1 = spark.read.table(f"default.ratings")
df1.count()

# COMMAND ----------

df1 = df1.withColumn("ts", F.col("timestamp").cast("timestamp"))
df1 = df1.withColumn("month", F.month("ts"))
df1 = df1.withColumn("day", F.dayofmonth("ts"))

# COMMAND ----------

df_filtered = df1.filter(F.col("month").isin(12))

# COMMAND ----------

df_grouped = df_filtered.groupBy("movieId").agg(F.mean("rating").alias("avg_rating"), F.count("rating").alias("count")).orderBy(F.desc("count"))

# COMMAND ----------

movies = spark.read.table("default.movies").withColumn("movieYear", F.regexp_extract("title", r'\((.*?)\)[^()]*$', 1))

# COMMAND ----------

df_grouped = df_grouped.join(movies, on=["movieId"], how="left")

# COMMAND ----------

df_grouped.orderBy(F.desc("count")).display()

# COMMAND ----------

df_grouped.filter(F.col("count") > 500).orderBy(F.desc("avg_rating")).display()

# COMMAND ----------

df_grouped = df_grouped.withColumn('Number_genre', F.size(F.split(F.col("genres"), "|")))

# COMMAND ----------

df_grouped.select(F.max("Number_genre")).show()

# COMMAND ----------


