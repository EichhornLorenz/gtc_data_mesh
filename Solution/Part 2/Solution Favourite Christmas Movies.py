# Databricks notebook source
# MAGIC %md
# MAGIC # Christmas Movie Viewer Prediction
# MAGIC
# MAGIC In this notebook, we aim to determine which Christmas movie will bring the most viewers based on December's ratings. We will join the 'movies' dataset with the 'ratings' dataset, filter ratings within December, calculate average ratings and rating frequencies per movie, summarize the results, join with the 'genomes' dataset, filter on the Christmas genome for tags with high relevance, and finally produce the ultimate Christmas movie suggestion.
# MAGIC
# MAGIC ### **Step 1: Import Datasets and Define Paths**
# MAGIC We start by importing necessary functions and defining the paths to the 'movies' and 'ratings' datasets. The datasets are then loaded into PySpark DataFrames for analysis.

# COMMAND ----------

from pyspark.sql import functions as F
import re

# Get the current username
user_id = spark.sql("select current_user() as user").collect()[0]['user']
user_id = re.sub(r'@.+$', "", user_id).replace(".", "_")
# Define the output path for the processed data
genomes_data_path = f"{user_id}_processed_genome_scores"
movies_data_path = f"{user_id}_movies_table"
ratings_data_path = f"{user_id}_processed_ratings_table"

# Load datasets as PySpark DataFrames
movies_df = spark.read.table(movies_data_path)
ratings_df = spark.read.table(ratings_data_path)
genomes_df = spark.read.table(genomes_data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Filter Ratings for December
# MAGIC We filter the ratings dataset to only include ratings within the month of December.

# COMMAND ----------

# Filter ratings within December
december_ratings_df = ratings_df.filter(F.col("month") == 12)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Join with Movies Dataset
# MAGIC We join the summary table with the 'movies' dataset to enrich the dataset with the movie name and year.

# COMMAND ----------

# Join with genomes dataset and filter for Christmas genome with high relevance
december_ratings_df = december_ratings_df.join(movies_df, "movieId")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Calculate Average Ratings and Frequencies
# MAGIC We calculate the average rating and rating frequency per movie based on December's ratings. Then filter out movies with less than 100 ratings.

# COMMAND ----------

# Calculate average rating and rating frequency per movie
movie_ratings_summary_df = december_ratings_df.groupBy("movieId", "title", "year").agg(
    F.avg("rating").alias("average_rating"), 
    F.count("rating").alias("rating_count")
).filter(F.col("rating_count") >= 100).orderBy(F.desc("average_rating"))

movie_ratings_summary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Join with Genomes Dataset and Filter for Christmas Genome with High Relevance
# MAGIC We join the summary table with the 'genomes' dataset, filter for tags with high relevance.
# MAGIC

# COMMAND ----------

# Filter genome dataset for tags with high relevance
filtered_genomes_df = genomes_df.filter(
    F.col("relevance_bucket").isin("Very High", "Excellent")
).select("movieId", "tag_names")

# Join with filtered genomes dataset
final_movie_suggestion_df = movie_ratings_summary_df.join(
    filtered_genomes_df, "movieId", "left"
)

# Display the final suggestion
final_movie_suggestion_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Visualize the Results
# MAGIC Let's create some visualizations to better understand the data.

# COMMAND ----------

# Import necessary libraries for visualization
import matplotlib.pyplot as plt

# Visualize average ratings
plt.figure(figsize=(10, 6))
plt.bar(final_movie_suggestion_df.toPandas()["tag_names"], final_movie_suggestion_df.toPandas()["average_rating"], color='skyblue')
plt.xlabel('Movie ID')
plt.ylabel('Average Rating')
plt.title('Average Ratings of Christmas Movies')
plt.show()

# Visualize rating counts
plt.figure(figsize=(10, 6))
plt.bar(final_movie_suggestion_df.toPandas()["tag_names"], final_movie_suggestion_df.toPandas()["rating_count"], color='lightgreen')
plt.xlabel('Movie ID')
plt.ylabel('Rating Count')
plt.title('Rating Counts of Christmas Movies')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook analyzes December's ratings, calculates average ratings and rating frequencies per movie, filters for Christmas genome tags with high relevance, and provides the ultimate Christmas movie suggestion. Visualizations have been created to provide insights into the data. Please ensure to update the file paths as necessary and make use of Databricks' powerful visualization capabilities.