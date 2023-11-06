# Databricks notebook source
# MAGIC %md
# MAGIC # Christmas Movie Viewer Prediction
# MAGIC
# MAGIC In this notebook, we aim to determine which Christmas movie will bring the most viewers based on December's ratings. We will join the 'movies' dataset with the 'ratings' dataset, filter ratings within December, calculate average ratings and rating frequencies per movie, summarize the results, join with the 'genomes' dataset, filter on the Christmas genome for tags with high relevance, and finally produce the ultimate Christmas movie suggestion.
# MAGIC
# MAGIC ### **Step 1: Import Datasets and Define Paths**
# MAGIC We start by importing necessary functions and defining the paths to the 'movies' and 'ratings' datasets. The datasets are then loaded into PySpark DataFrames for analysis.

# COMMAND ----------

# In case one of the exercises in part one could not be completed run the relevant cell below to prepare the output for part 2

# COMMAND ----------

# MAGIC %run "Repos/Shared/gtc_data_mesh/Solution/Part 1/Solution Commercial Department"

# COMMAND ----------

# MAGIC %run "Repos/Shared/gtc_data_mesh/Solution/Part 1/Solution Customers Department"

# COMMAND ----------

# MAGIC %run "Repos/Shared/gtc_data_mesh/Solution/Part 1/Solution Genomics Department"

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
# december_ratings_df =

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Join with Movies Dataset
# MAGIC We join the summary table with the 'movies' dataset to enrich the dataset with the movie name and year.

# COMMAND ----------

# Join with genomes dataset and filter for Christmas genome with high relevance
# december_ratings_df =

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Calculate Average Ratings and Frequencies
# MAGIC We determine the average rating and rating frequency for each movie using the ratings from December. Exclude movies with fewer than 100 ratings to ensure a representative sample for the average rating calculation.

# COMMAND ----------

# Calculate average rating and rating frequency per movie
# movie_ratings_summary_df =

# movie_ratings_summary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Join with Genomes Dataset and Filter for Christmas Genome with High Relevance
# MAGIC We join the summary table with the 'genomes' dataset, retaining only MovieIds in the genomes dataset that are tagged as highly relevant or excellent relevance for Christmas.
# MAGIC

# COMMAND ----------

# Join with genomes dataset and filter for MovieIds with high relevance or excellent relevance for the Christmas tag
# filtered_genomes_df =

# Join with filtered genomes dataset
# final_movie_suggestion_df =

# Display the final suggestion
# final_movie_suggestion_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Visualize the Results
# MAGIC Let's create some visualizations to better understand the data.

# COMMAND ----------

# Import necessary libraries for visualization
import matplotlib.pyplot as plt

# Create a dictionary to map unique movie titles to colors
title_colors = {title: plt.cm.tab20(i / len(final_movie_suggestion_df.toPandas()["title"].unique())) for i, title in enumerate(final_movie_suggestion_df.toPandas()["title"].unique())}

# Visualize average ratings with zoomed-in y-axis and specified colors based on titles
plt.figure(figsize=(10, 6))

# Iterate through DataFrame rows and plot bars with corresponding colors
for index, row in final_movie_suggestion_df.toPandas().iterrows():
    title = row['title']
    color = title_colors[title]
    plt.bar(title, row['average_rating'], color=color)

plt.xlabel('Movie Title')
plt.xticks(rotation=45, ha="right")
plt.ylabel('Average Rating')
plt.title('Average Ratings of Christmas Movies')

# Set y-axis limits to zoom in (minimum value to 1.5)
plt.ylim(1.5, max(final_movie_suggestion_df.toPandas()["average_rating"]) + 0.5)

plt.show()


# Visualize rating counts using points with smaller legend and different colors for different titles
plt.figure(figsize=(10, 6))
for title, color in title_colors.items():
    subset_df = final_movie_suggestion_df.toPandas()[final_movie_suggestion_df.toPandas()["title"] == title]
    plt.scatter(subset_df["average_rating"], subset_df["rating_count"], color=color, label=title, alpha=0.7, s=50)

plt.xlabel('Average Rating')
plt.ylabel('Rating Count')
plt.title('Rating Counts of Christmas Movies')

# Move legend to the right side next to the graph
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), prop={'size': 8})

plt.show()



# COMMAND ----------

# MAGIC %md
# MAGIC This notebook analyzes December's ratings, calculates average ratings and rating frequencies per movie, filters for Christmas genome tags with high relevance, and provides the ultimate Christmas movie suggestion. Visualizations have been created to provide insights into the data. Please ensure to update the file paths as necessary and make use of Databricks' powerful visualization capabilities.
