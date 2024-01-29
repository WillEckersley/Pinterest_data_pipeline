# Databricks notebook source
# MAGIC %md
# MAGIC #### Import relevant libraries:

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount S3 bucket:

# COMMAND ----------

delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

ACCESS_KEY = aws_keys_df.select("Access key ID").collect()[0]["Access key ID"]
SECRET_KEY = aws_keys_df.select("Secret access key").collect()[0]["Secret access key"]
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
AWS_S3_BUCKET = "user-12471ce1b695-bucket"
MOUNT_NAME = "/mnt/12471ce1b695-mount/"
SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_BUCKET}"

# COMMAND ----------

#Used only when mounting for first time - comment out if running the whole notebook from scratch.
#dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the post, location and user data into dataframes:

# COMMAND ----------

pin_topic_location = "/mnt/12471ce1b695-mount/topics/12471ce1b695.pin/partition=0/"

pin_df = spark.read.format("json") \
    .option("inferSchema", "true") \
        .load(pin_topic_location)

# COMMAND ----------

geo_topic_location = "/mnt/12471ce1b695-mount/topics/12471ce1b695.geo/partition=0/"

geo_df = spark.read.format("json") \
    .option("inferSchema", "true") \
        .load(geo_topic_location)

# COMMAND ----------

user_topic_location = "/mnt/12471ce1b695-mount/topics/12471ce1b695.user/partition=0/"

user_df = spark.read.format("json") \
    .option("inferSchema", "true") \
        .load(user_topic_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean pin_df:

# COMMAND ----------

pin_df = pin_df.dropDuplicates()

# List of erroneous entries to remove.
erroneous_entries = ["No description available Story format", "User Info Error", "Image src error" , "User Info Error", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "No Title Data Available"]

# List comp to replace extant null values and empty cells with 'None'.
pin_df = pin_df.select(*[when(col(column) == "", None) \
                         .when(col(column).isNull(), None) \
                         .when(col(column).isin(erroneous_entries), None) \
                         .otherwise(col(column)).alias(column) for column in pin_df.columns]
                       )

# Regex removes/modifies unnecessary string appendages.
pin_df = pin_df.withColumn("save_location", regexp_replace(col("save_location"), r"^Local save in", ""))
pin_df = pin_df.withColumn("follower_count", regexp_replace(col("follower_count"), r"(\d)k$", "$1"))
pin_df = pin_df.withColumn("follower_count", regexp_replace(col("follower_count"), r"(\d)M$", "$1\\000"))

# Recast dtypes.
pin_df = pin_df.withColumn("follower_count", pin_df["follower_count"].cast("int"))
pin_df = pin_df.withColumn("downloaded", pin_df["downloaded"].cast("boolean"))

# Rename and reorder columns.
pin_df = pin_df.withColumnRenamed("index", "ind")
pin_df = pin_df.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category", "downloaded")
pin_df = pin_df.orderBy("ind")

# Recast dtype after renaming and reordering.
pin_df = pin_df.withColumn("ind", pin_df["ind"].cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean geo_df:

# COMMAND ----------

geo_df = geo_df.dropDuplicates()

# Construct new array column 'coordinates' using latitude and longitude; drop latitude and longitude cols.
geo_df = geo_df.withColumn("coordinates", array("latitude", "longitude"))
geo_df = geo_df.drop("latitude", "longitude")

# Recast dtype
geo_df = geo_df.withColumn("timestamp", to_timestamp("timestamp"))

# Rename and Reorder cols.
geo_df = geo_df.withColumnRenamed("index", "ind")
geo_df = geo_df.select("ind", "country", "coordinates", "timestamp")
geo_df = geo_df.orderBy("ind")

# Trim whitespace in 'country' col.
geo_df = geo_df.withColumn("country", trim(geo_df["country"]))

# Recast after reordering and naming.
geo_df = geo_df.withColumn("ind", geo_df["ind"].cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean user_df:

# COMMAND ----------

user_df.dropDuplicates()

# Construct new col 'user_name' using 'first_name' and 'last_name' cols; drop 'first_name' and 'last_name' cols.
user_df = user_df.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
user_df = user_df.drop("first_name", "last_name")

# Recast dtypes.
user_df = user_df.withColumn("date_joined", to_timestamp("date_joined"))
user_df = user_df.withColumn("age", user_df["age"].cast("int"))

# Reorder and rename cols.
user_df = user_df.withColumnRenamed("index", "ind")
user_df = user_df.select("ind", "user_name", "age", "date_joined")
user_df = user_df.orderBy("ind")

# Recast after reorder and rename.
user_df = user_df.withColumn("ind", user_df["ind"].cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analysis:

# COMMAND ----------

pin_df.createOrReplaceTempView("pin_df")
geo_df.createOrReplaceTempView("geo_df")
user_df.createOrReplaceTempView("user_df")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the most popular Pinterest category by country:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH count_df AS(
# MAGIC   SELECT 
# MAGIC     geo_df.country AS country, 
# MAGIC     pin_df.category AS category, 
# MAGIC     COUNT(*) AS category_count, 
# MAGIC     RANK() OVER(PARTITION BY geo_df.country ORDER BY COUNT(*) DESC) AS ranking
# MAGIC   FROM 
# MAGIC     pin_df
# MAGIC   JOIN  
# MAGIC     geo_df
# MAGIC   ON 
# MAGIC     pin_df.ind = geo_df.ind
# MAGIC   GROUP BY 
# MAGIC     geo_df.country, 
# MAGIC     pin_df.category
# MAGIC )
# MAGIC SELECT
# MAGIC   country, 
# MAGIC   category,
# MAGIC   category_count
# MAGIC FROM 
# MAGIC   count_df
# MAGIC WHERE
# MAGIC   ranking = 1
# MAGIC ORDER BY
# MAGIC   category_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find how many posts each category had between 2018 and 2022:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH year_df AS(
# MAGIC   SELECT 
# MAGIC     category, 
# MAGIC     EXTRACT(YEAR FROM timestamp) AS post_year, 
# MAGIC     COUNT(*) AS category_count
# MAGIC   FROM 
# MAGIC     pin_df
# MAGIC   JOIN
# MAGIC     geo_df
# MAGIC   ON 
# MAGIC     pin_df.ind = geo_df.ind
# MAGIC   GROUP BY
# MAGIC     category,
# MAGIC     post_year
# MAGIC   )
# MAGIC SELECT
# MAGIC   post_year, 
# MAGIC   FIRST(category) AS category, 
# MAGIC   SUM(category_count) AS category_count
# MAGIC FROM 
# MAGIC   year_df
# MAGIC WHERE 
# MAGIC   post_year BETWEEN 2018 AND 2022
# MAGIC GROUP BY 
# MAGIC   category, 
# MAGIC   post_year
# MAGIC ORDER BY 
# MAGIC   post_year DESC; 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the user with the most followers for each country:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH follower_df AS (
# MAGIC   SELECT
# MAGIC     geo_df.country AS country, 
# MAGIC     pin_df.poster_name AS poster_name, 
# MAGIC     pin_df.follower_count AS follower_count,
# MAGIC     RANK() OVER(PARTITION BY geo_df.country ORDER BY follower_count DESC) AS ranking
# MAGIC   FROM 
# MAGIC     pin_df
# MAGIC   JOIN
# MAGIC     geo_df
# MAGIC   ON  
# MAGIC     pin_df.ind = geo_df.ind
# MAGIC )
# MAGIC SELECT
# MAGIC   country, 
# MAGIC   poster_name,
# MAGIC   MAX(follower_count) AS follower_count
# MAGIC FROM 
# MAGIC   follower_df
# MAGIC WHERE 
# MAGIC   ranking = 1
# MAGIC GROUP BY
# MAGIC   country,
# MAGIC   poster_name
# MAGIC ORDER BY 
# MAGIC   follower_count;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Based on the above query, find the country with the user with most followers:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH follower_df AS (
# MAGIC   SELECT
# MAGIC     geo_df.country AS country, 
# MAGIC     pin_df.poster_name AS poster_name, 
# MAGIC     pin_df.follower_count AS follower_count,
# MAGIC     RANK() OVER(PARTITION BY geo_df.country ORDER BY follower_count DESC) AS ranking
# MAGIC   FROM 
# MAGIC     pin_df
# MAGIC   JOIN
# MAGIC     geo_df
# MAGIC   ON  
# MAGIC     pin_df.ind = geo_df.ind
# MAGIC )
# MAGIC SELECT
# MAGIC   country, 
# MAGIC   MAX(follower_count) AS follower_count
# MAGIC FROM 
# MAGIC   follower_df
# MAGIC WHERE 
# MAGIC   ranking = 1
# MAGIC GROUP BY
# MAGIC   country,
# MAGIC   poster_name
# MAGIC ORDER BY 
# MAGIC   follower_count DESC
# MAGIC LIMIT 
# MAGIC   1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the most popular category people post to based on the following age groups:
# MAGIC - 18-24
# MAGIC - 25-35
# MAGIC - 36-50
# MAGIC - +50

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN user_df.age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC     WHEN user_df.age BETWEEN 25 AND 35 THEN '25-35'
# MAGIC     WHEN user_df.age BETWEEN 36 AND 50 THEN '36-50'
# MAGIC     ELSE '50+'
# MAGIC   END AS age_group, 
# MAGIC   pin_df.category, 
# MAGIC   COUNT(*) AS category_count
# MAGIC FROM 
# MAGIC   pin_df
# MAGIC JOIN 
# MAGIC   user_df
# MAGIC ON  
# MAGIC   pin_df.ind = user_df.ind
# MAGIC GROUP BY
# MAGIC   age_group,
# MAGIC   pin_df.category
# MAGIC ORDER BY 
# MAGIC   category_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### What is the median follower count for users in the following age groups:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH age_groups_df AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN user_df.age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC       WHEN user_df.age BETWEEN 25 AND 35 THEN '25-35'
# MAGIC       WHEN user_df.age BETWEEN 36 AND 50 THEN '36-50'
# MAGIC       ELSE '50+'
# MAGIC     END AS age_group,
# MAGIC     pin_df.follower_count
# MAGIC   FROM
# MAGIC     pin_df
# MAGIC   JOIN 
# MAGIC     user_df
# MAGIC   ON 
# MAGIC     pin_df.ind = user_df.ind
# MAGIC )
# MAGIC SELECT
# MAGIC   age_group,
# MAGIC   PERCENTILE(follower_count, 0.5) AS median_follower_count
# MAGIC FROM 
# MAGIC   age_groups_df
# MAGIC GROUP BY 
# MAGIC   age_group
# MAGIC ORDER BY 
# MAGIC   median_follower_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Find how many users have joined between 2015 and 2020:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH year_df AS(
# MAGIC SELECT 
# MAGIC   EXTRACT(YEAR FROM date_joined) AS join_year, 
# MAGIC   COUNT(*) OVER(
# MAGIC     PARTITION BY EXTRACT(YEAR FROM date_joined)
# MAGIC   ) AS number_users_joined
# MAGIC FROM 
# MAGIC   user_df
# MAGIC )
# MAGIC SELECT
# MAGIC   join_year, 
# MAGIC   SUM(number_users_joined) AS number_users_joined
# MAGIC FROM 
# MAGIC   year_df
# MAGIC WHERE 
# MAGIC   join_year BETWEEN 2015 AND 2020
# MAGIC GROUP BY  
# MAGIC   join_year;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the median follower count of users have joined between 2015 and 2020:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH year_df AS (
# MAGIC   SELECT 
# MAGIC     EXTRACT(YEAR FROM user_df.date_joined) AS join_year, 
# MAGIC     PERCENTILE(pin_df.follower_count, 0.5) OVER(
# MAGIC       PARTITION BY EXTRACT(YEAR FROM user_df.date_joined)
# MAGIC     ) AS median_follower_count
# MAGIC   FROM 
# MAGIC     user_df
# MAGIC   JOIN 
# MAGIC     pin_df
# MAGIC   ON 
# MAGIC     user_df.ind = pin_df.ind
# MAGIC   )
# MAGIC SELECT
# MAGIC   join_year, 
# MAGIC   median_follower_count
# MAGIC FROM 
# MAGIC   year_df
# MAGIC WHERE 
# MAGIC   join_year BETWEEN 2015 AND 2020
# MAGIC GROUP BY  
# MAGIC   join_year,
# MAGIC   median_follower_count;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS(
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN user_df.age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC       WHEN user_df.age BETWEEN 25 AND 35 THEN '25-35'
# MAGIC       WHEN user_df.age BETWEEN 36 AND 50 THEN '36-50'
# MAGIC       ELSE '50+'
# MAGIC     END AS age_group, 
# MAGIC     EXTRACT(YEAR FROM user_df.date_joined) AS join_year, 
# MAGIC     PERCENTILE(pin_df.follower_count, 0.5) OVER(
# MAGIC         PARTITION BY EXTRACT(YEAR FROM user_df.date_joined), 
# MAGIC         CASE
# MAGIC           WHEN user_df.age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC           WHEN user_df.age BETWEEN 25 AND 35 THEN '25-35'
# MAGIC           WHEN user_df.age BETWEEN 36 AND 50 THEN '36-50'
# MAGIC           ELSE '50+'
# MAGIC         END
# MAGIC       ) AS median_follower_count
# MAGIC     FROM 
# MAGIC       user_df
# MAGIC     JOIN 
# MAGIC       pin_df
# MAGIC     ON 
# MAGIC       user_df.ind = pin_df.ind
# MAGIC   )
# MAGIC SELECT
# MAGIC   age_group, 
# MAGIC   join_year, 
# MAGIC   median_follower_count
# MAGIC FROM 
# MAGIC   cte
# MAGIC WHERE 
# MAGIC   join_year BETWEEN 2015 AND 2020
# MAGIC GROUP BY 
# MAGIC   join_year,
# MAGIC   age_group,
# MAGIC   median_follower_count;
