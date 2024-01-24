# Databricks notebook source
# MAGIC %md
# MAGIC ##### Import relevant libraries:

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

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

# MAGIC %md
# MAGIC ##### Mount and display S3 bucket:

# COMMAND ----------

#Used only when mounting for first time - comment out if running the whole notebook from scratch.
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false 

# COMMAND ----------

# MAGIC %md
# MAGIC Load the post, location and user data into dataframes. 

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
# MAGIC Inspect the first five rows of the pin dataframe to identify structure and begin diagnosing cleanup issues. 

# COMMAND ----------

display(pin_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Begin data cleaning:

# COMMAND ----------

pin_df = pin_df.dropDuplicates()

# Remove unnecessary string content:
pin_df = pin_df.withColumn("follower_count", regexp_replace(col("follower_count"), r"(\d)k$", "$1"))
pin_df = pin_df.withColumn("save_location", regexp_replace(col("save_location"), r"^Local save in", ""))

# Recast dtypes:
pin_df = pin_df.withColumn("follower_count", pin_df["follower_count"].cast("int"))
pin_df = pin_df.withColumnRenamed("index", "ind")
pin_df = pin_df.withColumn("ind", pin_df["ind"].cast("int"))
pin_df = pin_df.withColumn("downloaded", pin_df["downloaded"].cast("boolean"))

pin_df = pin_df.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category", "downloaded")
pin_df = pin_df.orderBy("ind")
pin_df = pin_df.withColumn("ind", monotonically_increasing_id())

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pin_df

# COMMAND ----------

null_counts = [pin_df.filter(col(column).isNull()).count() for column in pin_df.columns]

for column, count in zip(pin_df.columns, null_counts):
    print(f"Number of null values in column '{column}': {count}")

pin_df = pin_df.replace({"": None})

# COMMAND ----------

null_counts = [pin_df.filter(col(column).isNull()).count() for column in pin_df.columns]

for column, count in zip(pin_df.columns, null_counts):
    print(f"Number of null values in column '{column}': {count}")


# COMMAND ----------

display(geo_df.limit(5))

# COMMAND ----------

geo_df = geo_df.dropDuplicates()

geo_df = geo_df.withColumn("coordinates", array("latitude", "longitude"))
geo_df = geo_df.drop("latitude", "longitude")

geo_df = geo_df.withColumnRenamed("index", "ind")

geo_df = geo_df.withColumn("timestamp", to_timestamp("timestamp"))

geo_df = geo_df.select("ind", "country", "coordinates", "timestamp")

geo_df = geo_df.orderBy("ind")
geo_df = geo_df.withColumn("ind", monotonically_increasing_id())
geo_df = geo_df.withColumn("ind", geo_df["ind"].cast("int"))

# COMMAND ----------

geo_df.show()

# COMMAND ----------


print(geo_df.printSchema())

# COMMAND ----------

display(user_df.limit(5))

# COMMAND ----------

user_df.dropDuplicates()

user_df = user_df.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
user_df = user_df.drop("first_name", "last_name")

user_df = user_df.withColumn("date_joined", to_timestamp("date_joined"))
user_df = user_df.withColumn("age", user_df["age"].cast("int"))

user_df = user_df.withColumnRenamed("index", "ind")

user_df = user_df.select("ind", "user_name", "age", "date_joined")

user_df = user_df.orderBy("ind")
user_df = user_df.withColumn("ind", monotonically_increasing_id())
user_df = user_df.withColumn("ind", user_df["ind"].cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To Do:
# MAGIC
# MAGIC - check null values - particularly in the array in the coordinates column in the geo-df (see printSchema()).
# MAGIC - double check all dtypes for accuracy - should be ok but no longs etc. 
# MAGIC - reformatting of notebook structure - more Md cells etc. for explanation and guidence of process. 
