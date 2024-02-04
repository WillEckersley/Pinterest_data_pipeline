# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount the Kinesis streams:

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import urllib

# COMMAND ----------

delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
aws_keys_df = spark.read.format("delta").load(delta_table_path)
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build a function to create Spark DataFrames from Kinesis streams:

# COMMAND ----------

# Create a function to construct DataFrames from the incoming Kinesis streams. 

def create_df(stream_name, schema):
    """Creates a Spark DataFrame from an AWS Kinesis Stream 
       containing json objects.

    Args:
      stream_name (str): the name of the Kinesis stream to be transformed.
      schema (StructType): a StructType specifiying the desired DataFrame schema.

    Returns:
      A Spark DataFrame where the keys of the JSON objects are column names and 
      the values records 
    """
    df = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', stream_name) \
    .option('initialPosition','earliest') \
    .option('region','us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load() \
    .selectExpr("CAST(data as STRING)") \
    .withColumn("parsed_data", from_json(col("data"), schema)) \
    .select(col("parsed_data.*")) 
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schematise and load each DataFrame:

# COMMAND ----------

pin_df_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True), 
    StructField("follower_count", StringType(), True), 
    StructField("tag_list", StringType(), True), 
    StructField("is_image_or_video", StringType(), True), 
    StructField("image_src", StringType(), True), 
    StructField("downloaded", StringType(), True), 
    StructField("save_location", StringType(), True), 
    StructField("category", StringType(), True)
])

pin_df = create_df("streaming-12471ce1b695-pin", pin_df_schema)

# COMMAND ----------

geo_df_schema = StructType([
    StructField("index", IntegerType(), True), 
    StructField("timestamp", TimestampType(), True),
    StructField("latitude", StringType(), True), 
    StructField("longitude", StringType(), True),
    StructField("country", StringType(), True)
])

geo_df = create_df("streaming-12471ce1b695-geo", geo_df_schema)

# COMMAND ----------

user_df_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("date_joined", TimestampType(), True), 
    StructField("first_name", StringType(), True), 
    StructField("last_name", StringType(), True), 
    StructField("age", IntegerType(), True)
])

user_df = create_df("streaming-12471ce1b695-user", user_df_schema)

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

# Recast after reorder and rename.
user_df = user_df.withColumn("ind", user_df["ind"].cast("int"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write the DataFrames to Delta Tables:

# COMMAND ----------

# Create a function to write the DataFrames to Delta tables.

def write_table_to_delta(df, table_name):
    """Writes data from Spark DataFrame to a Delta table.

    Args:
      df (DataFrame): A DataFrame containig the data you wish to write to Delta.
      table_name (str): A string representation of the name of Delta table to write to.

    Returns:
      A stream which writes the contetnt of df to a Delta table with the name <table_name>.

    """
    write_table = (
        df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"tmp/checkpoints/{table_name}")
            .table(table_name)
    )
    return write_table

# COMMAND ----------

write_table_to_delta(pin_df, "12471ce1b695_pin_table")

# COMMAND ----------

write_table_to_delta(geo_df, "12471ce1b695_geo_table")

# COMMAND ----------

write_table_to_delta(user_df, "12471ce1b695_user_table")
