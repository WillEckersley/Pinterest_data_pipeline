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
MOUNT_NAME = "/mnt/12471ce1b695-mount"
SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_BUCKET}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount and display S3 bucket:

# COMMAND ----------

display(dbutils.fs.ls("/mnt/12471ce1b695-mount/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false 

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

display(pin_df)

# COMMAND ----------

display(geo_df)

# COMMAND ----------

display(user_df)
