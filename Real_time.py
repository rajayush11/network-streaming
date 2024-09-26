# Databricks notebook source
!pip install kafka-python

# COMMAND ----------

dbutils.fs.rm("/delta/checkpoints/dim_customers_new", recurse=True)

# COMMAND ----------

dbutils.fs.rm("/delta/checkpoints/dim_devices_new1", recurse=True)

# COMMAND ----------

dbutils.fs.rm("/delta/checkpoints/dim_complaints_new", recurse=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/delta/checkpoints/fact_telecom_metrics_new", recurse=True)

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Kafka DataFrame
kafka_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "	13.233.43.120:9092")
    .option("subscribe", "projecttest")
    .option("failOnDataLoss", "false")
    .load())

# Cast value to string and parse JSON
schema = StructType() \
    .add("DeviceID", StringType()) \
    .add("CustomerID", StringType()) \
    .add("Timestamp", StringType()) \
    .add("Region", StringType()) \
    .add("SignalStrength", DoubleType()) \
    .add("CallDropRate", DoubleType()) \
    .add("DataTransferSpeed", DoubleType()) \
    .add("ComplaintType", StringType()) \
    .add("CustomerSatisfaction", DoubleType()) \
    .add("ComplaintID", DoubleType())

parsed_data = kafka_df.selectExpr("CAST(value AS STRING)") \
                      .select(from_json(col("value"), schema).alias("data")) \
                      .select("data.*")

# Set the stop timeout configuration
spark.conf.set("spark.sql.streaming.stopTimeout", "120000")  # Increase timeout to 2 minutes

parsed_data.writeStream \
           .format("delta") \
           .outputMode("append") \
           .option("checkpointLocation", "/delta/checkpoints/bronze_test1") \
           .option("mergeSchema", "true") \
           .table("bronze_test")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE silver_data;

# COMMAND ----------

