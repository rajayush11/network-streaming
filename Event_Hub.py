# Databricks notebook source
# Import necessary libraries
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

# Set up the Event Hub connection string and configurations
connectionString = "Endpoint=sb://ayuraj77hub.servicebus.windows.net/;SharedAccessKeyName=test1;SharedAccessKey=9nTxxxuuFSGcTipzRQu7h9FtwP3sK9Bli+AEhEiVTDU=;EntityPath=producer"

# Configuration for Event Hub
ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# Define schema for the incoming data
eventSchema = StructType([
    StructField("DeviceID", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("SignalStrength", DoubleType(), True),
    StructField("Region", StringType(), True),
    StructField("City", StringType(), True)
])

# Create a streaming DataFrame by reading from the Event Hub
event_stream = spark \
    .readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# Parse the data (assuming it's in JSON format) using the schema
parsed_stream = event_stream \
    .select(from_json(col("body").cast("string"), eventSchema).alias("data")) \
    .select("data.*")

# Write the streaming data into a Delta table for storage
parsed_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/delta/checkpoints/heatmap_data_new") \
        .option("mergeSchema", "true") \
        .table("heatmap_data_new") 
