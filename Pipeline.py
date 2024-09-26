# Databricks notebook source
from pyspark.sql.functions import col, from_json, to_timestamp, avg, count, window, date_format
from pyspark.sql.types import StructType, StringType, DoubleType
import dlt

# Define the schema for the Kafka data
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
    .add("ComplaintID",DoubleType())

# BRONZE LAYER
@dlt.table(
  comment="Raw data ingested from Kafka topic: 'projecttest'.",
  table_properties={
    "quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bronze_data_new():
    kafka_df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "13.233.43.120:9092")
      .option("subscribe", "projecttest")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load())
  
    # Cast value to string and parse the JSON structure
    parsed_data = kafka_df.selectExpr("CAST(value AS STRING)") \
                          .select(from_json(col("value"), schema).alias("data")).select("data.*")
    
    # Write to Bronze Delta Table
    parsed_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/delta/checkpoints/bronze_data_new") \
        .option("mergeSchema", "true") \
        .table("bronze_data_new") 

    return parsed_data


# SILVER LAYER
@dlt.table(
  comment="Cleaned and filtered data from Bronze, partitioned by 'Region'.",
  table_properties={
    "quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid_signal_strength", "SignalStrength BETWEEN -120 AND -60")
@dlt.expect_or_drop("valid_call_drop_rate", "CallDropRate BETWEEN 0 AND 10")
@dlt.expect_or_drop("valid_data_transfer_speed", "DataTransferSpeed BETWEEN 20 AND 100")
def silver_data_new():
    bronze_data = dlt.read_stream("bronze_data_new")
    
    # Convert the timestamp field and clean the data
    cleaned_data = bronze_data.withColumn("Timestamp", to_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss"))
    
    # Drop rows with null values in critical columns like CustomerID and DeviceID
    cleaned_data = cleaned_data.na.drop(subset=["CustomerID", "DeviceID"])
    
    # Optional: Drop rows with null values in other non-critical columns
    cleaned_data = cleaned_data.na.drop(subset=[
        "SignalStrength", "CallDropRate", "DataTransferSpeed", "CustomerSatisfaction", "Region", "Timestamp", "ComplaintType"
    ])
    
    # Write to Silver Delta Table
    cleaned_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/delta/checkpoints/silver_data_new") \
        .option("mergeSchema", "true") \
        .table("silver_data_new") 

    return cleaned_data

from pyspark.sql.functions import date_format, year, col


# Dimension Table: Customers (Real-time)
@dlt.table(
  comment="Customer dimension storing customer satisfaction ratings in real-time."
)
def dim_customers_new():
    silver_data = dlt.read_stream("silver_data_new")  # Streaming read
    dim_customers_data = silver_data.select("CustomerID", "CustomerSatisfaction").distinct()

    # Write to Hive metastore via Delta
    dim_customers_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/delta/checkpoints/dim_customers_new") \
        .option("mergeSchema", "true") \
        .table("dim_customers_new")

    return dim_customers_data


# Dimension Table: Devices (Real-time)
@dlt.table(
  comment="Device dimension storing region information in real-time."
)
def dim_devices():
    silver_data = dlt.read_stream("silver_data_new")  # Streaming read
    dim_devices_data = silver_data.select("DeviceID", "Region").distinct()

    dim_devices_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/delta/checkpoints/dim_devices") \
        .option("mergeSchema", "true") \
        .table("dim_devices")

    return dim_devices_data

# Dimension Table: Complaints (Real-time)
@dlt.table(
  comment="real time complaints data ."
)
def dim_complaints_new():
    silver_data = dlt.read_stream("silver_data_new")  # Streaming read
    dim_complaints_data = silver_data.select("ComplaintID", "ComplaintType","Timestamp").distinct()

    dim_complaints_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/delta/checkpoints/dim_complaints_new") \
        .option("mergeSchema", "true") \
        .table("dim_complaints_new")

    return dim_complaints_data
  
@dlt.table(
  comment="Fact table storing aggregated telecom metrics for each customer in real-time."
)
def fact_telecom_metrics_new():
    silver_data = dlt.read_stream("silver_data_new")  # Streaming read
  
    
    # Repartition the data to prevent skew
    repartitioned_data = silver_data.repartition("CustomerID", "DeviceID")
    
    # Aggregating metrics such as average signal strength, call drop rate, etc. in real-time
    fact_metrics_data = repartitioned_data.groupBy("CustomerID", "DeviceID", "ComplaintID") \
        .agg(
            avg("SignalStrength").alias("AvgSignalStrength"),  # Calculate average signal strength
            avg("CallDropRate").alias("AvgCallDropRate"),      # Calculate average call drop rate
            avg("DataTransferSpeed").alias("AvgDataTransferSpeed"),  # Average data transfer speed
            count("ComplaintType").alias("TotalComplaints")    # Count the total number of complaints
        )

    # Write to Hive metastore via Delta
    fact_metrics_data.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "/delta/checkpoints/fact_telecom_metrics_new") \
        .option("mergeSchema", "true") \
        .table("fact_telecom_metrics_new")

    return fact_metrics_data

