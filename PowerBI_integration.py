# Databricks notebook source
import requests
import json
from pyspark.sql import HiveContext

# Set up Hive Context
hive_context = HiveContext(sc)

# Query your Databricks Hive Metastore table (adjust table name and columns as needed)
df = hive_context.sql("SELECT * FROM hive_metastore.default.dim_complaints_new")

# Convert DataFrame to Pandas for easy row iteration
pandas_df = df.toPandas()

# Power BI Streaming API URL (replace with your own Push URL)
power_bi_api_url = "https://api.powerbi.com/beta/dce87315-8ffa-4a01-ab40-0de5a7214b2f/datasets/4fafb655-1fc3-4c22-8081-486d399b9724/rows?experience=power-bi&selectedWorkspaceObjectId=me&key=p5vtugoXI6PC0ygaajR80T7xi62c5%2F5Cyjlq2tw8IMupJ60W%2FIhPKUTGliESekgP32tfqkV3wrHcHFtnLxfe5g%3D%3D"

# Function to push each row to Power BI Streaming API
def push_data_to_powerbi(row):
    data = [{
        'ComplaintID': str(row['ComplaintID']),
        'ComplaintType': row['ComplaintType'],
        'Timestamp': str(row['Timestamp'])
    }]
    
    # Send the data to Power BI Streaming API
    headers = {'Content-Type': 'application/json'}
    response = requests.post(power_bi_api_url, data=json.dumps(data), headers=headers)
    return response.status_code

# Iterate through each row and push to Power BI
for index, row in pandas_df.iterrows():
    response_code = push_data_to_powerbi(row)
    print(f"Pushed row {index} with status code: {response_code}")


# COMMAND ----------

import requests
import json
from pyspark.sql import HiveContext

# Set up Hive Context
hive_context = HiveContext(sc)

# Query your Databricks Hive Metastore table (adjust table name and columns as needed)
df_customers = hive_context.sql("SELECT * FROM hive_metastore.default.dim_customers_new")

# Convert DataFrame to Pandas for easy row iteration
pandas_df_customers = df_customers.toPandas()

# Power BI Streaming API URL (replace with your own Push URL for dim_customers_new)
power_bi_api_url_customers = "https://api.powerbi.com/beta/dce87315-8ffa-4a01-ab40-0de5a7214b2f/datasets/5effd6c5-ff23-4e70-9808-e92679c9c91a/rows?experience=power-bi&key=TAMuJNSCl5pAdeH9whFrEwAsuyr1%2Bv%2BxT73HCnMXETfZWT6Eq%2Ff%2FfK3LQYEivM7lggnTkQLpOA5jsqMomuK0DQ%3D%3D"

# Function to push each row to Power BI Streaming API
def push_data_to_powerbi_customers(row):
    data = [{
        'CustomerID': str(row['CustomerID']),
        'CustomerSatisfaction': row['CustomerSatisfaction']
    }]
    
    # Send the data to Power BI Streaming API
    headers = {'Content-Type': 'application/json'}
    response = requests.post(power_bi_api_url_customers, data=json.dumps(data), headers=headers)
    return response.status_code

# Iterate through each row and push to Power BI
for index, row in pandas_df_customers.iterrows():
    response_code = push_data_to_powerbi_customers(row)
    print(f"Pushed row {index} with status code: {response_code}")


# COMMAND ----------

import requests
import json
from pyspark.sql import HiveContext

# Set up Hive Context
hive_context = HiveContext(sc)

# Query your Databricks Hive Metastore table (adjust table name and columns as needed)
df_devices = hive_context.sql("SELECT * FROM hive_metastore.default.dim_devices_new")

# Convert DataFrame to Pandas for easy row iteration
pandas_df_devices = df_devices.toPandas()

# Power BI Streaming API URL (replace with your own Push URL for devices)
power_bi_api_url_devices = "https://api.powerbi.com/beta/dce87315-8ffa-4a01-ab40-0de5a7214b2f/datasets/97e3ba79-0306-41df-b9c8-95edab5e8e20/rows?experience=power-bi&key=4ue763bWm12uIQgkVD7KvXsTJgPN%2F5p4VAka9Q90lBfVPPKVT7SW2DjdJIXUXtHAaJFfueduYD39UeHiDDNzRw%3D%3D"

# Function to push each row to Power BI Streaming API
def push_data_to_powerbi_devices(row):
    data = [{
        'DeviceID': str(row['DeviceID']),
        'Region': row['Region']
    }]
    
    # Send the data to Power BI Streaming API
    headers = {'Content-Type': 'application/json'}
    response = requests.post(power_bi_api_url_devices, data=json.dumps(data), headers=headers)
    return response.status_code

# Iterate through each row and push to Power BI
for index, row in pandas_df_devices.iterrows():
    response_code = push_data_to_powerbi_devices(row)
    print(f"Pushed row {index} with status code: {response_code}")


# COMMAND ----------

import requests
import json
from pyspark.sql import HiveContext

# Set up Hive Context
hive_context = HiveContext(sc)

# Query your Databricks Hive Metastore table (adjust table name and columns as needed)
df = hive_context.sql("SELECT * FROM hive_metastore.default.fact_telecom_metrics_new")

# Convert DataFrame to Pandas for easy row iteration
pandas_df = df.toPandas()

# Power BI Streaming API URL (replace with your own Push URL)
power_bi_api_url = "https://api.powerbi.com/beta/dce87315-8ffa-4a01-ab40-0de5a7214b2f/datasets/f1b372ca-b89e-401b-882d-87d18695dbc0/rows?experience=power-bi&key=zpv%2FjDJfTdhantCpgs73q9d4AAkBdoqX0H%2BZfaugamrDSH0s2XEYmHcwuhSTq7H6HVJ5vSgeeEU1awjwpT5s5g%3D%3D"

# Function to push each row to Power BI Streaming API
def push_data_to_powerbi(row):
    data = [{
        'CustomerID': str(row['CustomerID']),
        'DeviceID': str(row['DeviceID']),
        'AvgSignalStrength': row['AvgSignalStrength'],
        'AvgCallDropRate': row['AvgCallDropRate'],
        'AvgDataTransferSpeed': row['AvgDataTransferSpeed'],
        'TotalComplaints': int(row['TotalComplaints'])
    }]
    
    # Send the data to Power BI Streaming API
    headers = {'Content-Type': 'application/json'}
    response = requests.post(power_bi_api_url, data=json.dumps(data), headers=headers)
    return response.status_code

# Iterate through each row and push to Power BI
for index, row in pandas_df.iterrows():
    response_code = push_data_to_powerbi(row)
    print(f"Pushed row {index} with status code: {response_code}")


# COMMAND ----------

import time
import requests
import json
from pyspark.sql import HiveContext

# Set up Hive Context
hive_context = HiveContext(sc)

# Perform the join across fact and dimension tables, and cast ComplaintID to string
composite_df = hive_context.sql("""
SELECT 
    f.CustomerID, f.DeviceID, f.AvgSignalStrength, f.AvgCallDropRate, f.AvgDataTransferSpeed, f.TotalComplaints, 
    c.CustomerSatisfaction, 
    d.Region, 
    co.ComplaintType, 
    CAST(co.ComplaintID AS STRING) AS ComplaintID,  -- Cast ComplaintID to String
    co.Timestamp
FROM 
    hive_metastore.default.fact_telecom_metrics_new f
JOIN 
    hive_metastore.default.dim_customers_new c ON f.CustomerID = c.CustomerID
JOIN 
    hive_metastore.default.dim_devices_new d ON f.DeviceID = d.DeviceID
JOIN 
    hive_metastore.default.dim_complaints_new co ON f.ComplaintID = co.ComplaintID
""")

# Convert to Pandas DataFrame for pushing to Power BI
composite_pandas_df = composite_df.toPandas()

# Function to batch rows and push to Power BI Streaming API
def push_data_to_powerbi_batch(data_batch, retries=3, delay=2):
    api_url = "https://api.powerbi.com/beta/dce87315-8ffa-4a01-ab40-0de5a7214b2f/datasets/16f8df0c-82a4-4e09-851b-5be2494088be/rows?experience=power-bi&key=u8LSOL1anLa%2B2Rl%2FAcgKRu1pN0VpkB1uaTo070CyMNcwr9EPFMUvRtA5TO5nUF50sRxQW7IG0PlSA2EuDz24YQ%3D%3D"
    headers = {'Content-Type': 'application/json'}
    
    for attempt in range(retries):
        response = requests.post(api_url, headers=headers, data=json.dumps(data_batch))
        
        if response.status_code == 200:
            print(f"Batch pushed successfully with status code: 200")
            break
        elif response.status_code == 429:
            print(f"Rate limit reached, retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            print(f"Failed to push batch: {response.status_code} - {response.text}")
            break

# Convert DataFrame rows to JSON and batch the data
def batch_and_push_data(df, batch_size=10):
    data_batch = []
    
    for index, row in df.iterrows():
        data = {
            'CustomerID': str(row['CustomerID']),
            'DeviceID': str(row['DeviceID']),
            'AvgSignalStrength': row['AvgSignalStrength'],
            'AvgCallDropRate': row['AvgCallDropRate'],
            'AvgDataTransferSpeed': row['AvgDataTransferSpeed'],
            'TotalComplaints': int(row['TotalComplaints']),
            'CustomerSatisfaction': row['CustomerSatisfaction'],
            'Region': row['Region'],
            'ComplaintType': row['ComplaintType'],
            'ComplaintID': str(row['ComplaintID']),  # ComplaintID as String
            'Timestamp': str(row['Timestamp'])
        }
        data_batch.append(data)
        
        if len(data_batch) == batch_size:
            push_data_to_powerbi_batch(data_batch)
            data_batch = []
    
    # Push remaining data
    if data_batch:
        push_data_to_powerbi_batch(data_batch)

# Batch and push data
batch_and_push_data(composite_pandas_df, batch_size=10)

# COMMAND ----------

