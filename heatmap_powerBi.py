# Databricks notebook source
import requests
import json
from pyspark.sql import HiveContext

# Set up Hive Context
hive_context = HiveContext(sc)

# Query your Databricks Hive Metastore table (adjust table name and columns as needed)
df = hive_context.sql("SELECT * FROM hive_metastore.default.heatmap_data_new")

# Convert DataFrame to Pandas for easy row iteration
pandas_df = df.toPandas()

# Power BI Streaming API URL (replace with your own Push URL)
power_bi_api_url = "https://api.powerbi.com/beta/dce87315-8ffa-4a01-ab40-0de5a7214b2f/datasets/8fb03f7e-6dcf-4283-b892-8749bc230c21/rows?experience=power-bi&key=86ESB6O9n1H8IBqZSSnXPKQCYySRXs8hSSqK8qhLFBsc3QhJ07ylEwI15GVrfRTnCMnBCdHphgNDmZ8ao4GMog%3D%3D"

# Function to push each row to Power BI Streaming API
def push_data_to_powerbi(row):
    data = [{
        'DeviceID': str(row['DeviceID']),
        'Latitude': row['Latitude'],
        'Longitude': row['Longitude'],
        'SignalStrength': row['SignalStrength'],
        'Region': str(row['Region']),
        'City': row['City']
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

