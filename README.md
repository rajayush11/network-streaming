# network-streaming

The goal is to stream real-time network metrics such as signal strength and call drop rates, process them in Databricks, and visualize the data in Power BI.


Tech Stack

1. Apache Kafka: Used for real-time stream ingestion of network performance metrics.
2. Databricks: Used for processing streaming data with Structured Streaming.
3. Delta Lake: Storage layer for real-time data with ACID properties.
4. Power BI: Visualize streaming data in real-time dashboards.
5. Python: Used for streaming ETL scripts.


Step-by-Step Solution

1. Data Ingestion Using Kafka
Set up Apache Kafka to stream network performance data (e.g., signal strength, call drop rates) from various network devices. Kafka will act as the real-time ingestion layer for data sources such as network routers and devices.

2. Real-Time Data Processing in Databricks
Use Databricks Structured Streaming to process data as it arrives from Kafka. Apply business rules such as detecting network anomalies (e.g., signal drops or high call drop rates) and calculating metrics like average signal strength per region.

3. Storage in Delta Lake
Store the processed streaming data in Delta Lake. Use Delta Lake to manage real-time data with ACID transactions. This ensures data consistency while handling high-velocity data streams.

4. Automation using Delta Live Tables(DLT)
The complete pipeline for the deltalake transformation is automated using the delta live table.

   
5. Sending Data to Power BI Using Streaming API
After processing the data, use the Power BI Streaming API to push real-time data to Power BI. Visualize metrics such as signal strength trends and call drop rates in real-time. Configure dashboards to update dynamically as new data is streamed into Power BI.


Use Case: Real-Time Network Performance Monitoring

This use case involves streaming network performance metrics like signal strength and call drop rates. The data is processed in real-time and stored in Delta Lake. Power BI dashboards visualize the real-time metrics, providing insights into network health and potential outages.


Steps for Docker :

1. Create a File for the project. Save the docker .yml file there.
2. Open cmd in that file and run " docker-compose up " command.
3. After this run " docker-compose ps " command to check if the kafka and zookeeper is up.
4. FOR KAFKA - docker ps for kafka contanier ID. 
5. use " docker exec -it <container_id> /bin/bash " for executing the kafka .
6. " kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 " command for creating a topic inside the container.
7. " kafka-console-producer.sh --topic test --bootstrap-server localhost:9092 " for producer.
8. " kafka-console-consumer.sh --topic test --bootstrap-server localhost:9092 --from-beginning " for consumer.
