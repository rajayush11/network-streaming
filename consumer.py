from kafka import KafkaConsumer
import json

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'first',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Start reading at the earliest message
    enable_auto_commit=True,
    group_id='network-consumers',  # Group ID for consumer
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # JSON deserialization
)

print("Waiting for messages from Kafka...")

for message in consumer:
    data = message.value
    print(f"Consumed data from Kafka: {data}")
