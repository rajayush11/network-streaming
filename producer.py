from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timedelta

# Kafka Producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization
)

# Define possible values for each column
device_ids = [f"Device_{i}" for i in range(1, 50)]
customer_ids = [f"Customer_{i}" for i in range(1, 21)]
regions = ['East', 'North', 'South', 'West']
complaint_types = ['Service Disruption', 'Billing Issue', 'Network Issue', 'Other']
signal_strength_range = (-120, -60)
call_drop_rate_range = (0.0, 10.0)
data_transfer_speed_range = (20.0, 100.0)
satisfaction_range = (1.0, 10.0)

def generate_random_data():
    return {
        "DeviceID": random.choice(device_ids),
        "CustomerID": random.choice(customer_ids),
        "Timestamp": (datetime.now() - timedelta(seconds=random.randint(0, 100000))).strftime("%Y-%m-%d %H:%M:%S"),
        "Region": random.choice(regions),
        "SignalStrength": round(random.uniform(*signal_strength_range), 2),
        "CallDropRate": round(random.uniform(*call_drop_rate_range), 2),
        "DataTransferSpeed": round(random.uniform(*data_transfer_speed_range), 2),
        "ComplaintType": random.choice(complaint_types),
        "CustomerSatisfaction": round(random.uniform(*satisfaction_range), 2)
    }

if __name__ == "__main__":
    while True:
        data = generate_random_data()
        producer.send('first', value=data)  # Send data to 'network-metrics' topic
        print(f"Data sent to Kafka: {data}")
        time.sleep(5)  # Wait for 5 seconds before sending the next data point
