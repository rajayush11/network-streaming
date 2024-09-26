from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timedelta
 
# Kafka Producer configuration
producer = KafkaProducer(
    bootstrap_servers='43.204.235.252:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization
)
 
# Define possible values for each column
device_ids = [f"Device_{i}" for i in range(1, 50)]
customer_ids = [f"Customer_{i}" for i in range(1, 21)]
regions = ['East', 'North', 'South', 'West']i
complaint_types = ['Service Disruption', 'Billing Issue', 'Network Issue', 'Other']
signal_strength_range = (-120, -60)  # In dBm (decibel-milliwatts)
data_transfer_speed_range = (20.0, 100.0)  # Mbps
satisfaction_range = (1.0, 10.0)  # Scale from 1 to 10
 
# Random ComplaintID range
complaint_id_range = (1, 99999)
 
def generate_correlated_data():
    # Randomly select region and device
    region = random.choice(regions)
    device_id = random.choice(device_ids)
    customer_id = random.choice(customer_ids)
    
    # Generate a random ComplaintID
    complaint_id = random.randint(*complaint_id_range)
    
    # Signal strength: Some regions have consistently weaker signals
    if region == 'East':
        signal_strength = random.uniform(-120, -90)  # Poor signal region
    else:
        signal_strength = random.uniform(-85, -60)   # Good signal region
    
    # Call drop rate: Correlated with signal strength (lower signal -> higher drop rate)
    if signal_strength < -100:
        call_drop_rate = random.uniform(0.5, 1.0)  # High drop rate for low signal
    else:
        call_drop_rate = random.uniform(0.0, 0.4)  # Low drop rate for strong signal
    
    # Data transfer speed: Correlated with signal strength (higher signal -> higher speed)
    if signal_strength < -100:
        data_transfer_speed = random.uniform(20.0, 50.0)  # Slow data transfer for poor signal
    else:
        data_transfer_speed = random.uniform(50.0, 100.0)  # Fast data transfer for good signal
    
    # Complaint type: If signal strength is poor or call drop rate is high, the complaint is more likely to be a network issue
    if signal_strength < -95 or call_drop_rate > 0.7:
        complaint_type = 'Network Issue'
    elif call_drop_rate > 0.5:
        complaint_type = 'Service Disruption'
    else:
        complaint_type = random.choice(complaint_types)
    
    # Customer satisfaction: Correlated with signal strength, call drop rate, and complaint type
    if complaint_type in ['Network Issue', 'Service Disruption']:
        customer_satisfaction = random.uniform(1.0, 4.0)  # Low satisfaction for network issues
    else:
        customer_satisfaction = random.uniform(7.0, 10.0)  # High satisfaction otherwise
    
    # Generate timestamp within the last day
    timestamp = (datetime.now() - timedelta(seconds=random.randint(0, 100000))).strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        "ComplaintID": complaint_id,
        "DeviceID": device_id,
        "CustomerID": customer_id,
        "Timestamp": timestamp,
        "Region": region,
        "SignalStrength": round(signal_strength, 2),
        "CallDropRate": round(call_drop_rate, 2),
        "DataTransferSpeed": round(data_transfer_speed, 2),
        "ComplaintType": complaint_type,
        "CustomerSatisfaction": round(customer_satisfaction, 2)
    }
 
if __name__ == "__main__":
    while True:
        data = generate_correlated_data()
        producer.send('projecttest', value=data)  # Send data to 'first' topic
        print(f"Data sent to Kafka: {data}")
        time.sleep(2)  # Wait for 5 seconds before sending the next data point