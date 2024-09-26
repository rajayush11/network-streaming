import random
import time
from datetime import datetime, timedelta

# Define possible values for each column
device_ids = [f"Device_{i}" for i in range(1, 50)]
customer_ids = [f"Customer_{i}" for i in range(1, 21)]
regions = ['East', 'North', 'South', 'West']
complaint_types = ['Service Disruption', 'Billing Issue', 'Network Issue', 'Other']
signal_strength_range = (-120, -60)  # Min and max for signal strength
call_drop_rate_range = (0.0, 10.0)   # Min and max for call drop rate
data_transfer_speed_range = (20.0, 100.0)  # Min and max for data transfer speed
satisfaction_range = (1.0, 10.0)     # Min and max for satisfaction

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

def print_data(data):
    print(f"{data['DeviceID']}\t{data['CustomerID']}\t{data['Timestamp']}\t{data['Region']}\t{data['SignalStrength']}\t{data['CallDropRate']}\t{data['DataTransferSpeed']}\t{data['ComplaintType']}\t{data['CustomerSatisfaction']}")

if __name__ == "__main__":
    while True:
        data = generate_random_data()
        print_data(data)
        time.sleep(10)  # Wait for 10 seconds before generating the next entry
