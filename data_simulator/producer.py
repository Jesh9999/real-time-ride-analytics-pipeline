from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

pickup_zones = ['Downtown', 'Uptown', 'Midtown', 'Airport', 'Harbor']
dropoff_zones = ['Downtown', 'Uptown', 'Midtown', 'Airport', 'Harbor']

def generate_ride():
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'pickup': random.choice(pickup_zones),
        'dropoff': random.choice(dropoff_zones),
        'distance_km': round(random.uniform(1, 25), 2),
        'fare_usd': round(random.uniform(5, 60), 2)
    }

while True:
    ride_event = generate_ride()
    print("Sending:", ride_event)
    producer.send('ride_data', ride_event)
    time.sleep(2)
