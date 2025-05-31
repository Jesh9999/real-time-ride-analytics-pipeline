from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

locations = ['Downtown', 'Airport', 'Uptown', 'Midtown', 'Harbor']

while True:
    ride_event = {
        'timestamp': datetime.utcnow().isoformat(),
        'pickup': random.choice(locations),
        'dropoff': random.choice(locations),
        'distance_km': round(random.uniform(1, 25), 2),
        'fare_usd': round(random.uniform(5, 50), 2)
    }
    print("Sending:", ride_event)
    producer.send('ride_data', ride_event)
    time.sleep(2)
