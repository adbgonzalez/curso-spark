# bikes_producer_citybikes.py
from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka-1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "https://api.citybik.es/v2/networks/bicing"

while True:
    try:
        response = requests.get(API_URL)
        data = response.json()
        stations = data['network']['stations']
        for station in stations:
            payload = {
                "station_id": station['id'],
                "name": station['name'],
                "city": data['network']['location']['city'],
                "latitude": station['latitude'],
                "longitude": station['longitude'],
                "free_bikes": station.get('free_bikes', 0),
                "empty_slots": station.get('empty_slots', 0),
                "timestamp": datetime.utcnow().isoformat()
            }
            producer.send("bikes-status", value=payload)
            print("📤 Bike:", payload)
        time.sleep(30)  # espera entre peticións
    except Exception as e:
        print("❌ Erro:", e)
        time.sleep(10)
