from kafka import KafkaProducer
from json import dumps
import requests, time
from datetime import datetime

cities = [
    {"name": "Santiago", "latitude": 42.8782, "longitude": -8.5448},
    {"name": "Madrid", "latitude": 40.4168, "longitude": -3.7038},
]

producer = KafkaProducer(
    bootstrap_servers=['kafka-1:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

while True:
    for city in cities:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={city['latitude']}&longitude={city['longitude']}&current_weather=true"
        try:
            r = requests.get(url)
            data = r.json().get("current_weather", {})
            if data:
                data["city"] = city["name"]
                data["timestamp"] = datetime.utcnow().isoformat()
                producer.send("open-meteo-weather", value=data)
                print(f"[Meteo] {data}")
        except Exception as e:
            print(f"Erro meteo: {e}")
    time.sleep(10)
