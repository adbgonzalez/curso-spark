# weather_producer_openmeteo.py
from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka-1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Coordenadas para Barcelona
LAT = 41.3888
LON = 2.159
CITY = "Barcelona"
API_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current=temperature_2m,windspeed_10m,weathercode"

while True:
    try:
        response = requests.get(API_URL)
        data = response.json()
        current = data.get("current", {})

        payload = {
            "city": CITY,
            "temperature": current.get("temperature_2m"),
            "windspeed": current.get("windspeed_10m"),
            "weathercode": current.get("weathercode"),
            "timestamp": datetime.utcnow().isoformat()
        }
        producer.send("open-meteo-weather", value=payload)
        print("Weather:", payload)
        time.sleep(60)  # frecuencia de actualización
    except Exception as e:
        print("Erro:", e)
        time.sleep(20)
