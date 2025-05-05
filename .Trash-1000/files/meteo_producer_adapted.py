import requests
import time
import json
from kafka import KafkaProducer
from datetime import datetime

# Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka-1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Coordenadas das cidades (coinciden coas do fluxo de bikes)
CITIES = {
    "Barcelona": {"lat": 41.3851, "lon": 2.1734},
    "Valencia": {"lat": 39.4699, "lon": -0.3763},
    "Girona": {"lat": 41.9794, "lon": 2.8214},
    "Pamplona": {"lat": 42.8125, "lon": -1.6458}
}

# Bucle infinito: cada 20 segundos envia datos do tempo
while True:
    for city, coords in CITIES.items():
        try:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={coords['lat']}&longitude={coords['lon']}"
                f"&current=temperature_2m,wind_speed_10m"
            )
            response = requests.get(url, timeout=5)
            data = response.json()
            current = data.get("current", {})

            payload = {
                "city": city,
                "temperature": current.get("temperature_2m"),
                "windspeed": current.get("wind_speed_10m"),
                "timestamp": datetime.utcnow().isoformat()
            }

            producer.send("open-meteo-weather", payload)
            print(f"[{payload['timestamp']}] Enviado tempo de {city}: {payload}")

        except Exception as e:
            print(f"Erro ao consultar {city}: {e}")
    
    time.sleep(20)
