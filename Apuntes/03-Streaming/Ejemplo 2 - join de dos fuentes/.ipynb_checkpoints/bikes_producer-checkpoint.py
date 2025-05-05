import requests
import time
import json
from kafka import KafkaProducer
from datetime import datetime

# Kafka config
producer = KafkaProducer(
    bootstrap_servers='kafka-1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Lista de redes (podes engadir máis)
NETWORKS = {
    "bicing": "Barcelona",
    "valenbisi": "Valencia",
    "servici": "Sevilla",
    "tusbic": "Santander",
    "girocleta": "Girona",
    "nbici": "Pamplona"
}

# Consulta e envía datos de todas as redes, en bucle
while True:
    for network_id, city_name in NETWORKS.items():
        try:
            url = f"https://api.citybik.es/v2/networks/{network_id}"
            response = requests.get(url, timeout=10)
            data = response.json()
            stations = data["network"]["stations"]
            timestamp = datetime.utcnow().isoformat()

            for station in stations:
                payload = {
                    "station_id": station.get("id"),
                    "name": station.get("name"),
                    "city": city_name,
                    "latitude": station.get("latitude"),
                    "longitude": station.get("longitude"),
                    "free_bikes": station.get("free_bikes"),
                    "empty_slots": station.get("empty_slots"),
                    "timestamp": timestamp
                }
                producer.send("bikes-status", payload)

            print(f"[{timestamp}] {city_name}: Enviadas {len(stations)} estacións.")
        
        except Exception as e:
            print(f"Erro ao procesar {city_name}: {e}")
    
    # Espera antes de repetir o ciclo completo
    time.sleep(30)

