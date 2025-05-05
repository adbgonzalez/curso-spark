from kafka import KafkaProducer
from json import dumps
import time
import requests
from datetime import datetime

# Indicamos las localizaciones (nombre, latutitud y longitud) de las cuales queremos obtener datos.
cities = [
    {"name": "Santiago", "latitude": 42.8782, "longitude": -8.5448},
    {"name": "Madrid", "latitude": 40.4168, "longitude": -3.7038},
    {"name": "New York", "latitude": 40.7128, "longitude": -74.0060},
]
# Creamos un KafkaProducer indicando el servidor de kafka y la codificación
producer = KafkaProducer(
    bootstrap_servers=['kafka-1:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
# Bucle infinito
while True:
    for city in cities: #Para cada localización
        url = f"https://api.open-meteo.com/v1/forecast?latitude={city['latitude']}&longitude={city['longitude']}&current_weather=true" #URL de la petición
        try:
            response = requests.get(url)
            weather_data = response.json()

            if "current_weather" in weather_data:
                data = weather_data["current_weather"]
                data["city"] = city["name"]
                data["local_timestamp"] = datetime.utcnow().isoformat()  # Nueva marca de tiempo UTC

                producer.send('open-meteo-weather', value=data)
                print(f"[{datetime.now()}] Enviado: {data}")

        except Exception as e:
            print(f"Error obteniendo datos de {city['name']}: {e}")

    time.sleep(10)  # Enviamos eventos cada 10 segundos.
