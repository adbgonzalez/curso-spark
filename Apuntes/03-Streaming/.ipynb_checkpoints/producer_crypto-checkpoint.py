from kafka import KafkaProducer
from json import dumps
import requests, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['kafka-1:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd,eur"

while True:
    try:
        response = requests.get(url)
        prices = response.json()
        timestamp = datetime.utcnow().replace(microsecond=0).isoformat()

        for coin, values in prices.items():
            event = {
                "coin": coin,
                "usd": values["usd"],
                "eur": values["eur"],
                "timestamp": timestamp
            }
            producer.send("crypto-prices", value=event)
            print(f"[Crypto] {event}")

    except Exception as e:
        print(f"Erro crypto: {e}")
    time.sleep(10)