import requests
import json
from kafka import KafkaProducer
import time

# Kafka config
KAFKA_BROKER = 'localhost:9092'  # Your KRaft broker address
TOPIC_NAME = 'crypto_prices'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# CoinGecko API
COINS = ['bitcoin', 'ethereum', 'dogecoin']
URL = f'https://api.coingecko.com/api/v3/simple/price?ids={",".join(COINS)}&vs_currencies=usd'

try:
    while True:
        response = requests.get(URL)
        if response.status_code == 200:
            data = response.json()
            print(f"Sending to Kafka: {data}")
            producer.send(TOPIC_NAME, value=data)
        else:
            print(f"API Error: {response.status_code}")
        time.sleep(5)
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.close()
