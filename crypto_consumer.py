import json
from kafka import KafkaConsumer

# Kafka config
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'crypto_prices'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    group_id='crypto-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started...")

try:
    for message in consumer:
        print(f"Received: {message.value}")
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
