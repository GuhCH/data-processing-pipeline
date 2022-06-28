from kafka import KafkaConsumer
import json

streaming_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: json.loads(message),
    auto_offset_reset="earliest"
)

streaming_consumer.subscribe(topics='PinterestData')

for message in streaming_consumer:
    print(message)