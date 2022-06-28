from kafka import KafkaConsumer
from json import loads

streaming_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest"
)

streaming_consumer.subscribe(topics='PinterestData')

for message in streaming_consumer:
    print(message)