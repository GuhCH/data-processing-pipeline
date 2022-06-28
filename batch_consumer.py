from kafka import KafkaConsumer
from json import loads

batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest"
)

batch_consumer.subscribe(topics='PinterestData')

for message in batch_consumer:
    print(message)

