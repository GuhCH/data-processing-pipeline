from kafka import KafkaConsumer
import json
import os
import uuid
import datetime

folder = datetime.date.today().strftime('%Y-%m-%d')

batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: json.loads(message),
    auto_offset_reset="earliest"
)

batch_consumer.subscribe(topics='PinterestData')

os.makedirs(f'./data/{folder}', exist_ok=True)
for message in batch_consumer:
    with open(f'./data/{folder}/{str(uuid.uuid4())}.json', 'w') as f:
        f.write(json.dumps(message.value))