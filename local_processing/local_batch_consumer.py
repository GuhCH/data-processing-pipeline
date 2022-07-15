from kafka import KafkaConsumer
import json
import os
import uuid
import datetime

batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: json.loads(message),
    auto_offset_reset="earliest"
)

batch_consumer.subscribe(topics='PinterestData')

for message in batch_consumer:
    folder = datetime.date.today().strftime('%Y-%m-%d')
    os.makedirs(f'./data/{folder}', exist_ok=True)
    with open(f'./data/{folder}/{str(uuid.uuid4())}.json', 'w') as f:
        f.write(json.dumps(message.value))