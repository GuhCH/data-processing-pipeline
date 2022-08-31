from kafka import KafkaConsumer
import json
import os
import uuid
import datetime
import logging

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('logs/local_batch_consumer.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

try:
    batch_consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",    
        value_deserializer=lambda message: json.loads(message),
        auto_offset_reset="earliest"
    )
    logger.info('Connected to Kafka consumer at localhost on port 9092')
except Exception as e:
    logger.error(e)

try:
    batch_consumer.subscribe(topics='PinterestData')
    logger.info('Successfully subscribed to kafka topic PinterestData')
except Exception as e:
    logger.error(e)

for message in batch_consumer:
    try:
        folder = datetime.date.today().strftime('%Y-%m-%d')
        filename = str(uuid.uuid4())
        os.makedirs(f'./data/{folder}', exist_ok=True)
        with open(f'./data/{folder}/{filename}.json', 'w') as f:
            f.write(json.dumps(message.value))
        logger.info(f'File {filename}.json saved locally')
    except Exception as e:
        logger.error(e)