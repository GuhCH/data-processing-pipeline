from kafka import KafkaConsumer
import json
import boto3
import os
import uuid
import datetime
import logging

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('logs/batch_consumer.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

s3_client = boto3.client('s3')

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

filepath = os.getcwd()+'/tmp/'
os.makedirs(filepath, exist_ok=True)
for message in batch_consumer:
    try:
        filename = str(uuid.uuid4())+'.json'
        os.makedirs(f'./data/{folder}', exist_ok=True)
        with open(f'./data/{folder}/{str(uuid.uuid4())}.json', 'w') as f:
            f.write(json.dumps(message.value))
        folder = datetime.date.today().strftime('%Y-%m-%d')
        response = s3_client.upload_file(filepath+filename, 'pinterest-data-pipeline-project', f'batch/{folder}/{filename}')
        os.remove(filepath+filename)
        logger.info(f'{filename} successfully uploaded to S3 at pinterest-data-pipeline-project/batch/{folder}')
    except Exception as e:
        logger.error(e)