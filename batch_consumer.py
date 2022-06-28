from kafka import KafkaConsumer
import json
import boto3
import os

s3_client = boto3.client('s3')

batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: json.loads(message),
    auto_offset_reset="earliest"
)

batch_consumer.subscribe(topics='PinterestData')

filepath = os.getcwd()+'/tmp/'
os.makedirs(filepath, exist_ok=True)
for message in batch_consumer:
    filename = f'{message.timestamp}.json'
    with open(filepath+filename, 'w') as f:
        json.dumps(message)
    response = s3_client.upload_file(filepath+filename, 'pinterest-data-pipeline-project', filename)
    os.remove(filepath+filename)
    