from kafka import KafkaConsumer
import json
import boto3
import os
import uuid
import datetime

s3_client = boto3.client('s3')

folder = datetime.date.today().strftime('%Y-%m-%d')

batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: json.loads(message),
    auto_offset_reset="earliest"
)

batch_consumer.subscribe(topics='PinterestData')

filepath = os.getcwd()+'/tmp/'
os.makedirs(filepath, exist_ok=True)
for message in batch_consumer:
    filename = str(uuid.uuid4())+'.json'
    with open(filepath+filename, 'w') as f:
        f.write(json.dumps(message.value))
    response = s3_client.upload_file(filepath+filename, 'pinterest-data-pipeline-project', f'batch/{folder}/{filename}')
    os.remove(filepath+filename)
    print(f'{filename} successfully saved to pinterest-data-pipeline-project/batch/{folder}')
    