from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer
import logging

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('logs/pin_API.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = FastAPI()

try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                            client_id='Pinterest data producer',
                            value_serializer=lambda message: dumps(message).encode("ascii"))
    logger.info('Connected to Kafka producer at localhost on port 9092')
except Exception as e:
    logger.error(e)

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    try:
        data = dict(item)
        producer.send(topic='PinterestData', value=data)
        logger.info('Data posted to Kafka producer')
        return item
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    try:
        uvicorn.run("project_pin_API:app", host="localhost", port=8000)
        logger.info('Connected to API at localhost on port 8000')
    except Exception as e:
        logger.error(e)
