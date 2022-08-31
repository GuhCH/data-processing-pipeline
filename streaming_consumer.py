from curses import window
import findspark
findspark.init('/home/guhch/spark/spark-3.3.0-bin-hadoop3/')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
import logging

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('logs/local_spark_processing.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

try:
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.2 streaming_consumer.py pyspark-shell'
    logger.info('Required additional packages successfully loaded')
except Exception as e:
    logger.error(e)

kafka_topic_name = 'PinterestData'
kafka_bootstrap_servers = 'localhost:9092'

try:
    session = SparkSession \
            .builder \
            .appName("Kafka") \
            .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    data_df = session \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
        .option('subscribe', kafka_topic_name) \
        .option('startingOffsets', 'earliest') \
        .load()

    logger.info('Spark streaming connected to Kafka topic PinterestData at localhost on port 9092')

except Exception as e:
    logger.error(e)

data_df.printSchema()

Schema = StructType([
    StructField('category', StringType(), True),
    StructField('index', IntegerType(), True),
    StructField('unique_id', StringType(), True),
    StructField('title', StringType(), True),
    StructField('description', StringType(), True),
    StructField('follower_count', StringType(), True),
    StructField('tag_list', StringType(), True),
    StructField('is_image_or_video', StringType(), True),
    StructField('image_src', StringType(), True),
    StructField('downloaded', IntegerType(), True),
    StructField('save_location', StringType(), True)
])

def foreach_batch_function(df, epoch_id):
    try:
        # selects relevant data and timestamp for each post
        df = df.selectExpr("CAST(value as STRING)",'timestamp')
        # cleans data (removes unwanted columns, renames timestamp to time_stamp to avoid clashes with SQL syntax, renames index to site_index for consistency with batch layer)
        df = df.withColumn('value', F.from_json(df['value'], Schema)).select(F.col('value.*'),F.col('timestamp').alias('time_stamp'))
        df = df.drop('unique_id','downloaded','save_location').withColumnRenamed('index','site_index')
        df.write.format('jdbc')\
            .option('url', 'jdbc:postgresql://localhost:5432/pinterest_streaming')\
            .option('driver', 'org.postgresql.Driver')\
            .option('dbtable', 'experimental_data')\
            .option('user', 'postgres')\
            .option('password', 'xzxzxzxzx')\
            .mode('append')\
            .save()
        logger.info('Microbatch successfully sent to postgres')
    except Exception as e:
        logger.error(e)

data_df.writeStream\
    .foreachBatch(foreach_batch_function)\
    .start().awaitTermination()
