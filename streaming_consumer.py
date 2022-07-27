import findspark
findspark.init('/home/guhch/spark/spark-3.3.0-bin-hadoop3/')
import pyspark
from pyspark.sql import SparkSession
import os
import multiprocessing

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 streaming_consumer.py pyspark-shell'

kafka_topic_name = 'PinterestData'
kafka_bootstrap_servers = 'localhost:9092'

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

data_df = data_df.selectExpr("CAST(value as STRING)")
data_df.writeStream.outputMode('append').format('console').start().awaitTermination()
