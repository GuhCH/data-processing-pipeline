from curses import window
import findspark
findspark.init('/home/guhch/spark/spark-3.3.0-bin-hadoop3/')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, from_json
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

df = data_df.selectExpr("CAST(value as STRING)",'timestamp')
df = df.withColumn('value', from_json(df['value'], Schema)).select(col('value.*'),col('timestamp'))
df = df.drop('unique_id','downloaded','save_location').withColumnRenamed('index','site_index') # drop unwanted columns
df = df.select('*',split(col('tag_list'),',').alias('tags')).drop('tag_list')

df.writeStream.outputMode('append').format('console').start().awaitTermination()

df.show(10)
