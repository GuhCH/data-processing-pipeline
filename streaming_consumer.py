from curses import window
import findspark
findspark.init('/home/guhch/spark/spark-3.3.0-bin-hadoop3/')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.2 streaming_consumer.py pyspark-shell'

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

def foreach_batch_function(df, epoch_id):
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

# posts_window = df.groupBy(F.window(df['time_stamp'], '1 minute', '1 minute'), df['category']).count()
# tag_window = df.groupBy(F.window(df['time_stamp'], '1 minute', '1 minute'), df['category']).count()

# print(posts_window)
# print(tag_window)

data_df.writeStream\
    .foreachBatch(foreach_batch_function)\
    .start().awaitTermination()
