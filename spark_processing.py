import findspark
findspark.init()
import multiprocessing
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import split, col
import boto3
import json
import re
import pandas
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.255,org.apache.hadoop:hadoop-aws:3.3.3,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'

s3r = boto3.resource('s3')
s3c = boto3.client('s3')
bucket = 'pinterest-data-pipeline-project'
myBucket = s3r.Bucket(bucket)

cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("TestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
    .set("spark.sql.catalog.myCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
)
 
print('Spark session config:')
print(cfg.toDebugString())

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

sc = session.sparkContext

access_key = input('Access key: ')
secret_key = input('Secret key:')
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', access_key)
hadoopConf.set('fs.s3a.secret.key', secret_key)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

rawSchema = StructType([
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

for file in myBucket.objects.all():
    print(file.key)
    df = session.read.json('s3a://pinterest-data-pipeline-project/'+file.key)
    df = df.drop('unique_id','downloaded','save_location') # drop unwanted columns
    df = df.select('*',split(col('tag_list'),',').alias('tags')).drop('tag_list').show() # convert 'tag_list' strings to lists (using , as delimiter)
    # df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="pinterest_data", keyspace="data").save()