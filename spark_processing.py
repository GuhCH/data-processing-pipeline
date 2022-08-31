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
import logging

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('logs/batch_consumer.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

try:
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.255,org.apache.hadoop:hadoop-aws:3.3.3,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'
    logger.info('Required additional packages successfully loaded')
except Exception as e:
    logger.error(e)

try:
    s3 = boto3.resource('s3')
    bucket = 'pinterest-data-pipeline-project'
    myBucket = s3.Bucket(bucket)
    access_key = input('AWS access key: ')
    secret_key = input('AWS secret key: ')
    logger.info(f'Connection made to S3 bucket {bucket}')
except Exception as e:
    logger.error(e)

def process_raw_data(date: str):
    '''
    Reads pinterest data from a given date from S3 bucket, cleans it and sends it to Cassandra table.

    args:
        a date in YYYY-MM-DD format
    '''
    try:
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

        session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

        sc = session.sparkContext
        
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', access_key)
        hadoopConf.set('fs.s3a.secret.key', secret_key)
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

        logger.info('Spark session successfully started')
    except Exception as e:
        logger.error(e)

    keyspace_name = 'data'
    table_name = 'pinterest_data'

    ## uncomment these (and comment above) to allow custom table names
    # keyspace_name = input('Cassandra keyspace name: ')
    # table_name = input('Cassandra table name: ')

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

    for file in myBucket.objects.filter(Prefix=f'batch/{date}'):
        try:
            df = session.read.json(f's3a://pinterest-data-pipeline-project/{file.key}', schema=rawSchema) # read file from s3
            df = df.drop('unique_id','downloaded','save_location').withColumnRenamed('index','site_index') # drop unwanted columns
            df = df.select('*',split(col('tag_list'),',').alias('tags')).drop('tag_list') # convert 'tag_list' strings to lists (using , as delimiter)
            df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table=table_name, keyspace=keyspace_name).save() # append row to cassandra table
            logger.info(file.key+' cleaned and sent to cassandra')
        except Exception as e:
            logger.error(e)

if __name__ == '__main__':
    process_raw_data('2022-07-07')