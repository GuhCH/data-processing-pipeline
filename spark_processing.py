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
)
 
print('Spark session config:')
print(cfg.toDebugString())

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

sc = session.sparkContext

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

cleanedSchema = StructType([
    StructField('category', StringType(), True),
    StructField('index', IntegerType(), True),
    StructField('title', StringType(), True),
    StructField('description', StringType(), True),
    StructField('follower_count', StringType(), True),
    StructField('tag_list', StringType(), True),
    StructField('is_image_or_video', StringType(), True),
    StructField('image_src', StringType(), True)
])

finalSchema = StructType([
    StructField('category', StringType(), True),
    StructField('index', IntegerType(), True),
    StructField('title', StringType(), True),
    StructField('description', StringType(), True),
    StructField('follower_count', StringType(), True),
    StructField('tag_list', ArrayType(StringType()), True),
    StructField('is_image_or_video', StringType(), True),
    StructField('image_src', StringType(), True)
])

def f(taglist):
    re.split(',',taglist)

emptyRDD = session.sparkContext.emptyRDD()
df = session.createDataFrame(emptyRDD,cleanedSchema).toPandas()

for file in myBucket.objects.all():
    print(file.key)
    s3c.download_file(bucket, file.key, 'tmp/tmp.json')
    df1 = session.read.json('tmp/tmp.json', schema=rawSchema).drop('unique_id','downloaded','save_location').toPandas()
    df = pandas.concat([df,df1])
df = session.createDataFrame(df)
df1 = df.select(split(col('tag_list'),',').alias('tags'),'index').drop('tag_list')
df.join(df1,['index']).drop('tag_list').cache()
df.sort('category').show()
