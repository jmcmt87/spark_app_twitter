import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import json
import os
import numpy

# Getting necessary variables for configuration

load_dotenv(dotenv_path='/Users/mac/Documents/TFM/spark_app/.env')
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
mongoDB = os.getenv("mongoDB")

packages = ','.join([
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1',
            'com.amazonaws:aws-java-sdk:1.11.563',
            'org.apache.hadoop:hadoop-aws:3.2.2',
            'org.apache.hadoop:hadoop-client-api:3.2.2',
            'org.apache.hadoop:hadoop-client-runtime:3.2.2',
            'org.apache.hadoop:hadoop-yarn-server-web-proxy:3.2.2',
            'com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.2',
            'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
        ])

# Starts spark session

spark = SparkSession.builder.appName('twitter_app_nlp')\
    .master("local[*]")\
    .config('spark.jars.packages', packages) \
    .config('spark.streaming.stopGracefullyOnShutdown', 'true')\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 
            'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config('spark.hadoop.fs.s3a.access.key', ACCESS_KEY) \
    .config('spark.hadoop.fs.s3a.secret.key', SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.sql.shuffle.partitions', 3) \
    .config("spark.driver.memory","8G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.mongodb.input.uri", mongoDB) \
    .config("spark.mongodb.output.uri", mongoDB) \
    .getOrCreate()


# Loading NLP pipelines from the Spark NLP library:

sentiment_pipeline = PretrainedPipeline("analyze_sentimentdl_use_twitter", lang='en')

document_assembler = DocumentAssembler() \
    .setInputCol('text') \
    .setOutputCol('document')

tokenizer = Tokenizer() \
    .setInputCols(['document']) \
    .setOutputCol('token')

sequenceClassifier = BertForSequenceClassification \
    .pretrained('bert_sequence_classifier_emotion', 'en') \
    .setInputCols(['token', 'document']) \
    .setOutputCol('class')

emotion_pipeline = Pipeline(stages=[document_assembler, tokenizer, sequenceClassifier])

# Defining variables

def give_current_path(s3_path):
    if datetime.now().strftime("%H") == '00':
        date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        date = datetime.now().strftime("%Y-%m-%d")
    pre_hour = (datetime.now() - timedelta(hours=1)).strftime("%H")
    return f"{s3_path}/date={date}/hour={pre_hour}/*"

def read_stream(spark, config):

    schema = StructType() \
        .add("data", StructType() \
            .add("created_at", TimestampType())
            .add("text", StringType()))

    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 
                config.get("kafka_servers")) \
        .option("subscribe", config.get("topic_list")) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(F.col('key').cast('string'),
                F.from_json(F.col("value").cast("string"), 
                    schema)['data']['created_at'].alias('created_at'),
                F.from_json(F.col("value").cast("string"), 
                    schema)['data']['text'].alias('text'),
                F.col('topic')) \
        .withColumn("hour", F.date_format(F.col("created_at"), "HH")) \
        .withColumn("date", F.date_format(F.col("created_at"), "yyyy-MM-dd"))


def write_stream(df, config):
    df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", config.get("checkpoint_path")) \
    .option("path", config.get("s3_path_raw")) \
    .trigger(once=True) \
    .partitionBy('date', 'hour') \
    .start()

def transform_sentiment_df(spark, config, sentiment_pipeline):
    paths = give_current_path(config.get("s3_path_raw"))
    df = spark.read.format("parquet").load(paths)
    return sentiment_pipeline.annotate(df, 'text').select('created_at', 'text', 
        'topic', F.element_at(F.col('sentiment.result'), 1).alias('sentiment'))

def process_df(sentiment_df, emotion_pipeline):
    return emotion_pipeline.fit(sentiment_df).transform(sentiment_df) \
        .select('created_at', 'text', 'topic', 'sentiment',
        F.element_at(F.col('class.result'), 1).alias('emotion'))

def aggregate_df(processed_df, mongoDB):
    agg_df = processed_df.groupBy("topic") \
        .agg(F.avg(F.when(F.col("sentiment").eqNullSafe("positive"), 1) \
            .otherwise(0)).alias('positivity'), 
            F.max(F.col('emotion')).alias('emotion'),
            F.count(F.col('topic')).alias('counts')) \
        .withColumn("date", F.date_format(F.col("created_at"), "yyyy-MM-dd")) \
        .withColumn('hour', F.date_format(F.col('created_at'), "HH")) \
        .select('topic', F.round('positivity',2).alias('positivity_rate'), 
                'emotion', 'counts', 'date', 'hour', 'created_at')

    agg_df.write.format("mongo").mode("append").option("uri", mongoDB).save()


def run_job(spark, config, sentiment_pipeline, emotion_pipeline, mongoDB):
    # Starts reading streaming data from Kafka and writes it to the S3 datalake in batches
    write_stream(read_stream(spark, config), config)
    # Starts transforming the dataframes and storing the aggregated one in MongoDB Atlas
    aggregate_df(process_df(transform_sentiment_df(config, sentiment_pipeline), emotion_pipeline), mongoDB)


def main(spark, mongoDB):

    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    run_job(spark, config, sentiment_pipeline, emotion_pipeline, mongoDB)

if __name__ == '__main__':

    while True:
        delta_hour = 0
        now_hour = datetime.datetime.now().hour

        if delta_hour != now_hour:
            print('Executing spark jobs...')
            main(spark, mongoDB)

        delta_hour = now_hour

        time.sleep(60)
