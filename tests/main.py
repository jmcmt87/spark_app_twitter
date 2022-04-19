#!/usr/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.annotator import *
from sparknlp.base import *

from datetime import datetime, timedelta
import time
import json


# ELT pipeline functions:

# Extract:

def read_stream(spark, config):
    '''Reads data from Kafka from the last time it got called
    and creates columns for time partition, returns a dataframe'''
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

# Load:

def write_stream(df, config):
    '''Writes data in parquet format into the s3 datalake'''
    df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", config.get("checkpoint_path")) \
    .option("path", config.get("s3_path_raw")) \
    .trigger(once=True) \
    .partitionBy('date', 'hour') \
    .start()

# Transform

def transform_sentiment_df(spark, config, sentiment_pipeline):
    '''Loads data from the s3 datalake from the last hour, performs
    sentiment analysis on the texts, returns a dataframe with a new column 
    with the sentiment'''
    def give_current_path(config):
        '''Creates a path to the datalake to extract 
        data from the previous hour'''
        if datetime.now().strftime("%H") == '00':
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            date = datetime.now().strftime("%Y-%m-%d")
        pre_hour = (datetime.now() - timedelta(hours=1)).strftime("%H")
        return f"{config.get('s3_path_raw')}/date={date}/hour={pre_hour}/*"

    paths = give_current_path(config)
    # Error handling in case the data hasn't arrived to the datalake yet:
    while True:
        try:
            df = spark.read.format("parquet").load(paths)
        except Exception:
            time.sleep(5)
            continue
        break

    return sentiment_pipeline.annotate(df, 'text').select('created_at', 'text', 
        'topic', F.element_at(F.col('sentiment.result'), 1).alias('sentiment'))

def process_df(sentiment_df, emotion_pipeline):
    '''Performs emoion analysis on the texts and returns a dataframe with a new column
    with the emotions'''
    return emotion_pipeline.fit(sentiment_df).transform(sentiment_df) \
        .select('created_at', 'text', 'topic', 'sentiment',
        F.element_at(F.col('class.result'), 1).alias('emotion'))

def aggregate_df(processed_df, config):
    '''Aggregates data from the last hour and send it to mongoDB'''
    agg_df = processed_df.groupBy("topic") \
      .agg(F.avg(F.when(F.col("sentiment").eqNullSafe("positive"), 1) \
          .otherwise(0)).alias('positivity'), 
          F.max(F.col('emotion')).alias('emotion'),
          F.count(F.col('topic')).alias('counts')) \
      .withColumn('created_at', 
      # 'created_at' is set to be the previous hour of the aggregation
          F.current_timestamp() - F.expr('INTERVAL 1 HOURS')) \
      .withColumn("date", F.date_format(F.col("created_at"), "yyyy-MM-dd")) \
      .withColumn('hour', F.date_format(F.col('created_at'), "HH")) \
      .select('topic', F.round('positivity',2).alias('positivity_rate'), 
            'emotion', 'counts', 'date', 'hour', 'created_at')

    agg_df.write.format("mongo").mode("append").option("uri", config.get("mongoDB")).save()

# Pipeline functions

def run_job(spark, config, sentiment_pipeline, emotion_pipeline): 
    # read and load data to s3 datalake
    write_stream(read_stream(spark, config), config)  
    # transform and load aggregated data to mongoDB
    aggregate_df(process_df(transform_sentiment_df(spark, config, sentiment_pipeline), emotion_pipeline), config)


def main():
    
    # Setting up all the necessary variables for the app
    
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    
    packages = ','.join(
        [
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1',
            'com.amazonaws:aws-java-sdk:1.11.563',
            'org.apache.hadoop:hadoop-aws:3.2.2',
            'org.apache.hadoop:hadoop-client-api:3.2.2',
            'org.apache.hadoop:hadoop-client-runtime:3.2.2',
            'org.apache.hadoop:hadoop-yarn-server-web-proxy:3.2.2',
            'com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.2',
            'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
        ]
    )

    # Starts spark session

    spark = SparkSession.builder.appName('twitter_app_nlp')\
        .master("local[*]")\
        .config('spark.jars.packages', packages) \
        .config('spark.streaming.stopGracefullyOnShutdown', 'true')\
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.access.key', 
                config.get("ACCESS_KEY")) \
        .config('spark.hadoop.fs.s3a.secret.key', 
                config.get("SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config('spark.sql.shuffle.partitions', 3) \
        .config("spark.driver.memory","8G")\
        .config("spark.driver.maxResultSize", "0") \
        .config("spark.kryoserializer.buffer.max", "2000M")\
        .config("spark.mongodb.input.uri", config.get("mongoDB")) \
        .config("spark.mongodb.output.uri", config.get("mongoDB")) \
        .getOrCreate()
    
    # Loading Spark NLP pre-trained pipelines
    
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
    
    # Running the whole job
    
    run_job(spark, config, sentiment_pipeline, emotion_pipeline)

if __name__ == "__main__":
    n = 0
    delta_hour = 0
    while True:
        now_hour = datetime.now().hour

        if delta_hour != now_hour:
            n += 1
            print(f'Executing spark jobs...')
            main()
            print(f'Spark app already processed data {n} times')

        delta_hour = now_hour

        time.sleep(60)
