import findspark
findspark.init('/Users/mac/Spark3/spark-3.2.1-bin-hadoop3.2')

import os
import sys

path_functions = os.path.abspath('./functions')
sys.path.insert(1, path_functions)

from functions import run_job

from pyspark.sql import SparkSession
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.annotator import *
from sparknlp.base import *
from pyspark.ml import Pipeline
from datetime import datetime
import time
import json
import numpy as np

def main():
    
    # Setting up all the necessary variables for the app
    
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
            'com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.3',
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

    sequenceClassifier = DistilBertForSequenceClassification \
          .pretrained('distilbert_sequence_classifier_emotion', 'en') \
          .setInputCols(['token', 'document']) \
          .setOutputCol('class') \
          .setMaxSentenceLength(512)

    emotion_pipeline = Pipeline(stages=[document_assembler, tokenizer, sequenceClassifier])
    
    # Running the whole job
    
    run_job(spark, config, sentiment_pipeline, emotion_pipeline)

if __name__ == '__main__':
    n = 0
    delta_hour = 0
    while True:
        now_hour = datetime.now().hour
        list_of_times = []
        if delta_hour != now_hour:
            list_of_times = []
            n += 1
            print(f'Executing spark jobs...')
            start_time = time.time()
            main()
            time_execution = np.round((time.time() - start_time), 2)
            print(f'Spark app already processed data {n} times, this time took {time_execution} seconds')
            list_of_times.append(time_execution)

        delta_hour = now_hour

        time.sleep(60)