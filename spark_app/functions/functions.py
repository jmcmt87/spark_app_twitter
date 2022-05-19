import findspark
findspark.init('/Users/mac/Spark3/spark-3.2.1-bin-hadoop3.2')

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.annotator import *
from sparknlp.base import *
from pyspark.ml import Pipeline
from datetime import datetime, timedelta
import time
import typing

#ELT pipeline functions:

# Extract:

def read_stream(spark: SparkSession, config:dict) -> DataFrame:
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
        .option("failOnDataLoss", "false") \
        .option("subscribe", config.get("topic_list")) \
        .option("startingOffsets", "latest") \
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

def write_stream(df:DataFrame, config:dict) -> None:
    '''Writes data in parquet format into the s3 datalake'''
    df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", config.get("checkpoint_path")) \
    .option("path", config.get("s3_path_raw")) \
    .partitionBy('date', 'hour') \
    .start()

# Transform

def transform_sentiment_df(spark:SparkSession, config:dict, 
                           sentiment_pipeline:PretrainedPipeline) -> DataFrame:
    '''Loads data from the s3 datalake from the last hour, performs
    sentiment analysis on the texts, returns a dataframe with a new column 
    with the sentiment'''
    def give_current_path(config: dict) -> str:
        '''Creates a path to the datalake to extract 
        data from the previous hour'''
        if datetime.now().strftime("%H") == '00':
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            date = datetime.now().strftime("%Y-%m-%d")
        pre_hour = (datetime.now() - timedelta(hours=1)).strftime("%H")
        return  f"{config.get('s3_path_raw')}/date={date}/hour={pre_hour}/*"

    paths = give_current_path(config)

    try:
        df = spark.read.format("parquet").load(paths)
    except Exception:
        time.sleep(3600)
        hour = datetime.now().strftime("%H")
        date = datetime.now().strftime('%Y-%m-%d')
        paths = f"{config.get('s3_path_raw')}/date={date}/hour={hour}/*"
        try:
            df = spark.read.format("parquet").load(paths)
        except Exception:
            print('No path')
            
    return sentiment_pipeline.annotate(df, 'text').select('created_at', 'text', 
        'topic', F.element_at(F.col('sentiment.result'), 1).alias('sentiment'))

def process_df(sentiment_df:DataFrame, emotion_pipeline:Pipeline) -> DataFrame:
    '''Performs emoion analysis on the texts and returns a dataframe with a new column
    with the emotions'''
    return emotion_pipeline.fit(sentiment_df).transform(sentiment_df) \
        .select('created_at', 'text', 'topic', 'sentiment',
        F.element_at(F.col('class.result'), 1).alias('emotion'))

def aggregate_df(processed_df:DataFrame, config:dict) -> None:
    '''Aggregates data from the last hour and send it to mongoDB'''   
    
    agg_sentiment = processed_df.groupBy("topic") \
      .agg(F.avg(F.when(F.col("sentiment").eqNullSafe("positive"), 1) \
          .otherwise(0)).alias('positivity'), 
          F.count(F.col('topic')).alias('counts')) \
      .withColumn('created_at', F.current_timestamp()) \
      .select(F.col('topic').alias('topic_agg'), 
              F.round('positivity', 2).alias('positivity_rate'), 
              'counts', 'created_at')
    
    agg_emotion = processed_df.groupby('topic', 'emotion') \
        .agg(F.count(F.col('topic')).alias('counts')) \
        .groupby('topic').pivot('emotion').sum('counts').na.fill(0)
    
    inner_join = agg_sentiment.join(agg_emotion, 
        agg_sentiment.topic_agg == agg_emotion.topic) \
        .select('*')
    
    inner_join.write.format("mongo").mode("append").option("uri", config.get("mongoDB")).save()
    
# Pipeline functions

def run_job(spark:SparkSession, config:dict, sentiment_pipeline:PretrainedPipeline, 
            emotion_pipeline:Pipeline) -> None: 
    # read and load data to s3 datalake
    write_stream(read_stream(spark, config), config)  
    # transform and load aggregated data to mongoDB
    aggregate_df(process_df(transform_sentiment_df(spark, config, sentiment_pipeline), emotion_pipeline), config)
