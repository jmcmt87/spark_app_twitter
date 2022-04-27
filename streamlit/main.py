import os
import sys

# path_modules = [os.path.abspath('./utilities'), os.path.abspath('./charts')]
# for index, module in enumerate(path_modules):
#     sys.path.insert(index, module)

import streamlit as st 
import pandas as pd
import altair as alt
import numpy as np

import pymongo
import certifi 
import typing

from utilities.utilities import process_data, open_json
from charts.charts import *



mongoDB = open_json(os.path.abspath('./config.json'))['mongoDB']
mongo_database = 'aggregated'
collection = 'aggregated_data_twitter'


# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection() -> pymongo.MongoClient:
    return pymongo.MongoClient(mongoDB, tlsCAFile=certifi.where())

client = init_connection()

# Pull data from the collection.
# Uses st.experimental_memo to only rerun when the query changes or after 1 hour.
@st.experimental_memo(ttl=3600)
def get_dataframe(database:str, collection:str) -> pd.DataFrame:
    db = client[database]
    items = db[collection].find()
    items = list(items)  # make hashable for st.experimental_memo
    return pd.DataFrame(items)

df_preprocessed = get_dataframe(mongo_database, collection)

df = process_data(df_preprocessed)

st.image('https://gray-wtvm-prod.cdn.arcpublishing.com/resizer/'\
         'qL3Z6OmQoxRCrwt_CLYoO3qZyB0=/800x200/smart/filters:quality(70)/'\
         'cloudfront-us-east-1.images.arcpublishing.com/gray/'\
         'JXBT6TQMSZG3VGFFKXKT53CI44.jpg')

st.title("Russo-Ukranian War Twitter Observer")

option = st.sidebar.selectbox(
     'Select Aggregation Level:',
     ('Global', 'Day', 'Hour'))

'''
## Twitter Trends Monitor.
'''

# GLOBAL AGGREGATION CHARTS:

if option == 'Global':
    st.subheader('Global')
    st.altair_chart(emotion_global_norm_bar_chart(df), use_container_width=True)
    st.altair_chart(total_global_emotion_donut_chart(df), use_container_width=True)
    st.altair_chart(total_counts_donut_chart(df), use_container_width=True)
    st.altair_chart(positivity_global_boxplot(df), use_container_width=True)
    st.altair_chart(global_pos_bar_chart(df), use_container_width=True)

if option == 'Day':
    st.subheader('Results on a Daily Basis')
    st.altair_chart(total_counts_period_area_chart(df, agg_level='date'),
        use_container_width=True)
    chart1 = emotion_period_area_chart(df, agg_level='date', normalize=False)
    chart2 = emotion_period_area_chart(df, agg_level='date', normalize=True)
    st.altair_chart(chart1 | chart2, use_container_width=False)
    chart3 = counts_topic_period_area_chart(df, agg_level='date', normalize=False)
    chart4 = counts_topic_period_area_chart(df, agg_level='date', normalize=True)
    st.altair_chart(chart3 | chart4, use_container_width=False)
    st.altair_chart(positivity_boxplot(df, 'date'))

if option == 'Hour':
    st.subheader('Results on a Hourly Basis')
    st.altair_chart(total_counts_period_area_chart(df, agg_level='date_hour'), 
        use_container_width=True)
    chart5 = emotion_period_area_chart(df, agg_level='date_hour', normalize=False)
    chart6 = emotion_period_area_chart(df, agg_level='date_hour', normalize=True)
    st.altair_chart(chart5 | chart6, use_container_width=True)
    chart7 = counts_topic_period_area_chart(df, agg_level='date_hour', normalize=False)
    chart8 = counts_topic_period_area_chart(df, agg_level='date_hour', normalize=True)
    st.altair_chart(chart7 | chart8, use_container_width=True)
    st.altair_chart(pos_period_line_chart(df, agg_level='date_hour'), 
        use_container_width=True)


'''
### Latest Inputs
'''

columns = ['date_hour', 'topic', 'fear', 'anger', 'sadness', 'joy', 'surprise', 
           'love', 'positivity_rate', 'counts']
st.dataframe(df[columns].tail(25))
st.write('\n')
