import pandas as pd
import json
import os



def open_json(file_path:str) -> dict:
    if isinstance(file_path, str) and os.path.exists(file_path):
            with open(file_path, 'r') as config_file:
                config = json.load(config_file)
    return config

def process_data(df:pd.DataFrame) -> pd.DataFrame:
    '''Makes dataframe suitable for pandas data wrangling'''
    def create_hour_cut(df:pd.DataFrame) -> pd.DataFrame:
        # Creates a specific cut for every hour
        df['hour'] = df['created_at'].dt.hour
        df['day'] = df['created_at'].dt.day
        df['month'] = df['created_at'].dt.month
        df['year'] = df['created_at'].dt.year
        df['date_hour'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']], 
            format="%Y-%d-%m %H")
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']], 
            format="%Y-%d-%m")
        return df

    df = create_hour_cut(df).drop_duplicates(subset=['topic', 'date_hour'])
    df = df[df.topic != "NoFlyZone"]
    columns = ['date', 'date_hour', 'topic', 'counts', 
        'positivity_rate', 'surprise', 'fear', 
        'joy', 'sadness', 'anger', 'love']

    return df[columns].fillna(0)
