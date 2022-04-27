import pandas as pd
import typing
import altair as alt
import numpy as np


def check_string(string:str) -> None:
    '''When it comes to period charts, 
    it checks if the agg_level is a correct value'''
    try:
        string in ['date_hour', 'date', 'topic']
    except Exception:
        print("Introduce a valid input: 'date_hour', 'date' or 'topic'")

# EMOTION CHARTS

def get_long_emotion_df(df:pd.DataFrame, agg_level:str) -> pd.DataFrame:
    '''This function accepts these aggregation levels:
        topic: gets global emotion data
        date: gets daily emotion data
        date_hour: gets hourly emotion data
    Data is returned in a long data format dataframe so it can be visualized'''
    def get_long_data(df:pd.DataFrame) -> pd.DataFrame:
        '''Select and transform emotion data into long data format'''
        agg_level = df.columns[0]
        df_emo_long = df.melt(id_vars=agg_level, 
            value_vars=['surprise', 'fear', 'joy', 'sadness', 'anger', 'love'])
        df_emo_long.rename(columns={'variable':'emotion', 'value':'counts'}, 
            inplace=True)
        df_emo_long['percent'] = df_emo_long['counts'] / df_emo_long \
            .groupby(agg_level)['counts'].transform('sum')
        
        return df_emo_long

    check_string(agg_level)

    df_emo = df[[agg_level, 'anger', 'fear', 
               'joy', 'love', 'sadness', 
               'surprise']].groupby(agg_level).sum().reset_index()
    
    return get_long_data(df_emo)

def emotion_global_norm_bar_chart(df: pd.DataFrame) -> alt.Chart:
    df_long_e = get_long_emotion_df(df, agg_level='topic')

    return alt.Chart(df_long_e).mark_bar().encode(
        x=alt.X('sum(counts)', stack="normalize"),
        y='topic',
        color='emotion',
        tooltip = ['topic', 'emotion', 
            alt.Tooltip('sum(percent):Q', format='.1%'), 
            'sum(counts)']
    ).properties(title='Emotions per Topic (Normalized)')

def total_global_emotion_donut_chart(df: pd.DataFrame) -> alt.Chart:
    df_long_e = get_long_emotion_df(df, agg_level='topic')

    return alt.Chart(df_long_e).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field='counts', type="quantitative"),
        color=alt.Color(field='emotion', type="nominal"),
        tooltip = ['topic', 'emotion', 
            alt.Tooltip('sum(percent):Q', format='.1%'), 'sum(counts)']
    ).properties(title='Total Count Emotions')

def emotion_period_area_chart(df:pd.DataFrame, agg_level:str, normalize:bool=False) -> alt.Chart:
    check_string(agg_level)
    df_long_e = get_long_emotion_df(df, agg_level)

    if normalize == True:
        stack = 'normalize'
        how = 'Normalized'
        opacity = 1
    else:
        stack = None
        how = 'Non-Normalized'
        opacity = 0.38
    if agg_level == 'date_hour':
        period = 'Hour'
    else:
        period = 'Day'

    return alt.Chart(df_long_e).mark_area(opacity=opacity).encode(
        x=agg_level, 
        y=alt.Y("counts:Q", stack=stack),
        color='emotion', 
        tooltip=[agg_level, 'counts', 'emotion']
    ).properties(title=f'Emotions per {period} ({how})')

# COUNT CHARTS

def total_counts_donut_chart(df:pd.DataFrame) -> alt.Chart:
    df_counts = df[['topic', 'counts']].groupby('topic').sum().reset_index()
    df_counts['percent'] = df_counts['counts'] / df_counts['counts'].sum()

    return alt.Chart(df_counts).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="counts", type="quantitative"),
        color=alt.Color(field="topic", type="nominal"),
        tooltip=[alt.Tooltip('percent:Q', format='.1%'), 'counts', 'topic']
    ).properties(title='Total Mentions per Topic')

def total_counts_period_area_chart(df:pd.DataFrame, agg_level:str) -> alt.Chart:
    check_string(agg_level)
    tweet_counts_period = df.groupby([agg_level])['counts'].sum().to_frame().reset_index()

    if agg_level == 'date_hour':
        period = 'Hour'
    else:
        period = 'Day'

    return alt.Chart(tweet_counts_period).mark_area().encode(
        x=agg_level, 
        y='counts', 
        tooltip=[agg_level, 'counts']
    ).properties(title=f'Tweets Count per {period}')

def counts_topic_period_area_chart(df:pd.DataFrame, agg_level:str, normalize:bool=False) -> alt.Chart:
    check_string(agg_level)
    tweet_counts_topic = df.groupby([agg_level, 'topic']).sum().reset_index()

    if normalize == True:
        stack = 'normalize'
        how = 'Normalized'
        opacity = 1
    else:
        stack = None
        how = 'Non-Normalized'
        opacity = 0.38
    if agg_level == 'date_hour':
        period = 'Hour'
    else:
        period = 'Day'
    
    return alt.Chart(tweet_counts_topic).mark_area(opacity=opacity).encode(
        x= agg_level,
        y = alt.Y('counts:Q', stack=stack),
        color='topic', 
        tooltip=[agg_level, 'counts', 'topic']
    ).properties(title=f"Tweets per {period}: ({how})")

# POSITIVITY CHARTS

def global_pos_bar_chart(df:pd.DataFrame) -> alt.Chart:
    df_positivity = df[['topic', 'positivity_rate']].groupby('topic').mean().reset_index()

    pos_chart = alt.Chart(df_positivity).mark_bar().encode(
        x = 'topic',
        y = alt.Y('positivity_rate:Q', axis=alt.Axis(title='Positivity Rate (Global)')),
        tooltip=['topic', alt.Tooltip("positivity_rate:Q", format=".2f")]
    )

    rule = alt.Chart(df_positivity).mark_rule(color='red').encode(y='mean(positivity_rate):Q')

    return (pos_chart + rule).properties(height=500, width=350, title='Average Global Positivity')

def pos_period_line_chart(df:pd.DataFrame, agg_level:str) -> alt.Chart:
    check_string(agg_level)
    if agg_level == 'date_hour':
        period = 'Hour'
    else:
        period = 'Day'

    chart_positivity = alt.Chart(df).mark_line().encode(
        x= agg_level, 
        y= alt.Y('positivity_rate'), 
        color= 'topic', 
        row= alt.Row('topic'),
        tooltip=[agg_level, 'positivity_rate', 'topic']
    ).properties(title=f'Tweets Positivity Rate per Topic per {period}', height=50, width=530)

    chart_positivity.encoding.y.title = 'pos rate'

    return chart_positivity


def positivity_boxplot(df:pd.DataFrame, agg_level:str) -> alt.Chart:
    check_string(agg_level)
    if agg_level == 'date_hour':
        period = 'Hour'
    else:
        period = 'Day'

    chart = alt.Chart(df).mark_boxplot().encode(
        x=agg_level,
        y='positivity_rate:Q',
        color= 'topic', 
        row= alt.Row('topic'),
        tooltip=['topic:O', 'positivity_rate:Q']
    ).properties(title=f'Positivity Rate per Topic per {period} (Boxplot)', height=50, width=530)

    chart.encoding.y.title = 'pos rate'

    return chart

def positivity_global_boxplot(df:pd.DataFrame) -> alt.Chart:

    return alt.Chart(df).mark_boxplot().encode(
        x='topic',
        y='positivity_rate:Q',
        color= 'topic', 
        tooltip=['topic', 'positivity_rate:Q', 'date']
    ).properties(title='Global Positivity Rate per Topic (Boxplot)')






    


