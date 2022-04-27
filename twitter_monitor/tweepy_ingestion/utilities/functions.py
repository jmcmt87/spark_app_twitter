from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import json
import tweepy
import typing
from typing import Literal


# KAFKA
# Defining kafka configuration:

servers = "localhost:9092,localhost:9093,localhost:9094"
conf = {'bootstrap.servers': servers,
        'partitioner': 'consistent_random'}
producer = Producer(conf)


# Defining Kafka functions

def configure_create_topics(servers:str=servers, topics:list=[]) -> None:
    '''Configures and creates the necessary topics for the kafka cluster'''

    a = AdminClient({'bootstrap.servers': servers})

    # Setting the topics configuration
    topics = [NewTopic(topic, num_partitions=3, replication_factor=3) 
              for topic in topics]

    # Call create_topics to asynchronously create topics, a dict
    # of <topic,future> is returned.
    fs = a.create_topics(topics, request_timeout=15.0)

    # Wait for operation to finish.
    # All futures will finish at the same time.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

def send_message(data:dict, name_topic:str, id:str) -> None:    
    '''Starts sending messages and creates a message key for every message.
    Every key is created taking the first two capitalized topic letters and 
    the tweet id'''
    producer.produce(topic=name_topic, value=data, 
                     key=f"{name_topic[:2].upper()}{id}".encode('utf-8'))


# TWEEPY
# Defining functions to attach the necessary rules for the Twitter API streaming filter:

def check_rules(bearer_token:str, rules:list, tags:list) -> None:
    '''Checks whether there are rules already attached to the
    bearer token or not, if there are rules attached it will 
    delete all the rules, then it will add all the necessary 
    rules in both cases'''
    def add_rules(client:tweepy.StreamingClient, rules:list, tags:list) -> None:
        '''Adds rules to the streamer filter'''
        for rule, tag in zip(rules, tags):
            client.add_rules(tweepy.StreamRule(value=rule, tag=tag))

    client = tweepy.StreamingClient(bearer_token, wait_on_rate_limit=True)
    if client.get_rules()[3]['result_count'] != 0:
        n_rules = client.get_rules()[0]
        ids = [n_rules[i_tuple[0]][2] for i_tuple in enumerate(n_rules)]
        client.delete_rules(ids)
        add_rules(client, rules, tags)
    else:
        add_rules(client, rules, tags)


# Creating a Twitter stream listener class:

class Listener(tweepy.StreamingClient):

    def on_data(self, data:dict) -> Literal[True]:
        print(data)
        message = json.loads(data)
        if 'matching_rules' in message:
            for rule in message['matching_rules']:
                send_message(data, name_topic=rule['tag'], 
                                id=message['data']['id'])
        else:
            print('Operational error, reconnecting...')
        return True
    
    def on_error(self, status):
        print(status)
