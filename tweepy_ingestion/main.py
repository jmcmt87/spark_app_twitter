import os
import json
from utilities.functions import Listener, configure_create_topics, check_rules



# Getting necessary variables from config.json:

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Defining topics of our interest:

tags = ['Zelensky', 'Putin', 'Biden', 'NATO', 'NoFlyZone']
query = config.get('query')
# -has:multimedia -is:retweet -has:link -is:quote -is:reply

rules = [f"{config.get('zelensky')} {query}", 
    f"{config.get('putin')} {query}", 
    f"{config.get('biden')} {query}", 
    f"{config.get('nato')} {query}", 
    f"{config.get('noflyzone')} {query}"]

def main():
    # Setting up Tweepy filter configuration:
    check_rules(config.get('bearer_token'), rules, tags)
    # Start and configure Kafka producer and topics:
    configure_create_topics(topics=tags)
    # Start streaming:
    Listener(config.get('bearer_token')).filter(tweet_fields=['created_at'])

if __name__ == '__main__':
    main()