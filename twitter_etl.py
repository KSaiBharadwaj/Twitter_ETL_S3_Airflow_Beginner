import pandas as pd
import os
import s3fs
import json
import tweepy
from dotenv import load_dotenv


#First set is to authenticate the twitter API with the keys
# Getting the keys from the secrets of Ubuntu server

# This can be done using vault but for simplicity i am using bashrc/python-dotenv
load_dotenv()

access_key = os.getenv('ACCESS_KEY')
access_secret = os.getenv('ACCESS_SECRET')
consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')

#Step 2 Authenticating Twitter

def twitter_etl_pipeline():

    auth_twitter = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth_twitter.set_access_token(access_key, access_secret)

    # Retriving the data for a User

    api = tweepy.API(auth_twitter)

    tweets_recieved = api.user_timeline(screen_name='@cnnbrk', count=150, include_rts=False)

    # Creating a list of required values and converting it into a data frame
    list_tweets = []
    for tweet in list_tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    # Storing in a designated folder in ubuntu server

    df.to_csv('/twitter/refined_tweets.csv')


