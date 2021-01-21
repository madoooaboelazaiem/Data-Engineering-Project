import airflow
from airflow import DAG
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from textblob import TextBlob

# For parsing tweets
import tweepy 
from tweepy import OAuthHandler
import re
from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
# Terminal Commands: 
    # cd ~/airflow
    # airflow webserver -p 8080
    # airflow scheduler
# Make Sure To Include The Data Folder In The Location Of The Script
# Make Sure To Change The Location Of The Saved CSV File In The Script 
# Polarity is float which lies in the range of [-1,1] where 1 means positive statement and -1 means a negative statement.
# Subjective sentences generally refer to personal opinion, emotion or judgment whereas objective refers to factual information.
# Subjectivity is also a float which lies in the range of [0,1]



# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date' :datetime.now().strftime('%Y-%m-%d'),
    # 'end_date' :datetime(2021,1,3).strftime('%Y-%m-%d')
    }
# step 3 - instantiate DAG
# Catch Up False Since We Will Run It Daily From The Moment It Gets Initialize We Don't Care About The Previous Schedules Missed and because we are increasing the tweets csv files to have better insights
dag = DAG(
    'ScrapersSentimentAnalysisHourly-DAG',
    default_args=default_args,
    description='Analysing tweets from twitter',
    # schedule_interval='0 15 * * 0-6',
    schedule_interval='@hourly',
    catchup=False
)

# Authenticating and applying the search parameters using (Countries: Benin as low happiness and USA as high happiness)
def twitterAuth(**context):
    API_key = 'ng8vy4JXVuRPyuwJgSbvgNlkV'
    API_secret = 'etwrhVe8oliy3lvfEqgAB2KnhCvAqAhhQ8RDiJCx80LjLdJsbG'
    access_token_secret = 'ASNAvCh8OjaikP1AFoxTJsGYROYm5tVpgSsd5HbOrEMxx'
    access_token = '861986246-rlMFxsrCECLefntFr5jP6SNpR7veeuAid05qwJCP'
    df = ['USA','Benin']
    try:
        auth = OAuthHandler(API_key,API_secret)
        auth.set_access_token(access_token,access_token_secret)
        api = tweepy.API(auth)
        print('Authenticated')
        tweetCount = 0
        tweets =[]
        for i in range(len(df)):
            places = api.geo_search(query=df[i], granularity="country")
            place_id = places[0].id
            new_tweets = api.search(q="place:%s" % place_id,count=40)
            # for tweet in tweets:
            #     print(tweet.text + " | " + tweet.place.name) if tweet.place else print("Undefined place")
            for tweet in new_tweets:
                parsed_tweet = {} 
                parsed_tweet['tweets'] = tweet.text
                parsed_tweet['country'] = df[i]
                # appending parsed tweet to tweets list 
                if tweet.retweet_count > 0: 
                    # if tweet has retweets, ensure that it is appended only once 
                    if parsed_tweet not in tweets: 
                        tweets.append(parsed_tweet) 
                else: 
                    tweets.append(parsed_tweet) 
                        
        tweetCount += len(new_tweets)
        ts = datetime.now().timestamp()
        x = str(ts)
        x = x.split('.')
        print("Saving CSV")
        df_tweets = tweets
        return df_tweets
    except:
        print("Sorry! Error in authentication!")

# Cleaning and Preprocessing the tweets by removing unwanted characters (@ http links and duplicated tweets)
def cleaningTweets(**context):
    tweets_df = context['task_instance'].xcom_pull(task_ids='twitterAuth')
    tweets_df = pd.DataFrame(tweets_df)
    def remove_pattern(text, pattern_regex):
        r = re.findall(pattern_regex, text)
        for i in r:
            text = re.sub(i, '', text)
        return text
    def cleaningTweets(tweets_df):
        # We are keeping cleaned tweets in a new column called 'tidy_tweets'
        tweets_df['tidy_tweets'] = np.vectorize(remove_pattern)(tweets_df['tweets'], "@[\w]*: | *RT*")

        cleaned_tweets = []

        for index, row in tweets_df.iterrows():
            # Here we are filtering out all the words that contains link
            words_without_links = [word for word in row.tidy_tweets.split()        if 'http' not in word]
            cleaned_tweets.append(' '.join(words_without_links))

        tweets_df['tidy_tweets'] = cleaned_tweets
        tweets_df.drop_duplicates(subset=['tidy_tweets'], keep=False)

        tweets_df['absolute_tidy_tweets'] = tweets_df['tidy_tweets'].str.replace("[^a-zA-Z# ]", "")
        tweets_df['tweets'] = tweets_df['absolute_tidy_tweets']
        tweets_df.drop(['absolute_tidy_tweets', 'tidy_tweets'], axis=1, inplace=True)
        return tweets_df

    tweets_df = cleaningTweets(tweets_df)
    return tweets_df.to_json()

# Applying Sentiment analysis and categorizing results as postive negative or neutral for the values above below and equal zero
def tweetsSentimentAnalysis(**context):
    tweets_df = context['task_instance'].xcom_pull(task_ids='cleaningTweets')
    tweets_df = pd.read_json(tweets_df)
    public_tweets = tweets_df['tweets'].tolist()
    tweets = []
    for tweet in public_tweets:
        tweet = str(tweet)
        #this will create a textblob out of the tweet
        blob = TextBlob(tweet)
        try:
            blob.translate(to= 'en')
        except:
            print('Same Translation')
        sentiment = blob.sentiment
        #if statements to give a label to the sentiment
        if sentiment[0]>0:
                tweets.append([tweet,sentiment[0],'positive'])
        elif sentiment[0]<0:
                tweets.append([tweet,sentiment[0],'negative'])
        else:
                tweets.append([tweet,sentiment[0],'Neutral'])
    tweets = pd.DataFrame(tweets,columns = ['tweets','sentiment score','label'])
    tweets.head()
    tweets_df['sentiment score'] = tweets['sentiment score']
    tweets_df['label'] = tweets['label']
    return tweets_df.to_json()

# Storing the output in a csv with the datetime corresponding to the day the dag ran at (Automatically daily dags runs at 12 am so this will be default time).
def store_data(**context):
    df = context['task_instance'].xcom_pull(task_ids='tweetsSentimentAnalysis')
    df = pd.read_json(df)
    ts = datetime.now().timestamp()
    x = str(ts)
    x = x.split('.')
    print("Saving CSV")
    df.to_csv("/home/mado/Mado/GUC/DE/Project/project-milestone-1-scrapers/data/"+x[0]+"_scrapersAnalysis.csv")


t2 = PythonOperator(
    task_id='twitterAuth',
    provide_context=True,
    python_callable=twitterAuth,
    dag=dag,
)
t3 = PythonOperator(
    task_id='cleaningTweets',
    provide_context=True,
    python_callable=cleaningTweets,
    dag=dag,
)
t4 = PythonOperator(
    task_id='tweetsSentimentAnalysis',
    provide_context=True,
    python_callable=tweetsSentimentAnalysis,
    dag=dag,
)
t5 = PythonOperator(
    task_id='store_data',
    provide_context=True,
    python_callable=store_data,
    dag=dag,
)


t2>>t3>>t4>>t5
