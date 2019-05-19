# tweetCleaner.py
# python tweetCleaner.py infile outfile
import re 
import sys
import tweepy 
from tweepy import OAuthHandler 
from textblob import TextBlob 
import pickle
import string
import config
import pandas as pd

class TwitterClient(object): 
    ''' 
    Generic Twitter Class for sentiment analysis. 
    '''
    def __init__(self): 
        ''' 
        Class constructor or initialization method. 
        '''

    def unpickle_data(self, filename):
        with open(filename, "rb") as handle:
            tweet_list = pickle.load(handle)
        return tweet_list
    
    def clean_tweet(self, tweet): 
        ''' 
        Utility function to clean tweet text by converting to lower case, removing numbers, . 
        '''    
        try:
#                 tweet = tweet.map(lambda x: " ".join(x.split()))
            tweet = tweet.map(lambda x: x.lower())
            tweet = tweet.map(lambda x: x.replace('rt', ''))
            tweet = tweet.map(lambda x: re.sub("http[^ ]+", "", x))
            tweet = tweet.map(lambda x: re.sub("#[^ ]+", "", x))
            tweet = tweet.map(lambda x: re.sub("(@[^ ]+ )*@[^ ]+", "", x))
            for punc in [".", ",", "?", "!"]:
                tweet = tweet.map(lambda x: re.sub("[{}]+".format(punc), "", x))
            tweet = tweet.map(lambda x: x.replace("n't", " not"))
            tweet = tweet.map(lambda x: " ".join(x.split()))
            tweet = tweet.map(lambda x: x.lstrip("MENTION "))
            tweet =  tweet.map(lambda x: x.rstrip())
            
            return tweet 
        
        except:
            print("Error cleaining tweets!")
            
    def get_class(self, analysis):
        try:
            if analysis > 0.25:
                val = 'positive'
            elif analysis < 0.25:
                val = 'negative'
            else:
                val = 'neutral'
            return val
        
        except:
            print("Class Error!")

    def get_tweet_sentiment(self, tweet): 
        ''' 
        Utility function to classify sentiment of passed tweet 
        using textblob's sentiment method 
        '''
        tweet['analysis'] = tweet.text.map(lambda text: TextBlob(text).sentiment.polarity)
        tweet['sentiment'] = tweet.analysis.map(lambda analysis: self.get_class(analysis))
            
        return tweet


    def get_tweets(self, fName, count = 10): 
        ''' 
        Main function to fetch tweets and parse them. 
        '''
        # empty dataframe to store parsed tweets 
        clean_tweets = pd.DataFrame()

        try: 
            # read tweets from file
            fetched_tweets = self.unpickle_data(fName)
            fetched_tweets = pd.DataFrame(fetched_tweets)
#             print(len(fetched_tweets))
            
            print(fetched_tweets.text[0])
            
        except: 
            # print error (if any) 
            print("Input Error!")
            
        clean_tweets = pd.DataFrame()

        #clean the text of the tweets
        clean_tweets['text'] = self.clean_tweet(fetched_tweets.text)
        clean_tweets = self.get_tweet_sentiment(clean_tweets)
        
        # remove duplicate tweets
        clean_tweets.drop_duplicates(subset=['text'], inplace=True)
            
        return clean_tweets 
    
def main(): 
    fName_in = sys.argv[1]
    fName_out = sys.argv[2]

    # creating object of TwitterClient Class 
    api = TwitterClient() 
    # calling function to get tweets 
    tweets = api.get_tweets(fName_in) 
    
    print("Number of tweets: {}".format(len(tweets)))
    
    # identify positive tweets from tweets 
    positive = tweets[tweets['sentiment'] == 'positive'].index 
    # percentage of positive tweets 
    print("\nPositive tweets percentage: {}%".format(round(100*len(positive)/len(tweets), 2))) 
    
    
    negative = tweets[tweets['sentiment'] == 'negative'].index
    # percentage of negative tweets
    print("Negative tweets percentage: {} %".format(round(100*len(negative)/len(tweets), 2)))

    # identify neutral tweets from tweets     
    neutral = tweets[tweets['sentiment'] == 'neutral'].index 
    # percentage of neutral tweets 
    print("Neutral tweets percentage: {} %".format(round(100*len(neutral)/len(tweets), 2)))

    
    # printing first tweets 
    print("\nPositive tweets:\n") 
    for tweet in positive[:10]:
        print(tweets.text[tweet])
    
    print("\nNegative tweets:\n") 
    for tweet in negative[:10]:
        print(tweets.text[tweet])
        
    print("\nNeutral tweets:\n") 
    for tweet in neutral[:10]:
        print(tweets.text[tweet])
 
    tweets.to_pickle(fName_out)
    print("Clean tweets saved as {}".format(fName_out))
    
if __name__ == "__main__": 
    # calling main function 
    main() 
