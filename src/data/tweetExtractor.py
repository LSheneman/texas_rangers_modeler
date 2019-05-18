# tweetExtractor.py
import sys
import tweepy 
from tweepy import OAuthHandler 
import pickle
import config


class TweetExtractor(object):
    '''
    Generic Tweet Extractor for building dataset from hashtags.
    '''

    def __init__(self):
        '''
        Class constructor
        '''

        # keys and tokens from the Twitter Dev Console 
        consumer_key = config.TWITTER_CONFIG['consumer_key']
        consumer_secret = config.TWITTER_CONFIG['consumer_secret']
        access_token = config.TWITTER_CONFIG['access_token']
        access_token_secret = config.TWITTER_CONFIG['access_token_secret']

        # attempt authentication 
        try: 
            # create OAuthHandler object 
            self.auth = OAuthHandler(consumer_key, consumer_secret) 
            # set access token and secret 
            self.auth.set_access_token(access_token, access_token_secret) 
            # create tweepy API object to fetch tweets 
            self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True) 
        except: 
            print("******\n Error: Authentication Failed\n ******\n") 

    def pickle_data(self, filename, tweet_list):
        with open(filename, "wb") as handle:
            try:
                pickle.dump(tweet_list, handle, protocol=pickle.HIGHEST_PROTOCOL)
            except:
                print("******\n Output error : " + str(e) + "\n ******\n")

    def unpickle_data(self, filename, tweet_list):
        '''
        Read json file to list
        '''
        with open(filename, "rb") as handle:
            try:
                tweet_list = pickle.load(handle)
            except:
                print("******\n Load error : " + str(e) + "\n ******\n")
        return tweet_list

    def get_tweets(self, maxTweets, searchQuery, tweetsPerQry = 100, sinceId = None, max_id = -1): 
        '''
        Function to load a large number of tweets

        Twitter allows a maximum of 100 tweets per query

        If results from a specific ID onwards are reqd, set since_id to that ID.
        else default to no lower limit, go as far back as API allows


        If results only below a specific ID are, set max_id to that ID.
        else default to no upper limit, start from the most recent tweet matching the search query.
        '''

        tweetCount = 0
        tweet_list = []
        print("Downloading max {0} tweets".format(maxTweets))

        # Download a cumulative list of tweets that pauses when tweets per 15 minutes is reached,
        # then resumes when it is able to.
        while tweetCount < maxTweets:
            try:
                if (max_id <= 0):
                    if (not sinceId):
                        new_tweets = self.api.search(q=searchQuery, count=tweetsPerQry)
                    else:
                        new_tweets = self.api.search(q=searchQuery, count=tweetsPerQry,
                                                since_id=sinceId)
                else:
                    if (not sinceId):
                        new_tweets = self.api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1))
                    else:
                        new_tweets = self.api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1),
                                                since_id=sinceId)
                if not new_tweets:
                    print("\nNo more tweets found")
                    break
                for tweet in new_tweets:
                    tweet_list.append(tweet._json)

                tweetCount += len(new_tweets)
                print("Downloaded {0} tweets".format(tweetCount))
                max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                # Just exit if any error
                print("*****\n Search error : " + str(e) + "\n ******\n")
                break
        return tweet_list, tweetCount

def main():

    #Set run parameters using command line arguments
    searchQuery = sys.argv[1]  # this is what we're searching for
    maxTweets = int(sys.argv[2]) # Some arbitrary large number
    fName = sys.argv[3] # We'll store the tweets in a json file.

    #Create object of TweetExtractor Class
    api = TweetExtractor()

    #calling function to get tweets and number of tweets
    tweet_list, tweetCount = api.get_tweets(maxTweets, searchQuery)

    api.pickle_data(fName, tweet_list)

        
    print ("======\n Downloaded {0} tweets saved to {1} \n======".format(tweetCount, fName))

if __name__ == "__main__": 
    # calling main function 
    main() 