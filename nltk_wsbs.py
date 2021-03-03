import nltk
# used to get context
from nltk.corpus import twitter_samples	
from nltk.tag import pos_tag
#from nltk import FreqDist
from nltk.stem import WordNetLemmatizer 
from nltk.corpus import wordnet

from nltk.corpus import stopwords

from nltk import classify
from nltk import NaiveBayesClassifier
from nltk.tokenize import word_tokenize

import re, string
import random
import json

# just create a CSV with all of the records in. Say, 75 records (not 100)

# https://www.digitalocean.com/community/tutorials/how-to-perform-sentiment-analysis-in-python-3-using-the-natural-language-toolkit-nltk

# Download NLTK data

'''
nltk.download('punkt')
nltk.download('twitter_samples')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')
'''

# Training data files
POSITIVE='positive_tweets.json'
NEGATIVE='negative_tweets.json'

# load tweet datasets
positive_tweets = twitter_samples.strings(POSITIVE)
negative_tweets = twitter_samples.strings(NEGATIVE)

# Tokenize positive tweets
tweet_tokens = twitter_samples.tokenized(POSITIVE)
# stop words: 'a stop word is a commonly used word (such as “the”, “a”, “an”, “in”) that a search engine has been programmed to ignore' 
stop_words = stopwords.words('english')

class DataProcessing:
    # remove noise data
    def denoise(self, tweet_tokens, stop_words = ()):
        '''
        ** Remove noise from the data **
        - This includes links, @s <- this could do with changing for Reddit. Is it even necessary other than for links?
        '''
        cleaned_tokens = []
        for token, tag in pos_tag(tweet_tokens):
            token = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|'\
                        '(?:%[0-9a-fA-F][0-9a-fA-F]))+','', token)
            token = re.sub("(@[A-Za-z0-9_]+)","", token)

            if tag.startswith("NN"):
                pos = 'n'
            elif tag.startswith('VB'):
                pos = 'v'
            else:
                pos = 'a'

            lemmatizer = WordNetLemmatizer()
            token = lemmatizer.lemmatize(token, pos)
            
            if len(token) > 0 and token not in string.punctuation and token.lower() not in stop_words:
                cleaned_tokens.append(token.lower())
        return cleaned_tokens
    
    def tokenise(self,inputFile):
        '''
        ** Tokenise a string **
        - Splits a string into a list of substrings
        '''
        output=[]
        # create tokens
        tokens=twitter_samples.tokenized(inputFile)
        for token in tokens:
            #print("\n\n {}".format(token))
            output.append(self.denoise(token, stop_words))

        return output

    def get_data_for_model(cleaned_tokens_list):
        '''
        ** Convert training data to a dictionary **
        '''
        for tweet_tokens in cleaned_tokens_list:
            yield dict([token, True] for token in tweet_tokens)

class GetSentiment:
    def get_sentiment(self,inputStr=None, classifier_obj=None):
        # remove noise and tokenise
        custom_tokens = DataProcessing().denoise(word_tokenize(inputStr))
        return (classifier_obj.classify(dict([token, True] for token in custom_tokens)))
    
    
    def check_sentiment(self):
        # need to split this one up
        # Get positive and negative examples... then split the dataset up for training and testing
        positive_cleaned_tokens_list=DataProcessing().tokenise(POSITIVE)
        negative_cleaned_tokens_list=DataProcessing().tokenise(NEGATIVE)
        train_data, test_data = TrainModel.split_dataset(positive_cleaned_tokens_list, negative_cleaned_tokens_list)
        classifier=TrainModel.build_classifier(train_data)
        return classifier

class TrainModel:
    def split_dataset(positiveTokens, negativeTokens):
        # get a dictionary of tokens for all positive and negative entries
        positive_tokens_for_model = DataProcessing.get_data_for_model(positiveTokens)
        negative_tokens_for_model = DataProcessing.get_data_for_model(negativeTokens)

        # split data into training and testing
        positive_dataset = [(tweet_dict, "Positive")
                            for tweet_dict in positive_tokens_for_model]

        negative_dataset = [(tweet_dict, "Negative")
                            for tweet_dict in negative_tokens_for_model]

        # combine positive and negative datasets into one dataset
        dataset = positive_dataset + negative_dataset
        random.shuffle(dataset)
        # split the dataset up
        train_data = dataset[:7000]
        test_data = dataset[7000:]
        
        return [train_data, test_data]

    def build_classifier(train_data):
        return NaiveBayesClassifier.train(train_data)


def main():
    # build the classifier
    constructed_classifier=GetSentiment().check_sentiment()
    # classift something
    print(GetSentiment().get_sentiment(inputStr='George was excited to visit the zoo', classifier_obj=constructed_classifier))
    print(GetSentiment().get_sentiment(inputStr='George hates the zoo', classifier_obj=constructed_classifier))

if __name__ == "__main__":
    main()
    
