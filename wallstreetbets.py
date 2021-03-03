# -*- coding: utf-8 -*-
import requests
import csv
import time
import re
import json
from bs4 import BeautifulSoup
import pymongo
from datetime import datetime
import tzlocal
import mysql.connector
import nltk_wsbs
from collections import Counter
import itertools
from functools import reduce
import glob
import sys
'''
TYPE PREFIXES:
t1_ = comment
t2_ = account
t3_ = link (a post)
t4_ = message
t5_ = subreddit
t6_ = award
'''
class RedditAPI:
   def request_data(self, url):
      headers = {'User-Agent': 'Mozilla/5.0'}
      data = requests.get(url, headers=headers).json()
      return data

   def convert_unix_timestamp(timestamp):
      unix_timestamp=float(timestamp)
      local_timezone = tzlocal.get_localzone()
      local_time = datetime.fromtimestamp(unix_timestamp, local_timezone)
      local=(local_time.strftime("%d/%m/%Y %H:%M:%S"))
      remote_time = datetime.utcfromtimestamp(unix_timestamp)
      remote=(remote_time.strftime("%Y-%m-%d %H:%M:%S.%f+00:00 (UTC)"))
      return [local, remote]

   def get_recent_posts(self, count, sort='new', subreddit='wallstreetbets'):
      url="https://api.reddit.com/r/{}/new?limit={}".format(subreddit, count)
      return self.request_data(url)
   
   def comment_func(self, post_id, subreddit='wallstreetbets'):
      # grab comments for a particular post (providing the post_id)
      # we only want the short name (omit t3_x)
      try:
         post_id=post_id.split("_")[1] # this line isn't needed
      except:
         post_id=post_id
         
      url="https://api.reddit.com/r/{}/comments/{}".format(subreddit,post_id)
      return self.request_data(url)
   

class ParseComments:
   def convert_comment_dict(self, comment_dict):
      # think it's a case that the key doesn't exist
      # RUN through the json and see what the key suggests
      
      id=comment_dict['id'] if comment_dict['id'] != None or "" else "-"
      body=comment_dict['body'] if comment_dict['body'] != None or "" else "-"
      likes=comment_dict['likes'] if comment_dict['likes'] != None or "" else "-"
      awards=comment_dict['total_awards_received'] if comment_dict['total_awards_received'] != None or "" else "-"
      created=comment_dict['created'] if comment_dict['created'] != None or "" else "-"
      controversiality=comment_dict['id'] if comment_dict['controversiality'] != None or "" else "-"
      parent_id=comment_dict['parent_id'] if comment_dict['parent_id'] != None or comment_dict['parent_id'] != "" else "-"
      replies=comment_dict['replies']
         
      comment={
         'id':id,
         'body': body,
         'likes': likes,
         'awards': awards,
         'created': created,
         'controversiality': controversiality,
         'parent_id': parent_id,
         'replies': replies
      }
            
      return comment
   
   
   def extract_relevant(self, child):
      # extracts relevant fields from a child of "children" in the raw json
      comment_data=child['data']
      # replies are replies made to this specific comment

      replies=[]
      if "replies" in comment_data:
         if comment_data['replies'] != "":
            replies=comment_data['replies']
      # Conver tthe dictionary response to a pruned list
      reply_info=self.convert_comment_dict(comment_data)
      
      return reply_info
   
   def manage_reply(self, children):
      '''
      takes in a set of children from the raw json
      and by utilising extract_relevant returns
      every reply to that comment alongside the other
      fields we're interested in
      '''
      # 3d list stores all reply information
      findings=[] 
      # list to hold "children" - this is used so that we
      # can work through replies to replies recursively
      children_arr=[children]
      for children in children_arr:
         # run through every child making up children(_arr)
         if len(children) == 0:
            continue

         for child in children['data']['children']:
            # we're ignoring more comments for now - can get them using a post
            # API call if reqd, however. Only used for hidden comments
            if child['kind'] == "t3" or child['kind'] == 'more':
               continue
            # get reply information for child
            reply_data=self.extract_relevant(child)

            # isolate replies to the current comment (child)
            
            replies=reply_data['replies']
            # isolate all but replies from reply_data
            information = {k: reply_data[k] for k in list(reply_data)[:7]}
            # add the information to our (soon to be) 3d list of findings
            findings.append(information)
            # if there are replies to the current comment then we want
            # to add them to out checking list (children_arr) to process
            if replies != '':
               children_arr.append(replies)
               
      return findings
   

   def get_comment_info(self, comments_json):
      # create a list to hold all of the information we extract from the comments
      comment_store=[]
      # run through the comments - typically only two elements as part of this loop. One
      # for the original post, and another for the comments 
      for item in comments_json:
         # isolate "children"
         children=item['data']['children']
         # run though each child
         for child in children:
            # check to see if the information corresponds to a comment (t1_)
            # or a post (t3_) - we're only interested in comments
            if child['kind'] == "t3" or child['kind'] == 'more':
               continue
            # isolate the information we're concerned with.
            # Note: this only acts on an original comment
            # responding directly to a post, not a comment reply
            comment_data=child['data']
            # isolate comment information
            original_comment=self.convert_comment_dict(comment_data)
            # grab data on responses to the aforementioned original comment
            replies=self.manage_reply(original_comment['replies'])
            original_comment_pruned={k: original_comment[k] for k in list(original_comment)[:7]}
            # add all of the information to our store
            comment_store += ([original_comment_pruned] + replies)
      return comment_store


class ParsePost():
   def parse_post(post_json):
      # parse Reddit post json
      try:
          id=post_json['id']
      except:
          id=None
          
      try:
         title=post_json['title']
      except:
         title=None
         
      try:
         content=post_json['selftext']
      except:
         content=None

      try:
          author_id=post_json['author_fullname']
      except:
          author_id=None
          
      try:
         author=post_json['author']
      except:
         author=None

      try:
         flair=post_json['link_flair_text']
      except:
         flair=None
         
      try:
         likes=post_json['likes']
      except:
         likes=None

      try:
         # convert Unix timestamp
         created=post_json['created']
         created=RedditAPI.convert_unix_timestamp(created)[0]
      except:
         created=None
         
      values={
         "id": id,
         "title": title,
         "content": content,
         "author_id": author_id,
         "author": author,
         "flair": flair,
         "likes": likes,
         "created": created
      }
      return values

# NASDAQ SOURCE:
# https://www.nasdaq.com/market-activity/stocks/screener?exchange=NASDAQ&render=download

class Ticker:
   def list_files(self, folder='./tickers'):
      # list all of the files in the ./tickers directory
      return glob.glob("{}/*.csv".format(folder))
      
   def get_tickers(self, csv_path):
      # just grab all of ticker symbols from
      # the inputted csv and store in a list
      entries=[]
      with open(csv_path) as csv_file:
         for row in csv_file:
            entries.append(row.split(",")[0])
      entries=entries[1:]
      return entries
   
   def load_tickers(self):
      # get all of the files containing tickers
      ticker_files=self.list_files()
      # load all of the ticker info into an array
      tickers=[]
      for filename in ticker_files:
         tickers+=self.get_tickers(csv_path=filename)
      # return all of the tickers as a single list
      return tickers

   def check_for_tickers(tickers, string):
      '''
      Used to check which tickers exist in the
      inputted string (Reddit post/comment body)
      '''
      # where tickers is an array of ticker symbols
      # and string is the string to check for tags

      # some tickers are words/acronyms, so you might wish to skip those out
      banned_tags=["CEO", "VERY", "IRS", "GOOD", "IMO", "ALL", "YOLO", "A","E","I" "U"]
      for item in banned_tags:
         try:
            tickers.remove(item)
         except Exception:
            pass

      # list to store matching tags
      tags=[]
      for tag in tickers:
         # find all relevant tags in the string
         output=re.findall(rf"\b{tag}\b",string)
         # append the tag to our array
         if len(output) > 0:
            tags+=output
      return tags

class CreateCSV:
   def create_csv(data, prefix=None, output_name=None):
      # create a csv from a 2d list
      now = datetime.now()
      # custom naming structure - if a name isn't specified add in a timestamp
      output_name = "{}_{}.csv".format(prefix, now.strftime("%d-%m-%Y_%H-%M-%S")) if output_name is None else output_name
      # write rows    
      with open(output_name, "w", encoding='utf-8') as my_csv:
         csv_writer=csv.writer(my_csv, delimiter=',')
         csv_writer.writerows(data)



# Get the top 100 posts - returns JSON data
posts=RedditAPI().get_recent_posts(25)

# define headings for both post and comment csv's
post_csv_heading=['id','title','content', 'author_id', 'author', 'flair', 'likes', 'created', 'sentiment', 'comment a', 'a', 'comment b', 'b', 'comment c', 'c', 'sum comment sentiment']
comments_csv_heading=['id', 'body', 'likes', 'awards', 'created', 'controversiality', 'parent_id', 'comment ticker a', 'comment occurances a', 'comment ticker b', 'comment occurances b', 'comment ticker c', 'comment occurances c']
#comments_csv_heading=['id', 'post_id', 'body', 'likes', 'awards', 'num_comments', 'replies', 'created', 'controversiality', 'parent_id', 'link_id', 'comment ticker a', 'comment occurances a', 'comment ticker b', 'comment occurances b', 'comment ticker c', 'comment occurances c']

# create 2d list to store post information (opposed to comment)
post_information=[post_csv_heading]

# instantiate our sentiment analyser
constructed_classifier=new_nlp.GetSentiment().check_sentiment()

# Load tickers
tickers=Ticker().load_tickers()

# run through all of the posts, and grab the bits of information we want
for post in posts['data']['children']:
   # ...extract the bits we want
   post_data=post['data']
   post_elements=ParsePost.parse_post(post_data)
   # Optional step: skip if the post has no body (could also modify for flair e.g. dd only)
   if post_elements['content'] == "":
      continue

   # find tickers discussed in the post
   post_tickers=Ticker.check_for_tickers(tickers, post_elements['content'])
   # grab the top three tickers for the post
   top_three_posts=dict(itertools.islice(Counter(post_tickers).items(), 3))
   # get the sentiment of the post
   post_sentiment=new_nlp.GetSentiment().get_sentiment(inputStr=post_elements['content'], classifier_obj=constructed_classifier)   
   
   # get the comments associated with the post
   comments_json=RedditAPI().comment_func(post_id=post_elements['id'])
   comments=ParseComments().get_comment_info(comments_json)
   # placeholder for summed (overall) comment sentiment
   comment_sentiments=0
   # store for all comment information - ultimately converted to a csv
   comment_information=[comments_csv_heading]
    # store for all tickers found - post and comment
   total_tickers=[]
   
   for single_comment in comments:
      # get the tickers for each of the comments
      comment_tickers=Ticker.check_for_tickers(tickers, single_comment['body'])
      total_tickers=total_tickers+comment_tickers
      # get the sentiment of the comment (either +ve (1) or -ve (-1))
      comment_sentiment=new_nlp.GetSentiment().get_sentiment(inputStr=single_comment['body'], classifier_obj=constructed_classifier)
      sentiment_val=1 if (comment_sentiment == 'Positive') else -1
      comment_sentiments += sentiment_val
      # flatten comment dict to list so we can write to csv
      try:
         comments_as_list=list(single_comment.values())
      except:
         comments_as_list=[]   
      # Get the top three most mentioned tickers in the comment
      top_three_tickers=dict(itertools.islice(Counter(comment_tickers).items(), 3))
      # flatten the tickers dict to list so we can write to csv
      try:
         top_three_flat=list(reduce(lambda x, y: x + y, top_three_tickers.items()))
      except: 
         top_three_flat=[]
      # because we're creating the csv on the assumption that there are x columns,
      # we need to created padding in case there are fewer than 3 (3*2 = 6) tickers
      # in the comment    
      while (len(top_three_flat) < 6):
         top_three_flat.append('')
      # add the information we've collected to our list which is ultimately
      # transformed into a csv file
      comment_information.append(comments_as_list + top_three_flat)
   
   # write comments to csv, and affix the related post id into the 
   CreateCSV.create_csv(data=comment_information, prefix="comments_{}".format(post_elements['id']))
   # get the overall (comments + post) three most popular tickers
   top_tickers=dict(itertools.islice(Counter(total_tickers).items(), 3))
   # flatten the dictionary of top tickers to list so we can add to csv list
   try:
      top_tickers_flat=list(reduce(lambda x, y: x + y, top_tickers.items()))
   except:
      top_tickers_flat=[]
      
   # make sure array has 6 elements (even if empty), otherwise csv order is messed up
   # 6=(3 top tickers)*2
   while (len(top_tickers_flat) < 6):
         top_tickers_flat.append('')
      
   
   post_csv_row=list(post_elements.values()) + [post_sentiment] + top_tickers_flat + [comment_sentiments]
   post_information.append(post_csv_row)


# save posts to csv (this needs to be a function)
# need to add a header to this, too. Even if it's pushing an array with the headings in
CreateCSV.create_csv(data=post_information, prefix='posts')
print("Task Complete!")

