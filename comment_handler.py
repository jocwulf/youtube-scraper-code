  #!/usr/bin/python
import httplib2
import json
import os
import sys
import logging
import csv

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.tools import argparser
import pdb


from random import randint

DEVELOPER_KEYS = [
# "AIzaSyChV4ClaMn3SLRb2Ks6S0lLKcD9zKLveBA",
# "AIzaSyDflZM0oBLQ4Zy22UITXmxS-YQEN6gTSWc",
# "AIzaSyBJv2rSblBo1qTf_qpVB4KzZRhBzY14PlQ",
# "AIzaSyCFp5JjMeB6INbTopPbTRwJXCeBnhvPtI4",
# "AIzaSyB3bFpEKCg9oYdubxejY783JsJOvpd8E8Q",
#  "AIzaSyDzIM-HL6bHYKJc9IQ9WKg1D07eAY9I5tg",
 "AIzaSyBKYY06phdfZwh22kJgCh9phzX965beiBI",
 "AIzaSyDje7dprx2ANe6DOWJix8yh6HF6f9p_FTQ",
 "AIzaSyDgVRzebzBCeOdwHXK3mrDKU3JcG-3Aa7s",
 "AIzaSyBySML8O8Z-GeVZNrOWwFuKnDempAt-qYU"
]
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def get_random_api_access():
  return youtube[randint(0, len(DEVELOPER_KEYS) - 1)]

if __name__ == "__main__":

  from test_celery.utils import *
  from test_celery.tasks import *
  youtube = [build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=key) for key in DEVELOPER_KEYS]
 

  # pdb.set_trace()

  # parse_video(get_random_api_access(), "6MdkPES4KYM", "test_comp", "test_channel")

  # parse_channel(get_random_api_access(),"UCWMWLPrg_mnD9czEj1jne8Q", "ABB")
  
  argparser.add_argument("--csv", help="Required: Path to CSV file with company names and channel urls, also takes web urls, if starting with http://", required=True)
  argparser.add_argument('--validate_urls_only', action='store_true')
  args = argparser.parse_args()
  
    

  youtube = [build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=key) for key in DEVELOPER_KEYS]

  i = 0

  
  # find all videos in database for which caption details have not been retrieved yet
  # try:
  #   for video in videos.find({"details.contentDetails.automaticSpeechRecognitionCaption": None}, no_cursor_timeout=True, modifiers={"$snapshot": True}):
  #     i += 1
  #     get_caption_metadata(get_random_api_access(), video)
  #     print "scheduled" + str(i)
  
  # except:
  #   pass

  # fetch and save videoCategories if these are not already in the database 
  if (db.videoCategories.count() == 0):
    db.videoCategories.insert_many(get_video_categories(get_random_api_access()))
    print "Loaded video category mappings into database" 
  

  # load channel urls from inputed csv file
  i = 0
  v = 0

  if args.csv.startswith("https://") or args.csv.startswith("http://"):
    url = args.csv
    url = url.replace("www.dropbox.com", "dl.dropboxusercontent.com") #convert dropbox sharing url to direct link if necessary
    handle = requests.get(url).iter_lines()
  else:
    handle =  open(args.csv, "r")

  r = csv.reader(handle, delimiter=";")
  
  if args.validate_urls_only == True:
    print "Validating provided channel urls.."
  else:
    print "Initating scraping.."
  
  for company in r: #open csv and skip header
    i += 1

    security_name = company[0]  
    channel_url = company[1] 
    
    if channel_url: # check if a channel url has been specified or if no channel exists for this company
      try:
        channel_id =  get_channel_id_from_url(get_random_api_access(), channel_url)
      except Exception, exc:
        logging.error(" Extracting channel id from " + str(channel_url) + " failed")
        continue
      
      if validate_channel_id(get_random_api_access(), channel_id) == False:
        logging.error(" Extracted channel id " + channel_id +  " from " + channel_url + " is invalid.")
        continue
       
      if args.validate_urls_only == False:
        if check_item_exists(channels, "_id", channel_id) == True:
          continue  
	  
        print("Initated scarping of " + channel_id + " " + security_name)
        parse_channel.delay(get_random_api_access(), channel_id, security_name)     
      v += 1
    else:
      logging.warning("WARNING: No url specified for " + security_name)  
  
  if args.validate_urls_only == False:
    print "Result: Succesfully initated scraping of " + str(v) + " / " + str(i) + " channels"
  else:
    print "Result:" + str(v) + " / " + str(i) + " channel urls valid"