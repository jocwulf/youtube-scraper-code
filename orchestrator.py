#!/usr/bin/python
"""
TODO: File description
"""

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

from test_celery.utils import *
from test_celery.tasks import *


# Specify Youtube API Keys here, the tool will distribute the API requests over the quotas associated with different keys through the get_random_api_access method
DEVELOPER_KEYS = [
"AIzaSyChV4ClaMn3SLRb2Ks6S0lLKcD9zKLveBA",
"AIzaSyDflZM0oBLQ4Zy22UITXmxS-YQEN6gTSWc",
"AIzaSyBJv2rSblBo1qTf_qpVB4KzZRhBzY14PlQ",
"AIzaSyCFp5JjMeB6INbTopPbTRwJXCeBnhvPtI4",
"AIzaSyB3bFpEKCg9oYdubxejY783JsJOvpd8E8Q",
 "AIzaSyDzIM-HL6bHYKJc9IQ9WKg1D07eAY9I5tg",
 "AIzaSyBKYY06phdfZwh22kJgCh9phzX965beiBI",
 "AIzaSyDje7dprx2ANe6DOWJix8yh6HF6f9p_FTQ",
 "AIzaSyDgVRzebzBCeOdwHXK3mrDKU3JcG-3Aa7s",
 "AIzaSyBySML8O8Z-GeVZNrOWwFuKnDempAt-qYU"
]
youtube = [build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=key) for key in DEVELOPER_KEYS]


YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def get_random_api_access():
  return youtube[randint(0, len(DEVELOPER_KEYS) - 1)]


if __name__ == "__main__":

  # Configure required command line arguments
  argparser.add_argument("--csv", help="Required: Path to CSV file with company names and channel urls, also takes web urls, if starting with http://", required=True)
  argparser.add_argument('--validate_urls_only', action='store_true')
  args = argparser.parse_args()
  
  i = 0

  # Request videoCategories from API and save in database, if these are not already in the database 
  if (db.videoCategories.count() == 0):
    db.videoCategories.insert_many(get_video_categories(get_random_api_access()))
    print "Loaded video category mappings into database" 
  

  # Load channel urls from specified direct web url to csv file or from specified path to local csv file
  i = 0
  v = 0

  if args.csv.startswith("https://") or args.csv.startswith("http://"):
    url = args.csv
    url = url.replace("www.dropbox.com", "dl.dropboxusercontent.com")  # if dropbox sharing url is specified, automatically convert it to direct link to the file
    handle = requests.get(url).iter_lines()
  else:
    handle =  open(args.csv, "r")

  r = csv.reader(handle, delimiter=";")
  

  # The tool can either be started in normal (collection) mode or in validation mode, where the provided channel urls are only checked, but no data for the channels is collected 
  if args.validate_urls_only == True:
    print "Validating provided channel urls.."
  else:
    print "Initating collecting.."
  
  for company in r:
    
    # skip empty lines in provided file
    if len(company) == 0:
      continue

    i += 1

    security_name = company[0]  
    channel_url = company[1] 
    
    if channel_url: # skip this entry from the file, if no check if a channel url has been specified for a company

      # Extract channeld id from the provided channel url using text operations and the Youtube API
      try:
        channel_id =  get_channel_id_from_url(get_random_api_access(), channel_url)
      except Exception, exc:
        logging.error(" Extracting channel id from " + str(channel_url) + " failed")
        continue
      
      # Validate extracted channel id by trying to retreive information about the channel from the Youtube API
      if validate_channel_id(get_random_api_access(), channel_id) == False:
        logging.error(" Extracted channel id " + channel_id +  " from " + channel_url + " is invalid.")
        continue
       
      # If valdidation only mode is not activated, check if channel is already saved in database. If not iniatiate data collection
      if args.validate_urls_only == False:
        if check_item_exists(channels, "_id", channel_id) == True:
          continue  
	  
        print("Initated data collection and scraping for " + channel_id + " " + security_name)
        parse_channel.delay(get_random_api_access(), channel_id, security_name)     
      v += 1
    else:
      logging.warning("WARNING: No url specified for " + security_name)  
  
  # Output statistics about results
  if args.validate_urls_only == False:
    print "Result: Succesfully initated data collection for " + str(v) + " / " + str(i) + " channels"
  else:
    print "Result:" + str(v) + " / " + str(i) + " channel urls valid"