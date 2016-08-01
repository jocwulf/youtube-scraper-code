#!/usr/bin/python
"""
Contains Code for Command Line interaction, importing and validating channel urls, API connection and initiating data collection for channels
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
from test_celery.scraping import *
from test_celery.api import *
from test_celery.settings import *




YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
youtube = [build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=key) for key in DEVELOPER_KEYS]


def get_random_api_access():
  return youtube[randint(0, len(DEVELOPER_KEYS) - 1)]

if __name__ == "__main__":

  """
  channels = [
    "UCK8sQmJBp8GCxrOtXWBpyEA",
    "UCWf2ZlNsCGDS89VBF_awNvA",
    "UCBmwzQnSoj9b6HzNmFrg_yw",
    "UCnIQPPwWpO_EFEqLny6TFTw",
    "UCrRK02_CbvPEPrDYMQ1l49w",
    "UCbmNph6atAoGfqLoCL_duAg",
    "UC_x5XG1OV2P6uZZ5FSM9Ttw",
    "UCL8ZULXASCc1I_oaOT0NaOQ",
    "UCL0iAkpqV5YaIVG7xkDtS4Q",
    "UClH-KpveOLJszChWRfFmPfQ",
    "UCOn4qooT6CDN8MYyCKtbPXA",
    "UCRnX4Yc585eRTj4UvNkBM8w",
    "UCced_wdSslkOf7xc_0qyT8w",
    "UCAdEqOhSnzlLBtaapu567AQ",
    "UCAdEqOhSnzlLBtaapu567AQ",
    "UCCQsxghGcSyzpDqAP3c50ig",
    "UCXQexglLCaJyTImYLmSO9Ng",
    "UCDndFlHdnnygb1v3UwfCEuA",
    "UCosXctaTYxN4YPIvI5Fpcrw",
    "UCWZTQLvNSm92fZPnjVGFRIA"
  ]

  i = 0
  for channel in channels:
    print channel
    parse_channel(get_random_api_access(), channel, "company")

    i += 1
  """

  # Configure required command line arguments
  argparser.add_argument("--csv", help="Required: Path to CSV file with company names and channel urls, also takes web urls, if starting with http://", required=True)
  argparser.add_argument('--validate_urls_only', action='store_true')
  args = argparser.parse_args()
  

  # Request videoCategories from API and save in database, if these are not already in the database 
  if (db.videoCategories.count() == 0):
    db.videoCategories.insert_many(get_video_categories(get_random_api_access()))
    print "Loaded video category mappings into database" 
  

  # Load channel urls from specified direct web url to csv file or from specified path to local csv file
  i = 0 # keep track of urls processed from file
  v = 0 # keep track of number of parse_channel jobs started
  channel_ids = [] # Keep track of channel ids to catch duplicate channel ids within imported csv file
  duplicates_count = 0

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
      
      # Check for duplicate channel ids in input file
      if channel_id in channel_ids:
        duplicates_count += 1
        logging.error(" Duplicate channel id "  + str(channel_id) + " from " + str(channel_url) + " is included multiple times in the file")
        continue
      else:
        channel_ids.append(channel_id)

      # Validate extracted channel id by trying to retreive information about the channel from the Youtube API
      if validate_channel_id(get_random_api_access(), channel_id) == False:
        logging.error(" Extracted channel id " + channel_id +  " from " + channel_url + " is invalid")
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
    print "Result: Succesfully initated data collection for " + str(v) + " / " + str(i) + " channels. Ignored " + str(duplicates_count) + " duplicates and " + str(i-v-duplicates_count) + " errors"
  else:
    print "Result:" + str(v) + " / " + str(i) + " channel urls valid: " + str(duplicates_count) + " duplicates and " + str(i-v-duplicates_count) + " errors"