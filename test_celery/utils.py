#!/usr/bin/python
"""
This file contains helper methods, e.g. for channel url parsing
"""

from apiclient.errors import HttpError
from urlparse import urlparse
from datetime import datetime
import pdb
import logging

from test_celery.settings import *

    
def check_item_exists(collection, IdFieldIdentifier, id):
    return bool(collection.find_one({IdFieldIdentifier: id}))
    
def delete_items(collection, IdFieldIdentifier, id):
    return collection.delete_many({ IdFieldIdentifier : id })
 

   
def get_channel_id_from_url(youtube, youtube_url):
  """
  Extract channeld id from a channel url (youtube_url) using text splitting.
  If the channel url includes a channel name instead of channel id, use the API to retreive channel id using extracted channel names
  
  Originally based on source code from: https://github.com/rhayun/python-youtube-api/blob/master/youtubeapi.py

  """
  path = parse_url_path(youtube_url)
  if '/channel' in path:
      segments = path.split('/')
      channel_id = segments[len(segments) - 1]
  elif '/user' in path:
      segments = path.split('/')
      username = segments[len(segments) - 1]
      channel_id = get_channel_id_by_name(youtube, username)
  elif '/c' in path:
      segments = path.split('/')
      username = segments[len(segments) - 1]
      channel_id = get_channel_id_by_name(youtube, username)
  else:
      raise Exception('Url is not a youtube URL: ' + youtube_url)

  return channel_id
    
def parse_url_path(url):
  array = urlparse(url)
  return array[2]


def get_channel_id_by_name(youtube, username):
  results = youtube.channels().list(
    part="snippet",
    forUsername=username,
    
  ).execute()
  
  return results["items"][0]["id"]
  
def validate_channel_id(youtube, channel_id):
  results = youtube.channels().list(
    part="snippet",
    id=channel_id,
    
  ).execute()
  
  # channel id is valid if at least one channel corresponds to provided id
  return results["pageInfo"]["totalResults"] > 0
   


def clean_channel_data(channel_id, delete_associated_videos=True):  
  """
  Remove all data associated with channel to allow clean restart of channel parsing 
  delete_associated_videos specifies if vidoes uploaded by the channel will be removed as well
  """
  if delete_associated_videos == True:
    videos.delete_many({ "channelId" : channel_id })
    videoComments.delete_many({ "channelId" : channel_id })
    videoReplies.delete_many({ "channelId" : channel_id })
    captions.delete_many({ "channelId" : channel_id })
    advancedVideoStatistics.delete_many({ "channelId" : channel_id })
  
  
  subscriptions.delete_many({ "channelId" : channel_id })
  channelComments.delete_many({ "channelId" : channel_id })
  channelReplies.delete_many({ "channelId" : channel_id })
  channel_activities.delete_many({ "channelId" : channel_id })
  playlists.delete_many({ "channelId" : channel_id })
  db.playlistItems.delete_many({ "channelId" : channel_id }) 

def clean_video_data(video_id):  
  """
  Remove all data associated with video to allow clean restart of channel parsing 
  """
  videos.delete_many({ "_id" : video_id })
  videoComments.delete_many({ "videoId" : video_id })
  videoReplies.delete_many({ "videoId" : video_id })
  playlists.delete_many({ "videoId" : video_id })
  advancedVideoStatistics.delete_many({ "videoId" : video_id })
  captions.delete_many({ "videoId" : video_id })

  