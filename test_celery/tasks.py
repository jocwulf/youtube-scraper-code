from __future__ import absolute_import
from test_celery.celery import app
import time
    
from celery.contrib import rdb
from test_celery.utils import *
from test_celery.scraping import *
# necessary to fix circular imports
from test_celery.api import get_video_categories, get_video_details,  parse_comments_for_video, parse_replies_for_video, get_video_ids_by_playlist, get_channel_details, parse_subscriptions_by_channel, parse_activities_by_channel, parse_comments_for_channel, parse_replies_for_channel

import logging

from test_celery.settings import *



@app.task(ignore_result=True, default_retry_delay=30, max_retries=max_retries_parse_channel) 
def parse_channel(youtube, channel_id, company):
  """
  Celery task that can be exectued on distributed systems.
  Collects information for the specified channel by calling the respective helper methods.
  Also starts the collection of information for all videos uploaded by the channel by calling a respective helper method.
  Saves basic channel information into the channels collection if all helpers successfully collected information.


  Also implements an error handling / retry systems, since uncaught errors are likely due to bugs in the Youtube API, network connections etc.:
    If any unhandled error is raised while excuting this method, information relating to that channels (incl. all information related to videos uploaded by the channel) saved in the database so far will be cleaned up by deletion the parse_channel task is retried with a 30s seconds delay. 
    If the number of attempts reaches the value of max_retries_parse_channel, an error is logged.

    if any unhandled error is raised while collection information for a video, all information relating to that video saved in the database so far will be cleaned up by deletion, the error will be logged as a warning and parsing will be restarted.
    If the number of retries specified in max_retries_parse_video is reached for a video, this triggers a retstart of the parse_channel method according to the rules specified above.
  """
  try:
    
    # If the channel is already saved in the channels collection we can skip it, since it means all information about the channel was previously successfully collected and save in the channel collection and other collections
    if check_item_exists(channels, "_id", channel_id):
      return

    # make sure no leftover data is left in some collection
    clean_channel_data(channel_id, False)    
    
    # Call helper to retreive basic channel information and save it temporarly until all other helper tasks finish without errors
    channel = get_channel_details(youtube, channel_id)
  
    # Call helpers that retreive and save comments, subscriptionns and activities 
    parse_comments_for_channel(youtube, channel_id, company)
    parse_subscriptions_by_channel(youtube, channel_id, company)
    parse_activities_by_channel(youtube, channel_id, company)


    # Retreives all videos uploaded by the channel and collects information fo each video by calling the respective helper methods
    # Implements the retry/error mechanism outlined in the method specification
    channel["video_ids"] = get_video_ids_by_playlist(youtube, channel["contentDetails"]["relatedPlaylists"]["uploads"])   
    
    i = 0
    for video_id in channel["video_ids"]:
      for attempt in range(max_retries_parse_video):
        try:
          parse_video(youtube, video_id, company, channel_id) 

        except Exception, e:
          if attempt == max_retries_parse_video - 1:
            raise e
          else:
            clean_video_data(video_id)
            logging.warning("Retrying parsing of video %s from channel %s, attempt %s/%s", video_id, channel_id, attempt + 1, max_retries_parse_video, exc_info=e)

        else:
          i += 1
          print "parsed video: " + video_id + "  " + str(i) + "/" + str(len(channel["video_ids"])) + " of channel: " + str(channel_id) + " company: " + str(company)
          break


    # Call helpers that retrieve and save regular playlists of the channel.
    from test_celery.api import (parse_regular_playlists, parse_related_playlists)
    parse_regular_playlists(youtube, channel_id, company)

    # Retrieve and save related playlists (e.g. videos liked or favorited by the channel)
    parse_related_playlists(youtube, channel["contentDetails"]["relatedPlaylists"], channel_id, company)
    
  except Exception, exc:
    clean_channel_data(channel_id, False)	
    logging.warning("Error parsing channel %s. Cleaned all data related to channel and initating retry:", channel_id, exc_info=exc)

    raise parse_channel.retry(exc=exc)
  else:

    # Add indexing fields as well as timestamp and save basic channel information in the database
    channel["fetched_at"] = datetime.utcnow()
    channel["_id"] = channel_id
    channel["company"] = company
    channels.save(channel)
    print "parsed channel " + channel_id + " from company " + company 


def parse_video(youtube, video_id, company=None, channel_id=None):
  """
  Collects information for the specified video by calling the respective helper methods.
  Saves basic video information into the videos collection if all helpers successfully collected information.
  """

  # Skip  if video already in database
  if check_item_exists(videos, "_id", video_id):
    return False

  """ deactivated crawling of videos that only belong to a playlist but not to a company that we are crawling
  for now to speed up crawling process, since currently not needed for research
  if check_item_exists(videos, "_id", video_id):

    # check if video was previously saved in database as only belonging to a playlist but not a channel, since when parsing that playlist, the channel to which the video belongs was not yet parsed yet
    # if yes, reparse the video record to reflect that it belongs to a channel
    
    existing_video = videos.find_one({"_id": video_id})
    
    if existing_video["channelId"] == None:
      delete_items(videos, "_id", video_id)
    else:
      return

  """
  
  # make sure no leftover data is left in some collection
  clean_video_data(video_id)    
     

  # Call helper to retreive basic video information and save it temporarly until all other helper tasks finish without errors
  video = get_video_details(youtube, video_id)

  "if get_video_details returns false, it means youtube API did not find video with specified ID, thus video is private/deleted"
  if video == False:
    logging.warning("No video was returned by API for video_id: " + str(video_id) + " channel: " + str(channel_id) + " company: " + str(company))
    return False


  # Call helpers that retreive and save comments for the video  
  parse_comments_for_video(youtube, video_id, company, channel_id)

  # Call helpers that retreive and save extended statistics and captions, overwrite the retreived corresponding flags in the video object, since the values returned by videos are often inaccurate
  video["status"]["publicStatsViewable"]  =  parse_advanced_statistics(video_id, company, channel_id)
  video["contentDetails"]["caption"] = process_manual_captions(video_id, company, channel_id)

  # youtube api does not return caption information for unlisted videos
  if video["status"]["privacyStatus"] == "unlisted": 
    video["contentDetails"]["autogeneratedCaption"] = False
  else:  
    video["contentDetails"]["autogeneratedCaption"] = process_asr_caption(youtube, video_id, company, channel_id)
   
  # Add indexing fields as well as timestamp and save basic channel information in the database 
  video["fetched_at"] = datetime.utcnow()
  video["_id"] = video_id 
  video["company"] = company
  video["channelId"] = channel_id
  videos.save(video)

  return True

