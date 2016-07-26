from __future__ import absolute_import
from test_celery.celery import app
import time
    
from celery.contrib import rdb
from test_celery.utils import *

import logging

# Configure maximum numbers of retries here
max_retries_parse_channel = 2
max_retries_parse_video = 2
  

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
    
    # Call helper to retreive basic channel information and save it temporarly until all other helper tasks finish without errors
    channel = get_channel_details(youtube, channel_id)

    # Call helpers that retreive and save comments, subscriptionns, activities and playlistinformation
    parse_comments_for_channel(youtube, channel_id, company)
    parse_subscriptions_by_channel(youtube, channel_id, company)
    parse_activities_by_channel(youtube, channel_id, company)
    parse_playlists(youtube, channel_id, company)


    # Retreives all videos uploaded by the channel and collects information fo each video by calling the respective helper methods
    # Implements the retry/error mechanism outlined in the method specification
    channel["video_ids"] = get_video_ids_by_playlist(youtube, channel["contentDetails"]["relatedPlaylists"]["uploads"])   
    for video_id in channel["video_ids"]:
  
      for attempt in range(max_retries_parse_video):
        try:
          parse_video(youtube, video_id, company, channel_id) 
        except Exception, e:
          if attempt == max_retries_parse_video - 1:
            raise e
          else:
            clean_video_data(video_id)
            logging.warning("Retrying parsing of video %s, attempt %s/%s", video_id, attempt + 1, max_retries_parse_video, exc_info=e)
        else:
          break

    
  except Exception, exc:
    clean_channel_data(channel_id)	
    logging.warning("Error parsing channel %s. Cleaned all data related to channel and initating retry:", channel_id, exc_info=exc)
    raise parse_channel.retry(exc=exc)
  else:

    # Add indexing fields as well as timestamp and save basic channel information in the database
    channel["fetched_at"] = datetime.utcnow()
    channel["_id"] = channel_id
    channel["company"] = company
    channels.save(channel)
    print "parsed channel " + channel_id + " from company " + company 
