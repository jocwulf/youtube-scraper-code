from __future__ import absolute_import
from test_celery.celery import app
import time
    
from celery.contrib import rdb
from test_celery.utils import *

import logging

max_retries_parse_channel = 2
max_retries_parse_video = 2

@app.task
def get_caption_metadata(youtube, video):
      try:
        results = youtube.captions().list(
          part="snippet",
          videoId=video["_id"]
          
        ).execute()
        
      except HttpError, e:  
        if str(e.resp.status) == "404": #catch error that video_id can not be found (video might have been deleted/set to private in the meantime)
          print  ": Error in " + video["_id"] + str(e.resp)
        
          return
        else:
          
          return

          #raise e

      automaticSpeechRecognitionCaption = False

      captions = []
      for caption in results["items"]:
        
        # mark that video has automatically generated captions if one of the captions is of type ASR (automatic speech recognition)
        if caption["snippet"]["trackKind"] == "ASR":
          automaticSpeechRecognitionCaption = True      

        caption["videoId"] = video["_id"]
        caption["channelId"] = video["channelId"]
        caption["company"] = video["company"]
        caption["fetched_at"] = datetime.utcnow()  
        caption["_id"] = caption["id"]
        caption.pop("id", None)
    
        db.captions.save(caption)

      videos.update_one({"_id" : video["_id"]}, 
        {"$set": {"details.contentDetails.automaticSpeechRecognitionCaption": automaticSpeechRecognitionCaption}}
        )  

      print ": Updated " + video["_id"] + "  automaticSpeechRecognitionCaption = " + str(automaticSpeechRecognitionCaption)
  

@app.task(ignore_result=True, default_retry_delay=30, max_retries=max_retries_parse_channel) 
def parse_channel(youtube, channel_id, company):
  try:
    
    if check_item_exists(channels, "_id", channel_id):
      return
    

    
    channel = {}
    channel["_id"] = channel_id
    channel["company"] = company

    channel["details"] = get_channel_details(youtube, channel_id)
    
    parse_comments_for_channel(youtube, channel_id, company)
    parse_subscriptions_by_channel(youtube, channel_id, company)
    parse_activities_by_channel(youtube, channel_id, company)


    # parse videos of channel
    channel["video_ids"] = get_video_ids_by_playlist(youtube, channel["details"]["contentDetails"]["relatedPlaylists"]["uploads"])   
    for video_id in channel["video_ids"]:
      
      for attempt in range(max_retries_parse_video):
        try:
          parse_video(youtube, video_id, company, channel_id) 
        except Exception, e:
          if attempt == max_retries_parse_video - 1:
            raise e
          else:
            clean_video_data(video_id)
            print "Retrying parsing of video {}, attempt {}/{}".format(video_id, attempt + 1, max_retries_parse_video)
            logging.exception(e)
        else:
          break
		  
   
    parse_playlists(youtube, channel_id, company)
    
  except Exception, exc:
    clean_channel_data(channel_id)	
    logging.warning("Error parsing channel " + channel_id + ". Cleaned all data related to channel and initating retry:")
    logging.exception(exc)
    raise parse_channel.retry(exc=exc)
  else:
    channel["fetched_at"] = datetime.utcnow()
    channels.save(channel)
    print "parsed channel " + channel_id + " from company " + company 