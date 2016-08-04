
#!/usr/bin/python
"""
This file contains information collection procedures that use the youtube data API.
It is advised to read the data structure documentation (PDF) prior to diving into the code
Most procedures following a similar structure (Configuring API Call, paginated information retrival, error handling, data cleanup and saving) and thus are not commented in detail.
Please refer to parse_comments_for_video() for detailed comments on the standard structure
""" 

from apiclient.errors import HttpError
from datetime import datetime
import logging
import pymongo

from test_celery.settings import *
from test_celery.utils import *



"""
============================== VIDEO ==============================
"""

def get_video_categories(youtube):
  """
  Returns a mapping of all possible video categories indepent of regions (some categories are only available in specific regions)
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/videoCategories/list
  """
  results = youtube.videoCategories().list(
    part="snippet",
    id=(",".join(map(str, (range(1,50))))),
    fields="items(id,snippet(title))"
    
  ).execute()
  
  categories = []
  for result in results["items"]:
    categories.append({
       "_id": result["id"],
       "title": result["snippet"]["title"],
       "fetched_at": datetime.utcnow()   
     })
     
  return categories  

  
def get_video_details(youtube, video_id):
  """
  Retreives and return general information about specified youtube video from the API
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/videos/list
  """
  results = youtube.videos().list(
    part="status,contentDetails,statistics,snippet,player,topicDetails",
    id=video_id,
  ).execute()
  
 
  "If API does not return any items for specified id, video is private/deleted so skip it"
  if len(results["items"]) == 0:
      return False

  return results["items"][0]


def parse_comments_for_video(youtube, video_id, company=None, channel_id=None):
  """
  Retrieves and saves all commentThreads for a given youtube video using the API and calls the parse_replies_for_video method for each thread to retreive and save the replies to each commentThread
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/comments/list
  """
   
  comment_threads = youtube.commentThreads() # sets api function to be called to retreiving all comment threads for a video
  request = comment_threads.list( #prepare api request
    part="snippet", # determines what information about the comments will be received (possible values in this case: snippet (General Information) and replies (part of the replies a comment, but not all)
    videoId=video_id,
    maxResults=100, 
    textFormat="plainText"
  )

  items = []
  try:
    while request is not None: # cast multiple api requests to receive paginated data until end is reached
      results = request.execute()
      items = items + results["items"]
      request = comment_threads.list_next(request, results)
      
  
  # Error Handling: Do not raise an exception if comments for a video are disabled
  except HttpError, e:  
    if str(e.resp.status) == "403": 
      pass
    else:
      raise e
      
  # clean up data received from youtube api and add meta data such as videId, channelId and timestamp
  for commentThread in items:
    commentThread["videoId"] = video_id
    commentThread["channelId"] = channel_id
    commentThread["company"] = company
    commentThread["fetched_at"] = datetime.utcnow()  
    commentThread["_id"] = commentThread["id"]
    commentThread.pop("id", None)

    # set flag if comment writer is channel owner
    if "authorChannelId" in commentThread["snippet"]["topLevelComment"]["snippet"]:
      commentThread["snippet"]["topLevelComment"]["snippet"]["authorIsChannelOwner"] = (commentThread["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"] == channel_id)
    else:
      commentThread["snippet"]["topLevelComment"]["snippet"]["authorIsChannelOwner"] = False
 
    # initate retreival of replies associated with this comment thread
    if int(commentThread["snippet"]["totalReplyCount"]) > 0:
      parse_replies_for_video(commentThread["_id"], youtube, video_id, company, channel_id)
      
    # save in database (update if already existing in database)  
    videoComments.save(commentThread)   

 
def parse_replies_for_video(thread_id, youtube, video_id=None, company=None, channel_id=None):
  """
  This method is called by the parse_comments_for_video method to retreive and save the replies to a given commentThread (thread_id)
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/comments/list
  """

  comments = youtube.comments()
  request = comments.list(
    part="snippet",
    parentId=thread_id,
    maxResults=100,
    textFormat="plainText"
  )

  items = []
  try:
    while request is not None:
      results = request.execute()
      items = items + results["items"]
      request = comments.list_next(request, results)
    
  except HttpError, e:  
      if str(e.resp.status) == "403": #ignore errors that are triggered if the comments for a video are disabled by uploader
        pass
      else:
        raise e
        
  for comment in items:
    comment["videoCommentThreadId"] = thread_id
    comment["videoId"] = video_id
    comment["channelId"] = channel_id
    comment["company"] = company
    comment["fetched_at"] = datetime.utcnow()  
    comment["_id"] = comment["id"]
    comment.pop("id", None)

    # set flag if comment writer is channel owner
    if "authorChannelId" in comment["snippet"]:
      comment["snippet"]["authorIsChannelOwner"] = (comment["snippet"]["authorChannelId"]["value"] == channel_id)
    else:
      comment["snippet"]["authorIsChannelOwner"] = False
    
    
    videoReplies.save(comment)
  
  
"""
============================== CHANNEL ==============================
"""


def get_video_ids_by_playlist(youtube, playlist_id):
  """
  Retreives and returns the ids of all videos saved in the specified playlist.
  This method is used by the parse_channel method to get all video_ids from the channel uploads playlist and initate  data collection of all videos uploaded by the channel
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/playlistItems/list
  """
  playlistItems =  youtube.playlistItems()
  request = playlistItems.list(
    part="contentDetails",
    playlistId=playlist_id,
    maxResults=50 
  ) 
  
  videos = []
  while request is not None:
    results = request.execute()
    
    for result in results.get("items", []):
      videos.append(result["contentDetails"]["videoId"])
      
    request = playlistItems.list_next(request, results)

  return videos


def get_channel_details(youtube, channel_id):
  """
  Retreives and return general information about specified youtube channel from the API
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/channels/list
  """
  results = youtube.channels().list(
    part="brandingSettings,contentDetails,contentDetails,id,invideoPromotion,snippet,statistics,status,topicDetails",
    id=channel_id,
    
  ).execute()
  
  return results["items"][0]
 
 
def parse_subscriptions_by_channel(youtube, channel_id, company):
  """
  Retrieves and saves all channels that the specified channel subscribed to. Does not initate data collection processes that collect information about these channels themselves
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/subscriptions/list
  """
  subscriptions = youtube.subscriptions()
  request = subscriptions.list(
    part="snippet",
    channelId=channel_id,
    maxResults=50,
  )

  items = []
  try:
    while request is not None:
      results = request.execute()
      items = items + results["items"]
      request = subscriptions.list_next(request, results)
  except HttpError, e:  
    if str(e.resp.status) == "403": #ignore errors that are triggered if the channel has no subscribed channels
      pass
    else:
      raise e
    
  for subscription in items:
    subscription["channelId"] = channel_id
    subscription["company"] = company
    subscription["fetched_at"] = datetime.utcnow()  
    subscription["_id"] = subscription["id"]
    subscription.pop("id", None)
    
    
    db.subscriptions.save(subscription)   
 
def parse_activities_by_channel(youtube, channel_id, company):
  """
  Retrieves and saves all information about the activities of the specified channel
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/activities/list 
  """
  activities = youtube.activities()
  request = activities.list(
    part="snippet, contentDetails",
    channelId=channel_id,
    maxResults=50,
  )

  items = []
  try:
    while request is not None:
      results = request.execute()
      items = items + results["items"]
      request = activities.list_next(request, results)
  except HttpError, e:  
    if str(e.resp.status) == "403" or str(e.resp.status) == "403": #ignore errors if not enough rights or channel not found
      pass
    else:
      raise e
    
  for activity in items:
    activity["channelId"] = channel_id
    activity["company"] = company
    activity["fetched_at"] = datetime.utcnow()  
    activity["_id"] = activity["id"]
    activity.pop("id", None)
    
    channel_activities.save(activity)   


def parse_comments_for_channel(youtube, channel_id, company):
  """
  Retrieves and saves all commentThreads for a given youtube channel using the API and calls the parse_replies_for_video method for each thread to retreive and save the replies to each commentThread
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/comments/list
  """
  
  comment_threads = youtube.commentThreads()
  request = comment_threads.list(
    part="snippet",
    channelId=channel_id,
    maxResults=100,
    textFormat="plainText"
  )

  items = []
  try:
    while request is not None:
      results = request.execute()
      items = items + results["items"]
      request = comment_threads.list_next(request, results)
    
  
  except HttpError, e:  
      if str(e.resp.status) == "403": #ignore errors that are triggered if the comments for a video are disabled by uploader
        pass
      else:
        raise e
        
  for commentThread in items:
    commentThread["channelId"] = channel_id
    commentThread["company"] = company
    commentThread["fetched_at"] = datetime.utcnow()  
    commentThread["_id"] = commentThread["id"]
    commentThread.pop("id", None)

    # set flag if comment writer is channel owner
    if "authorChannelId" in commentThread["snippet"]["topLevelComment"]["snippet"]:
      commentThread["snippet"]["topLevelComment"]["snippet"]["authorIsChannelOwner"] = (commentThread["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"] == channel_id)
    else:
      commentThread["snippet"]["topLevelComment"]["snippet"]["authorIsChannelOwner"] = False
    
  
    
    if int(commentThread["snippet"]["totalReplyCount"]) > 0:
      parse_replies_for_channel(commentThread["_id"], youtube, channel_id, company)
    
    #print("get commentThreads for channel " + channel_id)
    channelComments.save(commentThread) 
    

def parse_replies_for_channel(thread_id, youtube, channel_id, company):
  """
  This method is called by the parse_comments_for_channel method to retreive and save the replies to a given commentThread (thread_id)
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/comments/list
  """

  comments = youtube.comments()
  request = comments.list(
    part="snippet",
    parentId=thread_id,
    maxResults=100,
    textFormat="plainText"
  )

  items = []
  try:
    while request is not None:
      results = request.execute()
      items = items + results["items"]
      request = comments.list_next(request, results)
    
  
  except HttpError, e:  
      if str(e.resp.status) == "403": #ignore errors that are triggered if the comments for a video are disabled by uploader
        pass
      else:
        raise e
        
  for comment in items:
    comment["channelCommentThreadId"] = thread_id
    comment["channelId"] = channel_id
    comment["company"] = company
    comment["fetched_at"] = datetime.utcnow()  
    comment["_id"] = comment["id"]
    comment.pop("id", None)

    # set flag if comment writer is channel owner
    if "authorChannelId" in comment["snippet"]:
      comment["snippet"]["authorIsChannelOwner"] = (comment["snippet"]["authorChannelId"]["value"] == channel_id)
    else:
      comment["snippet"]["authorIsChannelOwner"] = False

    # pdb.set_trace()
    
    #print("get replies for thread_id " + thread_id)
    channelReplies.save(comment) 


  
def parse_regular_playlists(youtube, channel_id, company):
  """
  Retrieves and saves all regular playlists (not related playlists that for example contains likes of the channel) for a given youtube channel using the API and calls the parse_playlist_items method for each playlistItem
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/playlists/list
  """
    
  playlists = youtube.playlists()
  request = playlists.list(
    part="id,player,snippet,status",
    channelId=channel_id,
    
  )
    
  items = []
  while request is not None:
    results = request.execute()
    items = items + results["items"]
    request = playlists.list_next(request, results)
     
  for playlist in items:
    playlist["channelId"] = channel_id
    playlist["company"] = company
    playlist["type"] = "regular"
    playlist["fetched_at"] = datetime.utcnow()  
    parse_playlist_items(youtube, playlist["id"], "regular", channel_id, company)
    playlist["_id"] = playlist["id"]
    playlist["fetched_at"] = datetime.utcnow() 
    playlist.pop("id", None)

    db.playlists.save(playlist)


def parse_related_playlists(youtube, related_playlists, channel_id, company):
  """
  Retrieve and save related playlists (e.g. videos liked or favorited by the channel) for a given youtube channel using the API and calls the parse_playlist_items method for each playlistItem
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/channels, https://developers.google.com/youtube/v3/docs/playlists
  """

  # Loop through related playlists while ignorning the related playlist that only contains uploads
  for (playlist_type, related_playlist_id) in related_playlists.items():
    if playlist_type != "uploads":

      playlists = youtube.playlists()
      request = playlists.list(
        part="id,player,snippet,status",
        id=related_playlist_id,
        
      )
      
      results = request.execute()
      
      for playlist in results["items"]:
      
        playlist["channelId"] = channel_id
        playlist["company"] = company
        playlist["type"] = playlist_type
        playlist["fetched_at"] = datetime.utcnow()  
        parse_playlist_items(youtube, playlist["id"], playlist_type, channel_id, company)
        playlist["_id"] = playlist["id"]
        playlist["fetched_at"] = datetime.utcnow() 
        playlist.pop("id", None)

        db.playlists.save(playlist)

        
def parse_playlist_items(youtube, playlist_id, playlist_type, channel_id, company):   
  """
  This method is called by the parse_playlists method to retreive and save mor detailed information about a single playlistItem
  For each playlistItem it also calls the parse_video method to collect and save all relevant information about the video included in the playlistItem, if the video is not already in the database. 
  The later is always the case if a video only belongs to a playlist but not to a company that is in the master list. (See data documentation PDF)
  Youtube API Reference: https://developers.google.com/youtube/v3/docs/playlistItems/list
  """
  playlistItems =  youtube.playlistItems()
  request = playlistItems.list(
    part="contentDetails,snippet,status",
    playlistId=playlist_id,
    maxResults=50
    
  ) 
  
  items = []
  while request is not None:
    results = request.execute()
    items = items + results["items"]
    
    request = playlistItems.list_next(request, results)
 
  for playlistItem in items:
    playlistItem["playlistId"] = playlist_id
    playlistItem["playlistType"] = playlist_type
    playlistItem["channelId"] = channel_id
    playlistItem["company"] = company
    playlistItem["fetched_at"] = datetime.utcnow()  
    playlistItem["_id"] = playlistItem["id"]
    playlistItem.pop("id", None)


    """ deactivated for now to speed up crawling process, since currently not needed for research
    # parse and save video if not already in database (for the case that a playlist contains videos that belong to another channel)
    if playlistItem["snippet"]["resourceId"]["kind"] == "youtube#video":
      playlistVideoId = playlistItem["snippet"]["resourceId"]["videoId"]
      from tasks import parse_video  
      if check_item_exists(videos, "_id", playlistVideoId) == False:
        parse_video(youtube, playlistVideoId, None, None, playlistItem["snippet"]["playlistId"])
    """

    try:
      db.playlistItems.save(playlistItem) 
    except pymongo.errors.DuplicateKeyError:
      pass
    
    
 
