import json
from apiclient.errors import HttpError
from urlparse import urlparse
from pymongo import MongoClient
from datetime import datetime
import requests
import re 
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pdb
import pprint
from bs4 import BeautifulSoup,CData
from HTMLParser import HTMLParser
import logging


# client = MongoClient("mongodb://scrape:scrape@ds015024-a0.mlab.com:15024/youtube-scraping-2", connect=False)
client = MongoClient("mongodb://scrape:scrape@ds061954.mlab.com:61954/youtube-scraping-1", connect=False)
# client = MongoClient("mongodb://localhost/finalTestSPI", connect=False)

db = client.get_default_database()
videos = db.videos
channels = db.channels
videoComments = db.videoCommentThreads
videoReplies = db.videoCommentThreadReplies
channelComments = db.channelCommentThreads
channelReplies = db.channelCommentThreadReplies
videoCategories = db.videoCategories
playlists = db.playlists
companies = db.companies
youtubeUrls = db.youtubeUrls
subscriptions = db.subscriptions
advancedVideoStatistics = db.advancedVideoStatistics
captions = db.captions

"""todo: add primary keys """


#TODO: MOVE THIS
#@app.task(ignore_result=True, default_retry_delay=30, max_retries=3) 
# TODO Document: playlist_id only set if video was parsed because it is included in a playlist of a parsed channel, but the video belongs to a channel that is not in our database
def parse_video(youtube, video_id, company=None, channel_id=None, playlist_id=None):
  #skip parsing if video already in database
  if check_item_exists(videos, "_id", video_id):

    """ check if video was previously saved in database as only belonging to a playlist but not a channel, since when parsing that playlist, the channel to which the video belongs was not yet parsed yet
    if yes, update the video record to reflect that it belongs to a channel"""
    
    existing_video = videos.find_one({"_id": video_id})

    pdb.set_trace()

    if existing_video["channelId"] == None:
      videos.update_one({"_id" : video_id}, 
        {
        "$set": {  "channelId": channel_id,
                   "company": company,
                   "playlist_id": None
                  }}
        )  
    return

  parse_comments_for_video(youtube, video_id, company, channel_id, playlist_id)
    
  video = {}
  video["_id"] = video_id #mongodb primary key
  video["company"] = company
  video["channelId"] = channel_id
  video["playlistId"] = playlist_id
  video["details"] = get_video_details(youtube, video_id)

  "if get_video_details returns false, it means youtube API did not find video with specified ID, thus video is private/deleted"
  if video["details"] == False:
    logging.warning("No video was returned for id {} by API".format(video_id))
    return

  #if video["details"]["status"]["publicStatsViewable"]: # youtube api field is not reliable
  parse_advanced_statistics(video_id, company, channel_id, playlist_id)

  video["details"]["contentDetails"]["caption"] = process_manual_captions(video_id, company, channel_id, playlist_id)

  if video["details"]["status"]["privacyStatus"] == "unlisted": # youtube api does not return caption information for unlisted videos
    video["details"]["contentDetails"]["autogeneratedCaption"] = False
  else:  
    video["details"]["contentDetails"]["autogeneratedCaption"] = process_asr_caption(youtube, video_id, company, channel_id, playlist_id)
    
  video["fetched_at"] = datetime.utcnow()

  videos.save(video)
  print "parsed video: " + video_id + " channel: " + str(channel_id) + " company: " + str(company) + " playlist: " + str(playlist_id)



""" 
TODO: DOC 
"""
def parse_advanced_statistics(video_id, company=None, channel_id=None, playlist_id=None):

  """ 
  Step 1:
  Load video page and extract session token and selected cookies that will be required to authorize the HTTP request that retrieves the statistics data later on
  We need to use a headless browser (CasperJS) to load the website, since the session token will only be injected into the web page source code by Youtube after the whole website has been loaded
  """
  
 
  """ use phantomJS to open website and wait until session token has been inserted into page by Youtube and get page source code """
  
  driver = webdriver.PhantomJS()
  driver.set_window_size(1000, 500)

  timeout = 10 
  driver.get("https://www.youtube.com/watch?v=" + video_id)
  WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.NAME, "session_token"))) 
  video_page_source = driver.page_source

  """  use regular expression to extract session token, might be necessary to adjust expression in the future if youtube changes the struture of the video page source code """
  session_token = None
  match = re.search('<input name="session_token" type="hidden" value="(.+)"></form>', video_page_source)

  if match != None:
    session_token = match.group(1)
  else:
    raise Exception("Could not extract session token for video id " + video_id + " from channel " + channel_id + " of company " + company)

  """ extract some selected cookies needed for request later """
  cookies = driver.get_cookies()
  
  cookie_out = {}
  for cookie in cookies:
    if cookie['name'] in ['YSC', 'VISITOR_INFO1_LIVE']:
      cookie_out[cookie['name']] = cookie['value']
 
  driver.quit()

  
  
  """ 
  Step 2:
  Use extracted session_token and cookies to request video statistics
  """

  url = "https://www.youtube.com/insight_ajax?action_get_statistics_and_data=1&v=" + video_id

  data = { "session_token": session_token}
  headers = {"content-type": "application/x-www-form-urlencoded"}

  r = requests.post(url, data=data, headers=headers, cookies=cookie_out)


  """ 
  Step 3: Extract statistics data from request response
  """
  statistics_data_raw = None

  soup = BeautifulSoup(r.text,"html.parser")
  for cdataNode in soup.findAll(text=True):
    if isinstance(cdataNode, CData) and cdataNode.parent.name == "graph_data":
      statistics_data_raw = json.loads(cdataNode)
  
  
  if statistics_data_raw == None: # no advanced statistics available for video
    # pdb.set_trace()
    #logging.warning("Advanced statistics CData not found for video " + str(video_id) + " even though video.status.publicStatsViewable is true, probably a bug in the API")
    return False
    

  """ save statistics data to mongodb """
  """ TODO: add primary key for videoId """
  statistics_data_raw["videoId"] = video_id
  statistics_data_raw["channelId"] = channel_id
  statistics_data_raw["company"] = company
  statistics_data_raw["playlistId"] = playlist_id
  statistics_data_raw["fetched_at"] = datetime.utcnow()  

  """ clean up
  many ifs are required, since for some videos, parts of the data are missing, e.g. no daily data or no watch-time data"""
  if "views" in statistics_data_raw:
    
    if "cumulative" in statistics_data_raw["views"]:
      statistics_data_raw["views"]["cumulative"].pop("opt", None)

    if "daily" in statistics_data_raw["views"]:
      statistics_data_raw["views"]["daily"].pop("opt", None)

  if "watch-time" in statistics_data_raw:

    if "cumulative" in statistics_data_raw["watch-time"]:
      statistics_data_raw["watch-time"]["cumulative"].pop("opt", None)

    if "daily" in statistics_data_raw["watch-time"]:
      statistics_data_raw["watch-time"]["daily"].pop("opt", None)

  if "shares" in statistics_data_raw:

    if "cumulative" in statistics_data_raw["shares"]:
      statistics_data_raw["shares"]["cumulative"].pop("opt", None)

    if "daily" in statistics_data_raw["shares"]:
      statistics_data_raw["shares"]["daily"].pop("opt", None)

	
  advancedVideoStatistics.save(statistics_data_raw)

""" 
Download youtube auto generated caption (always in language of the video)
Returns true if auto generated caption found and saved, returns false if not found
"""
def process_asr_caption(youtube, video_id, company=None, channel_id=None, playlist_id=None):
  """ 
  Step 1: Open video page in headless browser and extract authentification parameters
  """
  driver = webdriver.PhantomJS()
  driver.set_window_size(1000, 500)

  driver.get("https://www.youtube.com/watch?v=" + video_id)
 
  """ Extract the authentification parameters needed to request the autogenerated subtitles by executing the following javascript code in the scope of the headless browser """
  authenticated_url = driver.execute_script('return yt.getConfig("TTS_URL");')

  driver.quit()

  if authenticated_url == "":
      return False

  """ 
  Step 2:
  Use extracted authentification parameters to request and save caption (xml format)
  while using get_asr_language helper to determine language of auto generated caption
  """

  video_language = get_asr_language(youtube, video_id)

  # if no video language was found, no asr captions are available for the video
  if video_language == False:
    return

  url = authenticated_url + "&name&kind=asr&type=track&lang=" + video_language
  r = requests.get(url)
 
  """ if response empty or 404 error, no automatically generated caption is available for the video """
  if r.text.find("<transcript>") < 0:
    print "error"
    print asr_subtitle_xml
    return False

  """ 
  Step 3:
  Format xml formatted caption and extract plain text from xml formatted caption using regular expression 
  """
  caption = clean_captions_xml_and_extract_plain_text(r.text)

  """ TODO: add primary key for videoId """

  """ save in mongodb TODO: check if encoding saved correctly"""
  captions.save(
    {
    "type": "autoGenerated",
    "videoId": video_id,
    "channelId": channel_id,
    "playlistId": playlist_id,
    "company": company,
    "fetched_at": datetime.utcnow(),
    "xml": caption["xml"],
    "plainText": caption["plainText"],
    "language": video_language
   }
  )

  return True


def strip_empty_lines(string):
  return "\n".join([line for line in string.splitlines() if line.strip() != ""])

def clean_captions_xml_and_extract_plain_text(xml):
  caption = {}
  """ we need to unescape the unicode string returned by youtube, since some special characters are returned as html entities e.g. " is returned as &ldquo; """ 
  caption["xml"] = strip_empty_lines(HTMLParser().unescape(HTMLParser().unescape(xml))) # needs to be called twice because of bug in the function

  """   Extract plain text from xml formatted caption using regular expressions and some back and forth to get all the line breaks right """
  #TODO: Remove: <[^>]*> *: zero or more occurences of [^>]: all characters except for >
  subtitle_xml_stripped = re.sub("<[^>]*>", "\n", caption["xml"].replace("\n"," "))   # first remove all line breaks, then replace all xml tags with line breaks
  caption["plainText"] =  strip_empty_lines(subtitle_xml_stripped)

  return caption

"""
TODO: DOC
"""
def get_asr_language(youtube, video_id):
  """ fetch list of captions from API """ 
  try:
    results = youtube.captions().list(
      part="snippet",
      videoId=video_id
      
    ).execute()
  except HttpError, e:  
      if str(e.resp.status) == "404": #ignore errors if video is unlisted or blocked
        pass
      else:
        raise e

  """ find auto generated caption and return language if found otherwise return false """
  for caption in results["items"]:
    if caption["snippet"]["trackKind"] == "ASR":
      return caption["snippet"]["language"]    

  return False
  


""" 
Download all manually created captions for a specified video_id
Returns true if at least one caption found and saved, returns false if none found
"""
def process_manual_captions(video_id, company=None, channel_id=None, playlist_id=None):
  """ retrieve all manually created captions for the video and convert to a python list using BeautifulSoup """
  r = requests.get("https://video.google.com/timedtext?hl=en&type=list&v=" +  video_id)
  
  xml_soup = BeautifulSoup(r.text, "lxml") # TODO: add to setup docu, try running pip install --upgrade lxml if any issues in this line

  captions_list = [{
    "name": track.attrs["name"],  
    "language": track.attrs["lang_code"]  
    } 
    for track in xml_soup.findAll("track")
  ]

  if len(captions_list) == 0:
    return False

  for caption_meta_info in captions_list:
    r = requests.get("https://video.google.com/timedtext?hl={0}&lang={0}&name={1}&v={2}".format(caption_meta_info["language"], caption_meta_info["name"], video_id))
    
    """ Format xml formatted caption and extract plain text from xml formatted caption using regular expression  """
    caption = clean_captions_xml_and_extract_plain_text(r.text)
  
    """ save to mongodb """
    captions.save(
    {
    "type": "manual",
    "videoID": video_id,
    "channelId": channel_id,
    "playlistId": playlist_id,
    "company": company,
    "fetched_at": datetime.utcnow(),
    "xml": caption["xml"],
    "plainText": caption["plainText"],
    "language": caption_meta_info["language"],
    "name": caption_meta_info["name"]
    }
    )

  return True

    
def check_item_exists(collection, IdFieldIdentifier, id):
    return bool(collection.find_one({IdFieldIdentifier: id}))

# returns a mapping of all possible video categories indepent of regions (some categories are only available in specific regions)
def get_video_categories(youtube):
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
  results = youtube.videos().list(
    part="status,contentDetails,statistics,snippet",
    id=video_id,
  ).execute()
  
 
  "if API does not return any items for specified id, video is private/deleted"
  if len(results["items"]) == 0:
      return False

  return results["items"][0]

def get_channel_details(youtube, channel_id):
  results = youtube.channels().list(
    part="brandingSettings,contentDetails,contentDetails,id,invideoPromotion,snippet,statistics,status,topicDetails",
    id=channel_id,
    
  ).execute()
  
  #print("get channel details for " + channel_id)
  return results["items"][0]
 
 
def parse_subscriptions_by_channel(youtube, channel_id, company):
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
    
    
    db.activities.save(activity)   
  
  
def get_video_ids_by_playlist(youtube, playlist_id):

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
   
   
# SOURCE: https://github.com/rhayun/python-youtube-api/blob/master/youtubeapi.py
def get_channel_id_from_url(youtube, youtube_url):
    path = parse_url_path(youtube_url)
    if '/channel' in path:
        segments = path.split('/')
        channel_id = segments[len(segments) - 1]
    elif '/user' in path:
        segments = path.split('/')
        username = segments[len(segments) - 1]
        channel_id = get_channel_id_by_name(youtube, username)
    else:
        raise Exception('Url is not a youtube URL: ' + youtube_url)

    return channel_id
    
def parse_url_path(url):
  array = urlparse(url)
  return array[2]
  
def clean_channel_data(channel_id):  
  # remove all data associated with channel to allow clean restart of channel parsing  
  videos.delete_many({ "channelId" : channel_id })
  videoComments.delete_many({ "channelId" : channel_id })
  videoReplies.delete_many({ "channelId" : channel_id })
  playlists.delete_many({ "channelId" : channel_id })
  subscriptions.delete_many({ "channelId" : channel_id })
  channelComments.delete_many({ "channelId" : channel_id })
  channelReplies.delete_many({ "channelId" : channel_id })
  db.playlistItems.delete_many({ "channelId" : channel_id }) 
  advancedVideoStatistics.delete_many({ "channelId" : channel_id })
  captions.delete_many({ "channelId" : channel_id })

  print("cleaned channel data for " + channel_id)
  
def clean_video_data(video_id):  
  # remove all data associated with video to allow clean restart of channel parsing  
  videos.delete_many({ "_id" : video_id })
  videoComments.delete_many({ "videoId" : video_id })
  videoReplies.delete_many({ "videoId" : video_id })
  playlists.delete_many({ "videoId" : video_id })
  subscriptions.delete_many({ "videoId" : video_id })
  channelComments.delete_many({ "videoId" : video_id })
  channelReplies.delete_many({ "videoId" : video_id })
  db.playlistItems.delete_many({ "videoId" : video_id }) 
  advancedVideoStatistics.delete_many({ "videoId" : video_id })
  captions.delete_many({ "videoId" : video_id })

  

def parse_comments_for_video(youtube, video_id, company=None, channel_id=None, playlist_id=None):
   
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
      
  
  #do not raise an exception if comments for a video are disabled
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
    commentThread["playlistId"] = playlist_id
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
    
  #print("get commentThreads for video " + video_id)


def parse_comments_for_channel(youtube, channel_id, company):
  
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
    comment["commentThreadId"] = thread_id
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
        
def parse_playlist_items(youtube, playlist_id, channel_id, company):   
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
    playlistItem["channelId"] = channel_id
    playlistItem["company"] = company
    playlistItem["fetched_at"] = datetime.utcnow()  
    playlistItem["_id"] = playlistItem["id"]
    playlistItem.pop("id", None)

    # parse and save video if not already in database (for the case that a playlist contains videos that belong to another channel)
    if playlistItem["snippet"]["resourceId"]["kind"] == "youtube#video":
      playlistVideoId = playlistItem["snippet"]["resourceId"]["videoId"]
      
      if check_item_exists(videos, "_id", playlistVideoId) == False:
        parse_video(youtube, playlistVideoId, None, None, playlistItem["snippet"]["playlistId"])
	
    try:
      db.playlistItems.save(playlistItem) 
    except pymongo.errors.DuplicateKeyError:
      pass
    
	  
 
  
def parse_playlists(youtube, channel_id, company):
    
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
     
  playlists_ids = []
  for playlist in items:
  
    playlist["channelId"] = channel_id
    playlist["company"] = company
    playlist["fetched_at"] = datetime.utcnow()  
    parse_playlist_items(youtube, playlist["id"], channel_id, company)
    playlist["_id"] = playlist["id"]
    playlist["fetched_at"] = datetime.utcnow() 
   
    db.playlists.save(playlist)
    playlists_ids.append(playlist["_id"]) 
  
  return playlists_ids
  
  
  
def parse_replies_for_video(thread_id, youtube, video_id=None, company=None, channel_id=None, playlist_id=None):

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
    #print("get replies for thread_id" + thread_id)
  
  except HttpError, e:  
      if str(e.resp.status) == "403": #ignore errors that are triggered if the comments for a video are disabled by uploader
        pass
      else:
        raise e
        
  for comment in items:
    comment["commentThreadId"] = thread_id
    comment["videoId"] = video_id
    comment["channelId"] = channel_id
    comment["playlistId"] = playlist_id
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