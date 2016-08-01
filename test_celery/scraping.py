#!/usr/bin/python
"""
This file contains information collection procedures that use web scraping (http request simulation and phantomjs based)
It is advised to read the data structure documentation (PDF) prior to diving into the code
"""

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import json
from apiclient.errors import HttpError
from datetime import datetime
import requests
import re 
import pdb
import logging
from bs4 import BeautifulSoup,CData
from HTMLParser import HTMLParser

from test_celery.settings import *



"""
>>>>>>>>>>>>>>>>>>>> Advanced Video Statistics Scraping >>>>>>>>>>>>>>>>>>>>
"""

def parse_advanced_statistics(video_id, company=None, channel_id=None):
  """ 
  Uses the headless browser CasperJS and http requests to retreive advanced video timeseries data while circumventing youtube's mechanisms in place to prevent scraping
  """

  """ 
  Step 1:
  Load video page and extract session token and selected cookies that will be required to authorize the HTTP request that retrieves the statistics data later on
  We need to use a headless browser (CasperJS) to load the website, since the session token will only be injected into the web page source code by Youtube after the whole website has been loaded
  """
  
 
  # Use phantomJS to open website and wait until session token has been inserted into page by Youtube and get page source code 
  
  driver = webdriver.PhantomJS()
  driver.set_window_size(1000, 500)

  timeout = 10 
  driver.get("https://www.youtube.com/watch?v=" + video_id)
  WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.NAME, "session_token"))) 
  video_page_source = driver.page_source

  # Use regular expression to extract session token, might be necessary to adjust expression in the future if youtube changes the struture of the video page source code
  session_token = None
  match = re.search('<input name="session_token" type="hidden" value="(.+)"></form>', video_page_source)

  if match != None:
    session_token = match.group(1)
  else:
    raise Exception("Could not extract session token for video id " + video_id + " from channel " + channel_id + " of company " + company)

  # Extract some selected cookies needed for request later
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
    return False
    

  # Save statistics data to mongodb 
  statistics_data_raw["videoId"] = video_id
  statistics_data_raw["channelId"] = channel_id
  statistics_data_raw["company"] = company
  statistics_data_raw["fetched_at"] = datetime.utcnow()  

  """ clean up extracted data by removing irrelevant fields
  Many ifs are required, since for some videos, parts of the data are missing, e.g. no daily data or no watch-time data
  """
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

  if "subscribers" in statistics_data_raw:

    if "cumulative" in statistics_data_raw["subscribers"]:
      statistics_data_raw["subscribers"]["cumulative"].pop("opt", None)

    if "daily" in statistics_data_raw["subscribers"]:
      statistics_data_raw["subscribers"]["daily"].pop("opt", None)

	
  advancedVideoStatistics.save(statistics_data_raw)

  return True



"""
>>>>>>>>>>>>>>>>>>>> Video Captions Scraping >>>>>>>>>>>>>>>>>>>>
"""

"""
------------------------- ASR Captions -------------------------
"""

def process_asr_caption(youtube, video_id, company=None, channel_id=None):

	""" 
	Download youtube auto generated caption (always in language of the video)
	Returns true if auto generated caption found and saved, returns false if not found
	"""

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
		return False

	""" 
	Step 3:
	Format xml formatted caption and extract plain text from xml formatted caption using regular expression 
	"""
	caption = clean_captions_xml_and_extract_plain_text(r.text)



	""" save in mongodb """
	captions.save(
	{
	"type": "autoGenerated",
	"videoId": video_id,
	"channelId": channel_id,
	"company": company,
	"fetched_at": datetime.utcnow(),
	"xml": caption["xml"],
	"plainText": caption["plainText"],
	"language": video_language
	}
	)

	return True


def get_asr_language(youtube, video_id):
	"""
	For the given video_id, checks if auto generated captions are available for the video.
	If yes it returns the language of the caption otherwise it returns False
	"""

	""" fetch list of captions from API """ 
	try:
	 results = youtube.captions().list(
	   part="snippet",
	   videoId=video_id
	   # no pagination required/offered by API
	   
	 ).execute()
	except HttpError, e:  
	   if str(e.resp.status) == "404": #ignore errors if video is unlisted or blocked
	     return False
	   else:
	     raise e

	""" find auto generated caption and return language if found otherwise return false """
	for caption in results["items"]:
	 if caption["snippet"]["trackKind"] == "ASR":
	   return caption["snippet"]["language"]    

	return False
  


def strip_empty_lines(string):
  return "\n".join([line for line in string.splitlines() if line.strip() != ""])

"""
------------------------ Manual Captions ------------------------
"""

def process_manual_captions(video_id, company=None, channel_id=None):
	""" 
	Download all manually created captions for a specified video_id
	Returns true if at least one caption found and saved, returns false if none found
	"""

	""" retrieve all manually created captions for the video and convert to a python list using BeautifulSoup """
	r = requests.get("https://video.google.com/timedtext?hl=en&type=list&v=" +  video_id)

	xml_soup = BeautifulSoup(r.text, "lxml") 

	captions_list = [{
			"name": track.attrs["name"],  
			"language": track.attrs["lang_code"]  
		} 
		for track in xml_soup.findAll("track")
	]

	if len(captions_list) == 0:
		return False

	for caption_meta_info in captions_list:

		r = requests.get("https://video.google.com/timedtext?hl={0}&lang={0}&name={1}&v={2}".format(caption_meta_info["language"].encode("utf-8"), caption_meta_info["name"].encode("utf-8"), video_id))

		""" Format xml formatted caption and extract plain text from xml formatted caption using regular expression  """
		caption = clean_captions_xml_and_extract_plain_text(r.text)


		""" save to mongodb """
		captions.save(
			{
			"type": "manual",
			"videoId": video_id,
			"channelId": channel_id,
			"company": company,
			"fetched_at": datetime.utcnow(),
			"xml": caption["xml"],
			"plainText": caption["plainText"],
			"language": caption_meta_info["language"],
			"name": caption_meta_info["name"]
			}
		)

	return True


def clean_captions_xml_and_extract_plain_text(xml):
	"""
	Accepts xml caption file retreived from Google, fix text encoding issues and convert caption into plain text
	"""
	caption = {}
	""" we need to unescape the unicode string returned by youtube, since some special characters are returned as html entities e.g. " is returned as &ldquo; """ 
	caption["xml"] = strip_empty_lines(HTMLParser().unescape(HTMLParser().unescape(xml))) # needs to be called twice because of bug in the function

	"""   Extract plain text from xml formatted caption using regular expressions and some back and forth to get all the line breaks right """
	subtitle_xml_stripped = re.sub("<[^>]*>", "\n", caption["xml"].replace("\n"," "))   # first remove all line breaks, then replace all xml tags with line breaks
	caption["plainText"] =  strip_empty_lines(subtitle_xml_stripped)

	return caption
