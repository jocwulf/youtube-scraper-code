from pymongo import MongoClient
import os

# Configure database connection and collection names here
client = MongoClient(os.environ.get("MONGODB_URL","mongodb://159.203.156.236/debug"), connect=False)
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
channel_activities = db.activities
advancedVideoStatistics = db.extendedStatistics
captions = db.captions

# ensures that additional unique indices are set in mongodb to prevent duplicate entries, for collections that do not use the default mongodb unique index "_did"
advancedVideoStatistics.ensure_index("videoId", unique=True)
captions


# Configure maximum numbers of retries here
max_retries_parse_channel = 2
max_retries_parse_video = 2


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