from pymongo import MongoClient
import os

# Configure database connection and collection names here
client = MongoClient(os.environ.get("MONGODB_URL","mongodb://159.203.156.236/data"), connect=False)
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
subscriptions = db.subscriptions
channel_activities = db.activities
advancedVideoStatistics = db.extendedStatistics
captions = db.captions

# ensures that additional unique indices are set in mongodb to prevent duplicate entries, for collections that do not use the default mongodb unique index "_did"
advancedVideoStatistics.ensure_index("videoId", unique=True)

# Configure maximum numbers of retries here
max_retries_parse_channel = 2
max_retries_parse_video = 2


# Configure scraping web requests time out (in seconds)
requests_timeout = 15


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
 "AIzaSyBySML8O8Z-GeVZNrOWwFuKnDempAt-qYU",
 "AIzaSyCJTCvcyTtJxduOVrbq8jAqTID-YweTSCc",
 "AIzaSyBcZRFsck5pdl3HGZ2eV_l7wB8RuYtWruo"
]



# Specify fields to index in mongodb to speed up lookups
channels.ensure_index("company")

videos.ensure_index("company")
videos.ensure_index("channelId")

videoComments.ensure_index("company")
videoComments.ensure_index("channelId")
videoComments.ensure_index("videoId")

videoReplies.ensure_index("company")
videoReplies.ensure_index("channelId")
videoReplies.ensure_index("videoId")

channelComments.ensure_index("company")
channelComments.ensure_index("channelId")
channelComments.ensure_index("videoId")

channelReplies.ensure_index("company")
channelReplies.ensure_index("channelId")
channelReplies.ensure_index("videoId")

videoCategories.ensure_index("company")
videoCategories.ensure_index("channelId")
videoCategories.ensure_index("videoId")

playlists.ensure_index("company")
playlists.ensure_index("channelId")
playlists.ensure_index("videoId")

subscriptions.ensure_index("company")
subscriptions.ensure_index("channelId")
subscriptions.ensure_index("videoId")

channel_activities.ensure_index("company")
channel_activities.ensure_index("channelId")
channel_activities.ensure_index("videoId")

advancedVideoStatistics.ensure_index("company")
advancedVideoStatistics.ensure_index("channelId")
advancedVideoStatistics.ensure_index("videoId")

captions.ensure_index("company")
captions.ensure_index("channelId")
captions.ensure_index("videoId")