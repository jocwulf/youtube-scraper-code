from pymongo import MongoClient

# Configure database connection and collection names here
client = MongoClient("mongodb://159.203.156.236/testing", connect=False)
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


# Configure maximum numbers of retries here
max_retries_parse_channel = 2
max_retries_parse_video = 2