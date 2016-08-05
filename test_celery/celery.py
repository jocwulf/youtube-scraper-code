from __future__ import absolute_import
from celery import Celery
import os

CELERY_TASK_RESULT_EXPIRES = 30
CELERY_RESULT_BACKEND = None

#From https://www.cloudamqp.com/docs/celery.html
BROKER_POOL_LIMIT = 1 # Will decrease connection usage
BROKER_HEARTBEAT = None # We're using TCP keep-alive instead
BROKER_CONNECTION_TIMEOUT = 30 # May require a long timeout due to Linux DNS timeouts etc
CELERY_RESULT_BACKEND = None # AMQP is not recommended as result backend as it creates thousands of queues
CELERY_SEND_EVENTS = False # Will not create celeryev.* queues
CELERY_EVENT_QUEUE_EXPIRES = 60*3 # Will delete all celeryev. queues without consumers after 3 minutes.

# configuring best practices for using celery with bigwing hosted rabbitmq (http://www.lshift.net/blog/2015/04/30/making-celery-play-nice-with-rabbitmq-and-bigwig/)
BROKER_TRANSPORT_OPTIONS = {'confirm_publish': True}
BROKER_POOL_LIMIT = 1 # prevent too many connections to the hosted rabbitmq service (some services have restrictions on the number of concurrent connections)

app = Celery('test_celery',
broker=os.environ.get("CLOUDAMQP_URL", os.environ.get("RABBITMQ_BIGWIG_URL", "redis://localhost:6379/0")),
             include=['test_celery.tasks'])
