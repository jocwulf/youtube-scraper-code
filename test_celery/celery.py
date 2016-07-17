from __future__ import absolute_import
from celery import Celery
import os

CELERY_TASK_RESULT_EXPIRES = 30
CELERYD_POOL_RESTARTS = True

BROKER_HEARTBEAT = 10

# configuring best practices for using celery with bigwing hosted rabbitmq (http://www.lshift.net/blog/2015/04/30/making-celery-play-nice-with-rabbitmq-and-bigwig/)
BROKER_TRANSPORT_OPTIONS = {'confirm_publish': True}
BROKER_POOL_LIMIT = 1 # prevent too many connections to the hosted rabbitmq service (some services have restrictions on the number of concurrent connections)

app = Celery('test_celery',

broker=os.environ.get("RABBITMQ_BIGWIG_URL", "redis://localhost:6379/0"),
             backend='rpc://',
             include=['test_celery.tasks'])
