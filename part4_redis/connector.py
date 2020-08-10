from kafka import KafkaConsumer
import time
import json
from celery import Celery
from redis_handler import RedisHandler, add_toRedis

app = Celery('connector', broker='amqp://localhost')
redis_client = RedisHandler(exp_duration=3600*24*7)
key_times = ['year', 'month', 'day', 'hour']
value = 1

@app.task
def consumer(topic='messages',port=9092):
    print('here')
    consumer = KafkaConsumer(
    bootstrap_servers='localhost:%d' %(port),
    auto_offset_reset='earliest',
    consumer_timeout_ms=1000
    )
    consumer.subscribe([topic])
    for message in consumer:
        data = json.loads(list(message)[6])
        add_toRedis(data, redis_client, value, key_times=key_times)
    consumer.close()
