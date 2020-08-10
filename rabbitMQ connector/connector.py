from kafka import KafkaConsumer
import time
import json
from celery import Celery

app = Celery('connector', broker='amqp://localhost')

@app.task
def consumer(topic='sample',port=9092):
    consumer = KafkaConsumer(bootstrap_servers='localhost:%d' %(port),
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=1000)
    consumer.subscribe([topic])

    # while True:
    # time.sleep(2)
    out = []
    j=0
    for message in consumer:
        j+=1
        data = json.loads(list(message)[6])
        out.append(data)
        print(data)
    print('-------------------------------- tedad --------------------')
    print(j)
    consumer.close()
    return out
