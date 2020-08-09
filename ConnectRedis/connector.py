from kafka import KafkaConsumer
import time

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                            auto_offset_reset='earliest',
                            consumer_timeout_ms=1000)
consumer.subscribe(['sample'])

# while not self.stop_event.is_set():

while True:
    time.sleep(2)
    for message in consumer:
        data = json.loads(list(message)[6])
    
consumer.close()
