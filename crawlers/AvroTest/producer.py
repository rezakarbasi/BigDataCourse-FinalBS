

from confluent_kafka import Producer
import avro.schema
import avro.io
import io
import random
import time 

if __name__ == "__main__":

    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(**conf)

    # Kafka topic
    topic = "myTest"

    # Path to user.avsc avro schema
    schema_path = "AvroTest/user.avsc"
    schema = avro.schema.parse(open(schema_path).read())

    while True:
        time.sleep(2)
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write({"name": "123",
                      "favorite_color": "111",
                      "favorite_number": random.randint(0, 10)}, encoder)
        raw_bytes = bytes_writer.getvalue()
        producer.produce(topic, raw_bytes)
    producer.flush()

    print('done')
