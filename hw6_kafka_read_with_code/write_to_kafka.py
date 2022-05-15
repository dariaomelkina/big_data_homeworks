import csv
import datetime
import json
import time

from kafka import KafkaProducer

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='kafka-server:9092',
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))

    with open('twcs.csv', mode='r') as file:
        csvFile = csv.DictReader(file)
        for lines in csvFile:
            producer.send('tweets', {'created_at': str(datetime.datetime.now()),
                                     'text': lines['text']})
            time.sleep(0.07)

    producer.flush()
