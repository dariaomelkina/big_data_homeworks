import csv
import datetime
import json
import random
import time

from kafka import KafkaProducer

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='kafka-server:9092',
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))

    with open('PS_20174392719_1491204439457_log.csv', mode='r') as file:
        csvFile = csv.DictReader(file)
        for lines in csvFile:

            producer.send('frauds', {'nameOrig': lines['nameOrig'],
                                     'nameDest': lines['nameDest'],
                                     'isFraud': lines['isFraud'],
                                     'amount': lines['amount'],
                                     'transaction_date': (datetime.datetime.today() -
                                                          datetime.timedelta(
                                                              days=random.randint(0, 10))
                                                          ).strftime("%Y-%m-%d"),
                                     })
            time.sleep(0.04)

    producer.flush()
