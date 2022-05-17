import json

from CassandraClient import CassandraClient
from kafka import KafkaConsumer

if __name__ == "__main__":
    host = 'cassandra-node'
    port = 9042
    keyspace = 'frauds'
    client = CassandraClient(host, port, keyspace)
    client.connect()

    consumer = KafkaConsumer('frauds',
                             bootstrap_servers='kafka-server:9092',
                             value_deserializer=lambda m: json.loads(m.decode('ascii')))

    for message in consumer:
        value = message.value
        client.insert_into_table("sender_fraud", value['nameOrig'], value['isFraud'], value['amount'])
        client.insert_into_table("sender_amount", value['nameOrig'], value['amount'])
        client.insert_into_table("receiver", value['nameDest'], value['transaction_date'], value['amount'])

    client.close()
