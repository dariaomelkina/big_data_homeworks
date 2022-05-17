import json

from cassandra_part import CassandraClient
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
        client.insert_into_table("sender", message['nameOrig'], message['isFraud'], message['amount'])
        client.insert_into_table("receiver", message['nameDest'], message['transaction_date'], message['amount'])

    client.close()
