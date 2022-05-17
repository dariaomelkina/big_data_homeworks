## HW 8: Kafka and Cassandra communication through code
Create Kafka and Cassandra clusters in one network:
```
bash kafka_part/run-kafka-cluster.sh
bash cassandra_part/run-cassandra-cluster.sh
```

Write messages about transaction frauds into Kafka topic called "frauds" 
(path to file with transactions is hardcoded in [kafka_part/write_to_kafka.py](kafka_part/write_to_kafka.py) file):
```
bash kafka_part/write_to_kafka.sh
```

At the same time run code, which reads from Kafka and writes into Cassandra:
```
bash from_kafka_to_cassandra.sh
```



To shutdown both Kafka and Cassandra clusters:
```
bash shutdown-cluster.sh
```

P.S. Here are commands for manual checks of data in Kafka and Cassandra:
```
docker run -it --rm --network kafka-cassandra-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-console-consumer.sh --bootstrap-server kafka-server:9092 --topic frauds
docker run -it --network kafka-cassandra-network --rm cassandra cqlsh cassandra-node
```