docker build -f Dockerfile.from_kafka_to_cassandra . -t from_kafka_to_cassandra:1.0
docker run --network kafka-cassandra-network --rm from_kafka_to_cassandra:1.0