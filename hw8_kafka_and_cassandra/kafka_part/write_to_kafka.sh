docker build -f Dockerfile.write_to_kafka . -t kafka_write:1.0
docker run --network kafka-cassandra-network --rm kafka_write:1.0