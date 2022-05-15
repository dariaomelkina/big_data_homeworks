docker build . -t kafka_example:1.0
docker run --network kafka-network --rm kafka_example:1.0