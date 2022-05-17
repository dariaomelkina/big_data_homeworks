docker build -f Dockerfile.app . -t cassandra_api:1.0
docker run -p 8080:8080 --network kafka-cassandra-network --rm cassandra_api:1.0