docker build . -t cassandra_example:1.0
docker run --network my-cassandra-network --rm cassandra_example:1.0