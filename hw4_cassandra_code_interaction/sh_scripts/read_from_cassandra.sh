docker build . -t cassandra_example:1.0
docker run -p 8080:8080 --network my-cassandra-network --rm cassandra_example:1.0