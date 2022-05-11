docker build . -t cassandra_example:1.0
docker run -p 5000:5000 --network my-cassandra-network --rm cassandra_example:1.0