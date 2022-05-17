docker run --name cassandra-node --network kafka-cassandra-network -p 9042:9042 -d cassandra:latest
docker cp ddl.cql cassandra-node1:/
docker exec cassandra-node1 cqlsh -f ddl.cql