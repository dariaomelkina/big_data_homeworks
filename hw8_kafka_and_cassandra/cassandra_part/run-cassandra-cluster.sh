docker run --name cassandra-node --network kafka-cassandra-network -p 9042:9042 -d cassandra:latest
sleep 70
docker cp cassandra_part/ddl.cql cassandra-node:/
docker exec cassandra-node cqlsh -f ddl.cql