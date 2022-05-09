# Copying ddl file into cassandra-node container
docker cp cql_scripts/ddl.cql cassandra-node:/

# Creating tables
docker exec cassandra-node cqlsh -f ddl.cql