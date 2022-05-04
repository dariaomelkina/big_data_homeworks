docker exec cassandra-node1 cqlsh -e "USE hw2_omelkina; DESCRIBE TABLES;"
docker exec cassandra-node1 cqlsh -e "USE hw2_omelkina; SELECT * FROM favorite_songs;"
docker exec cassandra-node1 cqlsh -e "USE hw2_omelkina; SELECT * FROM favorite_movies;"