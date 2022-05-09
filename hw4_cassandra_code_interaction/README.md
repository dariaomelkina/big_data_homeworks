# HW 4: Cassandra code interaction

## Prerequisites:
```
pip install cassandra-driver
```

## Run example:
Launch one-node Cassandra, create tables and write data into it:
```
bash sh_scripts/launch_cassandra.sh
bash sh_scripts/create_tables.sh
bash sh_scripts/write_to_cassandra.sh
```

Read from Cassandra to answer the questions from task:
```
bash sh_scripts/read_from_cassandra.sh
```

Result example:
![Result example](images/result.png)

Finally shutdown Cassandra:
```
bash sh_scripts/shutdown_cassandra.sh
```


## Tasks details:
1. DDL script for creating tables: [cql_scripts/ddl.cql](cql_scripts/ddl.cql)
1. 

