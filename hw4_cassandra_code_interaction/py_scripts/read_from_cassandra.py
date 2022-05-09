from CassandraClient import CassandraClient

if __name__ == '__main__':
    host = 'cassandra-node'
    port = 9042
    keyspace = 'example_keyspace'
    table = 'my_courses'

    client = CassandraClient(host, port, keyspace)
    client.connect()
    client.read_from_table(table)
    client.close()
