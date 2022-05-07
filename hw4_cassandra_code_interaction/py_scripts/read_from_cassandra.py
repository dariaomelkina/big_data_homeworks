# Based on tutorial from notion
class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def read_from_table(self, table_name):
        query = "SELECT * FROM %s" % table_name
        rows = self.session.execute(query)
        for row in rows:
            print(row)


if __name__ == '__main__':
    host = 'cassandra-node'
    port = 9042
    keyspace = 'example_keyspace'
    table = 'my_courses'

    client = CassandraClient(host, port, keyspace)
    client.connect()
    client.read_from_table(table)
    client.close()
