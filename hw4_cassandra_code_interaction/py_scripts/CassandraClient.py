class CassandraClient:
    """
    Class for communicating with Cassandra.
    """
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

    def insert_course_record(self, table, name, year, conducted):
        query = "INSERT INTO %s (name, year, conducted) VALUES ('%s', %d, %r)" % (table, name, year, conducted)
        self.execute(query)
