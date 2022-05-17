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
        return self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_into_table(self, table, *values):
        if table == "sender":
            query = f"INSERT INTO {table} (uid, isFraud, amount) " \
                    f"VALUES ($${values[0]}$$, {values[1]}, {values[2]})"
        else:  # receiver
            query = f"INSERT INTO {table} (uid, transaction_data, amount) " \
                    f"VALUES ($${values[0]}$$, $${values[1]}$$, {values[2]});"
        self.execute(query)
