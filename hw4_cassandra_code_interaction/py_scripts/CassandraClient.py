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
        if table == "reviews_by_product":
            query = f"INSERT INTO {table} (product_id, product_title, star_rating, review_headline, review_body) " \
                    f"VALUES ($${values[0]}$$, $${values[1]}$$, {values[2]}, $${values[3]}$$, $${values[4]}$$)"
        elif table == "reviews_by_customer":
            query = f"INSERT INTO {table} (customer_id, product_title, review_headline, review_body) " \
                    f"VALUES ($${values[0]}$$, $${values[1]}$$, $${values[2]}$$, $${values[3]}$$)"
        elif table == "reviews_by_year_product":
            query = f"INSERT INTO {table} (year, product_id, star_rating, product_title) " \
                    f"VALUES ({values[0]}, '{values[1]}', {values[2]}, $${values[3]}$$)"
        else:
            query = f"INSERT INTO {table} (year, customer_id, star_rating) " \
                    f"VALUES ({values[0]}, '{values[1]}', {values[2]});"
        self.execute(query)
