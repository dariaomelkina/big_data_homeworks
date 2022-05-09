from CassandraClient import CassandraClient

if __name__ == '__main__':
    host = 'localhost'
    port = 9042
    keyspace = 'example_keyspace'
    table = 'my_courses'

    # TODO: insert data from amazon dataset

    records_to_insert = [
        ('Big Data Processing', 2022, False),
        ('Soft Skills', 2022, True),
        ('Systems Design', 2021, True)
    ]

    client = CassandraClient(host, port, keyspace)
    client.connect()
    for record in records_to_insert:
        client.insert_course_record(table, record[0], record[1], record[2])
    client.close()
