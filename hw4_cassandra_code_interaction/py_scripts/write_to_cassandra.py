import pandas as pd
from CassandraClient import CassandraClient

if __name__ == '__main__':
    host = 'localhost'
    port = 9042
    keyspace = 'amazon_keyspace'

    # Reading only a part of the amazon data (900 rows)
    amazon_df = pd.read_csv('amazon_reviews_us_Books_v1_02.tsv', sep='\t', nrows=900)

    client = CassandraClient(host, port, keyspace)
    client.connect()
    for idx, row in amazon_df.iterrows():
        client.insert_into_table("reviews_by_product", row['product_id'], row['product_title'], row['star_rating'],
                                 row['review_headline'], row['review_body'])

        client.insert_into_table("reviews_by_customer", row['customer_id'], row['product_title'],
                                 row['review_headline'], row['review_body'])

        client.insert_into_table("reviews_by_year_product", row['review_date'].split('-')[0], row['product_id'],
                                 row['star_rating'], row['product_title'])

        client.insert_into_table("reviews_by_year_customer", row['review_date'].split('-')[0], row['customer_id'],
                                 row['star_rating'])

        client.insert_into_table("customer_haters_backers", row['review_date'].split('-')[0], row['customer_id'],
                                 row['star_rating'])

    client.close()
