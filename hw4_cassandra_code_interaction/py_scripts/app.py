import pandas as pd
from CassandraClient import CassandraClient
from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=['POST'])
def facade():
    host = 'cassandra-node'
    port = 9042
    keyspace = 'amazon_keyspace'

    client = CassandraClient(host, port, keyspace)
    client.connect()

    print("1. Return all reviews for specified product_id")
    query = f"SELECT * FROM reviews_by_product WHERE product_id = '0439358078'"
    rows = client.execute(query)
    for row in rows:
        print(row)

    print("2. Return all reviews for specified product_id with given star_rating")
    query = f"SELECT * FROM reviews_by_product WHERE product_id = '0439358078' and star_rating = 5"
    rows = client.execute(query)
    for row in rows:
        print(row)

    print("3. Return all reviews for specified customer_id")
    query = f"SELECT * FROM reviews_by_customer WHERE customer_id = '52534781'"
    rows = client.execute(query)
    for row in rows:
        print(row)

    print("4. Return N most reviewed items (by # of reviews) for a given period of time")
    query = f"SELECT count(star_rating), product_id, product_title " \
            f"FROM reviews_by_year_product WHERE year = 2005 GROUP BY product_id"
    rows = client.execute(query)
    df = pd.DataFrame(rows, columns=['system_count_star_rating', 'product_id', 'product_title'])
    print(df.sort_values(by='system_count_star_rating', ascending=False).iloc[0:10])

    print(
        "5. Return N most productive customers (by # of reviews written for verified purchases) for a given period")
    query = f"SELECT count(star_rating), customer_id " \
            f"FROM reviews_by_year_customer WHERE year = 2005 GROUP BY customer_id"
    rows = client.execute(query)
    df = pd.DataFrame(rows, columns=['system_count_star_rating', 'customer_id'])
    print(df.sort_values(by='system_count_star_rating', ascending=False).iloc[0:10])

    print("6. Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period")
    query = f"SELECT customer_id " \
            f"FROM customer_haters_backers WHERE year = 2005 and star_rating < 3 ORDER BY star_rating LIMIT 10"
    rows = client.execute(query)
    for row in rows:
        print(row)

    print("7. Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period")
    query = f"SELECT customer_id " \
            f"FROM customer_haters_backers WHERE year = 2005 and star_rating > 3 ORDER BY star_rating DESC LIMIT 10"
    rows = client.execute(query)
    for row in rows:
        print(row)

    client.close()
    print(request.get_json())
    return "yay"


if __name__ == "__main__":
    app.run(debug=True)
