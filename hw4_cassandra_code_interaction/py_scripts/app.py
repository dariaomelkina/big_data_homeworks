import pandas as pd
from CassandraClient import CassandraClient
from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=['POST'])
def main():
    post_request = request.get_json()
    amazon_answer = dict()
    counter = 0

    host = 'cassandra-node'
    port = 9042
    keyspace = 'amazon_keyspace'

    client = CassandraClient(host, port, keyspace)
    client.connect()

    if post_request['question'] == 1:
        amazon_answer["answers_for_question"] = "1. Return all reviews for specified product_id"
        query = f"SELECT * FROM reviews_by_product WHERE product_id = '{post_request['product_id']}'"
        rows = client.execute(query)
        for row in rows:
            amazon_answer[str(counter)] = row
            counter += 1

    elif post_request['question'] == 2:
        amazon_answer["answers_for_question"] = "2. Return all reviews for specified product_id with given star_rating"
        query = f"SELECT * FROM reviews_by_product " \
                f"WHERE product_id = '{post_request['product_id']}' and star_rating = {post_request['star_rating']}"
        rows = client.execute(query)
        for row in rows:
            amazon_answer[str(counter)] = row
            counter += 1

    elif post_request['question'] == 3:
        amazon_answer["answers_for_question"] = "3. Return all reviews for specified customer_id"
        query = f"SELECT * FROM reviews_by_customer WHERE customer_id = '{post_request['customer_id']}'"
        rows = client.execute(query)
        for row in rows:
            amazon_answer[str(counter)] = row
            counter += 1

    elif post_request['question'] == 4:
        amazon_answer[
            "answers_for_question"] = "4. Return N most reviewed items (by # of reviews) for a given period of time"
        query = f"SELECT count(star_rating), product_id, product_title " \
                f"FROM reviews_by_year_product WHERE year = {post_request['year']} GROUP BY product_id"
        rows = client.execute(query)
        df = pd.DataFrame(rows, columns=['system_count_star_rating', 'product_id', 'product_title'])
        df = df.sort_values(by='system_count_star_rating', ascending=False)

        for idx, row in df.iterrows():
            amazon_answer[str(counter)] = {'# of reviews': row['system_count_star_rating'],
                                           'product_id': row['product_id'],
                                           'product_title': row['product_title']}
            counter += 1
            if counter == post_request['N']:
                break

    elif post_request['question'] == 5:
        amazon_answer["answers_for_question"] = "5. Return N most productive customers " \
                                                "(by # of reviews written for verified purchases) for a given period"
        query = f"SELECT count(star_rating), customer_id " \
                f"FROM reviews_by_year_customer WHERE year = {post_request['year']} GROUP BY customer_id"
        rows = client.execute(query)
        df = pd.DataFrame(rows, columns=['system_count_star_rating', 'customer_id'])
        df = df.sort_values(by='system_count_star_rating', ascending=False)

        for idx, row in df.iterrows():
            amazon_answer[str(counter)] = {'# of reviews': row['system_count_star_rating'],
                                           'customer_id': row['customer_id']}
            counter += 1
            if counter == post_request['N']:
                break

    elif post_request['question'] == 6:
        amazon_answer["answers_for_question"] = "6. Return N most productive “haters” " \
                                                "(by # of 1- or 2-star reviews) for a given period"
        query = f"SELECT customer_id " \
                f"FROM customer_haters_backers WHERE year = {post_request['year']} and star_rating < 3 " \
                f"ORDER BY star_rating LIMIT {post_request['N']}"
        rows = client.execute(query)
        for row in rows:
            amazon_answer[str(counter)] = row
            counter += 1

    elif post_request['question'] == 7:
        amazon_answer["answers_for_question"] = "7. Return N most productive “backers” " \
                                                "(by # of 4- or 5-star reviews) for a given period"
        query = f"SELECT customer_id " \
                f"FROM customer_haters_backers WHERE year = {post_request['year']} and star_rating > 3 " \
                f"ORDER BY star_rating DESC LIMIT {post_request['N']}"
        rows = client.execute(query)
        for row in rows:
            amazon_answer[str(counter)] = row
            counter += 1

    client.close()
    return amazon_answer


if __name__ == "__main__":
    app.run(debug=True)
