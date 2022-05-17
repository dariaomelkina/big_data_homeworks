from CassandraClient import CassandraClient
from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=['POST'])
def main():
    post_request = request.get_json()
    fraud_answer = dict()

    host = 'cassandra-node'
    port = 9042
    keyspace = 'frauds'

    client = CassandraClient(host, port, keyspace)
    client.connect()

    counter = 0
    if post_request['question'] == 1:
        fraud_answer["answers_for_question"] = "1. Return all fraud transaction for specific user (uid)"
        query = f"SELECT * FROM sender_fraud WHERE uid = '{post_request['uid']}' AND isFraud = 1;"

        rows = client.execute(query)
        for row in rows:
            fraud_answer[str(counter)] = row
            counter += 1

    elif post_request['question'] == 2:
        fraud_answer["answers_for_question"] = "2. Return 3 biggest transactions for specific user (uid)"
        query = f"SELECT * FROM sender_amount WHERE uid = '{post_request['uid']}' ORDER BY amount DESC LIMIT 3;"

        rows = client.execute(query)
        for row in rows:
            fraud_answer[str(counter)] = row
            counter += 1

    elif post_request['question'] == 3:
        fraud_answer["answers_for_question"] = "3. Return sum of " \
                                               "received transaction for specific user (uid) in time range"
        query = f"SELECT sum(amount) FROM receiver WHERE uid = '{post_request['uid']}' " \
                f"AND transaction_date >= '{post_request['start_date']}' " \
                f"AND transaction_date <= '{post_request['end_date']}';"

        rows = client.execute(query)
        fraud_answer["answer"] = rows[0]

    client.close()
    return fraud_answer


if __name__ == "__main__":
    app.run(debug=True)
