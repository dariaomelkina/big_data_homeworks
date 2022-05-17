CREATE  KEYSPACE frauds WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE frauds;

CREATE TABLE sender_fraud (uid text, isFraud int, amount float, PRIMARY KEY ((uid), isFraud));
CREATE TABLE sender_amount (uid text, amount float, PRIMARY KEY ((uid), amount));
CREATE TABLE receiver (uid text, transaction_date date, amount float, PRIMARY KEY ((uid), transaction_date));