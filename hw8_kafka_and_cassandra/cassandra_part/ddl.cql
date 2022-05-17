CREATE  KEYSPACE frauds WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE frauds;

CREATE TABLE sender (uid text, isFraud int, amount float, PRIMARY KEY ((uid), isFraud, amount));
CREATE TABLE receiver (uid text, transaction_date date, amount float, PRIMARY KEY ((uid), transaction_date));