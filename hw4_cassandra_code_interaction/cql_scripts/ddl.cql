CREATE  KEYSPACE amazon_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE amazon_keyspace;

DROP TABLE IF EXISTS reviews_by_product;
DROP TABLE IF EXISTS reviews_by_customer;
DROP TABLE IF EXISTS reviews_by_year;

CREATE TABLE reviews_by_product
(
    product_id    UUID,
    product_title text,
    star_rating   int,
    review_body   text,
    PRIMARY KEY ( (product_id), star_rating)
);

CREATE TABLE reviews_by_customer
(
    customer_id   UUID,
    product_title text,
    review_body   text,
    PRIMARY KEY ( (customer_id))
);


CREATE TABLE reviews_by_year
(
    year          int,
    customer_id   UUID,
    product_id    UUID,
    star_rating   int,
    product_title text,
    PRIMARY KEY ( (year), product_id, star_rating, customer_id)
);