CREATE KEYSPACE amazon_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE amazon_keyspace;

DROP TABLE IF EXISTS reviews_by_product;
DROP TABLE IF EXISTS reviews_by_customer;
DROP TABLE IF EXISTS reviews_by_year_product;
DROP TABLE IF EXISTS reviews_by_year_customer;
DROP TABLE IF EXISTS customer_haters_backers;

-- tasks 1, 2
CREATE TABLE reviews_by_product
(
    product_id      ascii,
    product_title   text,
    star_rating     int,
    review_headline text,
    review_body     text,
    PRIMARY KEY ( (product_id), star_rating)
);
-- INSERT INTO reviews_by_product (product_id, product_title, star_rating, review_headline, review_body)
-- VALUES ('1', $$test1$$, 3, $$test headline$$, $$test body$$);

-- task 3
CREATE TABLE reviews_by_customer
(
    customer_id     ascii,
    product_title   text,
    review_headline text,
    review_body     text,
    PRIMARY KEY ( (customer_id))
);
-- INSERT INTO reviews_by_customer (customer_id, product_title, review_headline, review_body)
-- VALUES ('1234', $$test title$$, $$test headline$$, $$test body$$);

-- task 4
CREATE TABLE reviews_by_year_product
(
    year          int,
    product_id    ascii,
    star_rating   int,
    product_title text,
    PRIMARY KEY ( (year), product_id, star_rating)
);
-- INSERT INTO reviews_by_year_product (year, product_id, star_rating, product_title)
-- VALUES (2005, '1', 4, $$test title$$);

-- task 5
CREATE TABLE reviews_by_year_customer
(
    year          int,
    customer_id   ascii,
    star_rating   int,
    PRIMARY KEY ( (year), customer_id, star_rating)
);
-- INSERT INTO reviews_by_year_customer (year, customer_id, star_rating)
-- VALUES (2005, '1234', 4);

CREATE TABLE customer_haters_backers
(
    year          int,
    customer_id   ascii,
    star_rating   int,
    PRIMARY KEY ( (year), star_rating, customer_id)
);
-- INSERT INTO customer_haters_backers (year, customer_id, star_rating)
-- VALUES (2005, '1234', 4);

