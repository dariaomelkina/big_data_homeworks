DROP TABLE IF EXISTS reviews_by_product;
DROP TABLE IF EXISTS reviews_by_customer;

CREATE TABLE reviews_by_product (
   product_id UUID,
   customer_id UUID,
   star_rating int,
   review_body text,
   PRIMARY KEY( (product_id), star_rating, customer_id)
   );

CREATE TABLE reviews_by_customer (
   customer_id UUID,
   product_id UUID,
   star_rating int,
   review_body text,
   PRIMARY KEY( (customer_id), product_id, star_rating, review_body)
   );




-- CREATE TABLE reviews_by_product (
--    product_id UUID,
--    customer_id UUID,
--    star_rating int,
--    review_body text,
--    PRIMARY KEY( (product_id), star_rating, customer_id)
--    );
--
-- CREATE TABLE reviews_by_customer (
--    customer_id UUID,
--    product_id UUID,
--    star_rating int,
--    review_body text,
--    PRIMARY KEY( (customer_id), product_id, star_rating, review_body)
--    );