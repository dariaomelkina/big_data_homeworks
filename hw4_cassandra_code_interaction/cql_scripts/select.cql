use amazon_keyspace;
-- # 1 Return all reviews for specified product_id
SELECT *
FROM reviews_by_product
WHERE product_id = '0439358078';

-- # 2 Return all reviews for specified product_id with given star_rating
SELECT *
FROM reviews_by_product
WHERE product_id = '0439358078'
  and star_rating = 5;

-- # 3 Return all reviews for specified customer_id
SELECT *
FROM reviews_by_customer
WHERE customer_id = '52534781';

-- # 4 Return N most reviewed items (by # of reviews) for a given period of time
SELECT count(star_rating), product_id, product_title
FROM reviews_by_year_product
WHERE year = 2005
GROUP BY product_id;
-- afterwards result of this query will be used in python code to find answer to the question

-- # 5 Return N most productive customers (by # of reviews written for verified purchases) for a given period
SELECT count(star_rating), customer_id
FROM reviews_by_year_customer
WHERE year = 2005
GROUP BY customer_id;
-- afterwards result of this query will be used in python code to find answer to the question

-- # 6 Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period
SELECT customer_id
FROM customer_haters_backers
WHERE year = 2005
  and star_rating < 3
ORDER BY star_rating
LIMIT 10;

-- # 7 Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period
SELECT customer_id
FROM customer_haters_backers
WHERE year = 2005
  and star_rating > 3
ORDER BY star_rating DESC
LIMIT 10;

-- For manual testing:
-- docker run -it --network my-cassandra-network --rm cassandra cqlsh cassandra-node




