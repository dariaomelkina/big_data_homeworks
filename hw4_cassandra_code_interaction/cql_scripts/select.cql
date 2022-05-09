-- # 1 Return all reviews for specified product_id
SELECT
FROM reviews_by_product
WHERE product_id = 'specified product_id';

-- # 2 Return all reviews for specified product_id with given star_rating
SELECT *
FROM reviews_by_product
WHERE product_id = 'specified product_id'
  and star_rating = 'given star_rating';

-- # 3 Return all reviews for specified customer_id
SELECT *
FROM reviews_by_customer
WHERE customer_id = 'specified customer_id';

-- # 4 Return N most reviewed items (by # of reviews) for a given period of time
SELECT *
FROM reviews_by_year
WHERE year = 'given period of time'
ORDER BY count(product_id) DESC
LIMIT 'N';

-- # 5 Return N most productive customers (by # of reviews written for verified purchases) for a given period
SELECT customer_id
FROM reviews_by_year
WHERE year = 'given period of time'
ORDER BY count(customer_id) DESC
LIMIT 'N';

-- # 6 Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period
SELECT customer_id
FROM reviews_by_year
WHERE year = 'given period of time' where star_rating<3
ORDER BY star_rating ASC
LIMIT 'N';

-- # 7 Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period
SELECT customer_id
FROM reviews_by_year
WHERE year = 'given period of time' where star_rating>3
ORDER BY star_rating
DESC
LIMIT 'N';






