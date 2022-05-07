-- # your_value, N –– is where user chooses value, for example product_id of reviews user wants to select

-- # 1 Return all reviews for specified product_id
SELECT review_body FROM reviews_by_product WHERE product_id="your_value";

-- # 2 Return all reviews for specified product_id with given star_rating
SELECT review_body FROM reviews_by_product WHERE product_id="your_value" and star_rating="your_value";

-- # 3 Return all reviews for specified customer_id
SELECT review_body FROM reviews_by_customer WHERE customer_id="your_value";

-- # 4 Return N most reviewed items (by # of reviews) for a given period of time
SELECT customer_id FROM product_reviews WHERE product_id="your_value" and count(customer_id)>"N";

-- # 5 Return N most productive customers (by # of reviews written for verified purchases) for a given period
SELECT count(review_body) FROM reviews_by_customer WHERE customer_id="your_value" and star_rating>3;

-- # 6 Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period


-- # 7 Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period





