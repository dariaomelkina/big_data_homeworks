-- # your_value, N –– is where user chooses value, for example product_id of reviews user wants to select

-- # 1
SELECT review_body FROM reviews_by_product WHERE product_id="your_value";  

-- # 2
SELECT review_body FROM reviews_by_product WHERE product_id="your_value" and star_rating="your_value";

-- # 3
SELECT review_body FROM reviews_by_customer WHERE customer_id="your_value"; 

-- # 4
SELECT customer_id FROM product_reviews WHERE product_id="your_value" and count(customer_id)>"N"; 

-- # 5
SELECT count(review_body) FROM reviews_by_customer WHERE customer_id="your_value" and star_rating>3; 
