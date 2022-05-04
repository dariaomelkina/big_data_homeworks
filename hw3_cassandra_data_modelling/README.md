# HW3: Cassandra data modelling

**Task 1 and 2:** Results are in ddl.cql and select.sql.

**Task 3:** I create two tables, with different partition keys, so in one case I will specify product_id and in th other case –– customer_id. For the first table, in order to specify star_rating, too –– I made star_rating a clustering key. I aslo made customer_id a clustering key, too, because we will need to count them for 4th question. For the second table, I needed to count number of rewievs, specify product_id and check if star_rating is >= 4. For this i made reviews, product_id and star_rating clustering keys, while customer_id remained a partition key. Even though I am not sure if counting works in Cassandra the way I think.