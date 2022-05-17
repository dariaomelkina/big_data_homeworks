USE frauds;
-- # 1 Return all fraud transaction for specific user (uid)
SELECT * FROM sender
WHERE uid = 'C840083671' AND isFraud = 1;

-- # 2 Return 3 biggest transactions for specific user (uid)
SELECT * FROM sender
WHERE uid = 'C840083671'
ORDER BY amount DESC
LIMIT 3;

-- # 3 Return sum of received transaction for specific user (uid) in time range
SELECT sum(amount) FROM receiver
WHERE uid = 'C476402209' AND
transaction_date >= '2022-05-01' AND transaction_date <= '2022-05-15';





