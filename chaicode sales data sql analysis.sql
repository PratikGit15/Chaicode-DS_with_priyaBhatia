drop database if exists chaicode;
create database if not exists chaicode;
use chaicode;


-- table creation 

drop table if exists inventory, customers, sales_transactions;
CREATE TABLE IF NOT EXISTS customers (
	`CustomerID` 	INT 				PRIMARY KEY		auto_increment, 
	`Age` 			DECIMAL(38, 0) 		NOT NULL, 
	`Gender` 		VARCHAR(10) 		NOT NULL		CHECK (GENDER IN ('Male', 'Female', 'Other')), 
	`Location` 		VARCHAR(5), 
	`JoinDate` 		VARCHAR(19)			NOT NULL
);
CREATE TABLE IF NOT EXISTS inventory (
	`ProductID` 	INT 				PRIMARY KEY 	auto_increment,
	`ProductName` 	VARCHAR(11) 		NOT NULL, 
	`Category`		VARCHAR(15) 		NOT NULL, 
	`StockLevel` 	INT 				NOT NULL, 
	`PerItemPrice` 		DECIMAL(38, 2) 		NOT NULL
);
CREATE TABLE IF NOT EXISTS sales_transactions (
	`TransactionID`	 	INT 				NOT NULL, 
	`CustomerID` 		INT 				NOT NULL, 
	`ProductID` 		INT 				NOT NULL, 
	`QuantityPurchased` DECIMAL(38, 0) 		NOT NULL, 
	`TransactionDate` 	VARCHAR(19),		
	`PerItemPrice` 			DECIMAL(38, 2) 		NOT NULL,
    FOREIGN KEY (ProductID) 	REFERENCES inventory(ProductID),
	FOREIGN KEY (CustomerID) 	REFERENCES customers(CustomerID)
);

-- loading data

LOAD DATA INFILE 'D:/customer_profiles.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'D:/product_inventory.csv'
INTO TABLE inventory
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'D:/sales_transaction.csv'
INTO TABLE sales_transactions
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- disables safe update mode in MySQL, allowing you to run UPDATE or DELETE statements without a WHERE clause or a key-based filter. 
-- It gives you more flexibility—but also more risk if you're not careful.
SET SQL_SAFE_UPDATES = 0;

-- converting varchar date to timestamp date.
UPDATE customers
SET JoinDate = DATE_FORMAT(STR_TO_DATE(JoinDate, '%d/%m/%y'), '%Y-%m-%d %H:%i:%s');
ALTER TABLE customers
MODIFY COLUMN JoinDate TIMESTAMP;

UPDATE sales_transactions
SET TransactionDate = DATE_FORMAT(STR_TO_DATE(TransactionDate, '%d/%m/%y'), '%Y-%m-%d %H:%i:%s');
ALTER TABLE sales_transactions
MODIFY COLUMN TransactionDate TIMESTAMP;

SET SQL_SAFE_UPDATES = 1;

-- let see if any duplicates records in any table.

SELECT ProductID, ProductName, Category, Stocklevel, PerItemPrice, COUNT(*) AS duplicates_count
FROM inventory
GROUP BY ProductID, ProductName, Category, Stocklevel, PerItemPrice
HAVING COUNT(*) > 1;

SELECT customerid, age, gender, location, joindate, COUNT(*) AS duplicates_count
FROM customers
GROUP BY customerid, age, gender, location, joindate
HAVING COUNT(*) > 1;

SELECT transactionID, CustomerID, ProductID, QuantityPurchased, TransactionDate, peritemprice, COUNT(*) AS duplicates_count
FROM sales_transactions
GROUP BY transactionID, CustomerID, ProductID, QuantityPurchased, TransactionDate, peritemprice
HAVING COUNT(*) > 1;

-- create new table with unique rows
CREATE TEMPORARY TABLE sales_transactions_unique AS
SELECT DISTINCT transactionID, CustomerID, ProductID, QuantityPurchased, TransactionDate, PerItemPrice
FROM sales_transactions;
TRUNCATE TABLE sales_transactions;
INSERT INTO sales_transactions
SELECT * FROM sales_transactions_unique;
DROP TABLE sales_transactions_unique;

-- lets see if there are any duplicates in transaction_id in sales_data so that we can make it primry key.
SELECT transactionid, COUNT(*) AS duplicate_count
	FROM sales_transactions
GROUP BY transactionid
HAVING COUNT(*) > 1;

-- setting transaction id as primary key.
ALTER TABLE sales_transactions 
ADD PRIMARY KEY (transactionID);

-- checking if any null value sin customers
SELECT * FROM customers
WHERE Age IS NULL
   OR Gender IS NULL OR TRIM(Gender) = ''
   OR Location IS NULL OR TRIM(Location) = ''
   OR JoinDate IS NULL;

-- updatting null location with most occuring value
SET SQL_SAFE_UPDATES = 0;
UPDATE customers
SET Location = (
    SELECT most_common_location
    FROM (
        SELECT Location AS most_common_location
        FROM customers
        WHERE Location IS NOT NULL AND TRIM(Location) <> ''
        GROUP BY Location
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) AS derived
)
WHERE Location IS NULL OR TRIM(Location) = '';

SELECT * FROM inventory
WHERE ProductID IS NULL OR TRIM(ProductID) = ''
   OR ProductName IS NULL OR TRIM(ProductName) = ''
   OR Category IS NULL OR TRIM(Category) = ''
   OR StockLevel IS NULL
   OR PerItemPrice IS NULL;

SELECT *
FROM sales_transactions
WHERE TransactionID IS NULL
   OR CustomerID IS NULL
   OR ProductID IS NULL
   OR QuantityPurchased IS NULL
   OR TransactionDate IS NULL
   OR PerItemPrice IS NULL;

SET SQL_SAFE_UPDATES = 1;

-- ANALYSIS
-- lets see the trends. sales  as per quater.
SELECT  YEAR(TransactionDate) AS sales_year,  QUARTER(TransactionDate) AS sales_quarter,
  SUM(quantitypurchased) AS total_quantity_sold
FROM
  sales_transactions
GROUP BY
  YEAR(TransactionDate),
  QUARTER(TransactionDate)
ORDER BY
  sales_year DESC,
  sales_quarter DESC;
-- shows that recent quater sales have decreased drastically. Only 1609 quantities sold.

-- sales m-o-m quatities sold.
SELECT
  DATE_FORMAT(TransactionDate, '%Y-%m') AS Month,
  SUM(QuantityPurchased) AS TotalQuantityPurchasedPerMonth
FROM sales_transactions
GROUP BY DATE_FORMAT(TransactionDate, '%Y-%m')
ORDER BY Month DESC;

SELECT  YEAR(joindate) AS join_year,  QUARTER(joindate) AS join_quarter,
  count(customerid) AS total_customer_joined
FROM
  customers
GROUP BY
  YEAR(joindate),
  QUARTER(joindate)
ORDER BY
  join_year DESC,
  join_quarter DESC;
  -- we can see only 1 customer joined last quater.
  
-- Most purchased product where quantity purchased tells how much was each product sold.
select inventory.productid, category, productname, temp.total_quantity_purchased, stocklevel, peritemprice from inventory
JOIN
(SELECT productID, 
       SUM(QuantityPurchased) AS total_quantity_purchased
FROM sales_transactions
GROUP BY productID
ORDER BY total_quantity_purchased DESC
LIMIT 1) as temp
ON temp.productid = inventory.productid;
-- most bought sold item was product_182

-- average customer age.
SELECT AVG(Age) AS AverageCustomerAge FROM customers;
-- our average customer age is 43years

select inventory.productid, category, productname, temp.total_quantity_purchased, stocklevel, peritemprice from inventory
JOIN
(SELECT productID, 
       SUM(QuantityPurchased) AS total_quantity_purchased
FROM sales_transactions
GROUP BY productID
ORDER BY total_quantity_purchased
LIMIT 1) as temp
ON temp.productid = inventory.productid;
-- so product_142 is least bought product which is only purchased 27 times but its inventory is 185 which can be decreased..

-- category-level performance
SELECT
  i.Category,
  SUM(s.QuantityPurchased) AS TotalUnitsSold,
  SUM(s.QuantityPurchased * s.peritemprice) AS Revenue
FROM sales_transactions s
JOIN inventory i ON s.ProductID = i.ProductID
GROUP BY i.Category
ORDER BY Revenue DESC;
-- so most bought category is home appliances, we can think of bringing new items in home appliances.
-- and clothing has given the maximum revenue.

-- customer segmentation based on given labels in question: >30 is high; 10-30 is mid.
SELECT
  CustomerID,
  SUM(QuantityPurchased) AS TotalQuantity,
  SUM(QuantityPurchased * peritemprice) AS TotalSpending,
  CASE
    WHEN SUM(QuantityPurchased) = 0 THEN 'no orders'
    WHEN SUM(QuantityPurchased) BETWEEN 1 AND 10 THEN 'low value custome'
    WHEN SUM(QuantityPurchased) BETWEEN 11 AND 30 THEN 'mid value customer'
    WHEN SUM(QuantityPurchased) > 30 THEN 'high value customer'
  END AS customer_segmentation
FROM sales_transactions
GROUP BY CustomerID
ORDER BY TotalSpending DESC;
-- we got the customerid of most items bought customers which must be retained by giving discounts.

-- Customers who’ve bought the same product more than once
SELECT
  CustomerID,
  ProductID,
  COUNT(*) AS PurchaseCount
FROM sales_transactions
GROUP BY CustomerID, ProductID
HAVING COUNT(*) > 1
ORDER BY PurchaseCount DESC;
-- these customers can be given marketing messages with discounts for bulk buying.

-- Identifying Loyal Customers (Based on Time Between Purchases)
SELECT
  CustomerID,
  MIN(TransactionDate) AS FirstPurchase,
  MAX(TransactionDate) AS LastPurchase,
  DATEDIFF(MAX(TransactionDate), MIN(TransactionDate)) AS LoyaltyDurationDays,
  COUNT(*) AS TotalPurchases
FROM sales_transactions
GROUP BY CustomerID
ORDER BY LoyaltyDurationDays DESC, totalpurchases desc;
-- to loyal customers we can give a anytime discount of say 2% on any purchase to retain them for long time like amazon credit card.

-- number of purchases a customer made and how often
SELECT
  CustomerID,
  COUNT(DISTINCT DATE(TransactionDate)) AS PurchaseDays,
  COUNT(*) AS TotalTransactions,
  ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT DATE(TransactionDate)), 2) AS AvgPurchasesPerDay
FROM sales_transactions
GROUP BY CustomerID
ORDER BY AvgPurchasesPerDay DESC;
-- average buying of each customer.

-- Most purchased and most revenue-generating products
SELECT
  i.ProductName,
  i.Category,
  SUM(s.QuantityPurchased) AS TotalUnitsSold,
  SUM(s.QuantityPurchased * s.peritemprice) AS TotalRevenue
FROM sales_transactions s
JOIN inventory i ON s.ProductID = i.ProductID
GROUP BY i.ProductName, i.Category
ORDER BY TotalRevenue DESC;
-- product_51 generated maximum revenue for us followed by product_17.

-- to get average age of top50 customers.
WITH customer_totals AS (
  SELECT CustomerID,
    SUM(QuantityPurchased) AS TotalQuantity
  FROM sales_transactions
  GROUP BY CustomerID
), 
top_50_customers AS (
  SELECT ct.CustomerID, c.Age, ct.TotalQuantity
  FROM customer_totals ct
  JOIN customers c ON ct.CustomerID = c.CustomerID
  ORDER BY ct.TotalQuantity DESC
  LIMIT 50
)
SELECT
  AVG(Age) AS AverageAgeOfTop50Customers
FROM top_50_customers;
-- to find what is the average age of top 50 customers and it is 45years old. So, we can target them for marketing.

-- on which day maximum purchases were made.
SELECT
  TransactionDate,
  SUM(QuantityPurchased) AS TotalQuantity
FROM sales_transactions
GROUP BY TransactionDate
ORDER BY TotalQuantity DESC
LIMIT 1;
-- On 2nd June 2023 maximum quantities were purchased. SO, for next time we can have more inventory for this day and give discounts.

-- which location is most coming
SELECT
  Location,
  COUNT(*) AS CustomerCount
FROM customers
GROUP BY Location
ORDER BY CustomerCount DESC
LIMIT 1;
-- so from west people are coming more to purcahse. We can plan to open an outlet in west in future.

-- which category has how many products in it and what is stock count
SELECT Category,
  COUNT(*) AS ProductCount,
  SUM(StockLevel) AS TotalStock
FROM inventory
GROUP BY Category
ORDER BY ProductCount DESC;
-- clothing has very few items in it, we must increase items in it. As it is the most giving revernue category.

-- Hence we can say, clothing is the most revenue generating category. But it has least items which should be increased to lure customers.
-- We have more customers from west so we may think an outlet to open in west and focus to attract customer of other regions too.
-- 