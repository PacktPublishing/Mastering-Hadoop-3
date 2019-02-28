CREATE TABLE IF NOT EXISTS product(
product_id int,
product_name String,
product_catagory
price String,
manufacturer String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/user/packt/products;


CREATE EXTERNAL TABLE IF NOT EXISTS product(
product_id int,
product_name String,
product_catagory
price String,
manufacturer String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/user/packt/products;