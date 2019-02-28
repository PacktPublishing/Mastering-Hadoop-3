CREATE TABLE IF NOT EXISTS product(
product_id int,
product_name String,
product_catagory
price String,
manufacturer String
)
PARTITIONED BY (manufacturer_country STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/user/packt/products;