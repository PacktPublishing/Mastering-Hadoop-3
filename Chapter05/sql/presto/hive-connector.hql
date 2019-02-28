CREATE SCHEMA hive.test_db
WITH (location = '/home/packt/presto');

CREATE TABLE hive.test_db.test_table (
 id int,
 name varchar,
 email varchar,
 age int
)
WITH (
 format = 'TEXTFILE',
 external_location = '/user/packt/employees'
);

select * from hive.test_db.test_table;