CREATE TABLE IF NOT EXISTS status ( job String, rundate String,success String );


CREATE EXTERNAL TABLE table_out
(
id String, 
first_name String, 
last_name String, 
email String, 
mobile_no String,
sauda_date String
)
PARTITIONED BY (month string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("field.delim"="||")
STORED AS TEXTFILE 
LOCATION 's3://${s3_data_folder}/output/equity';