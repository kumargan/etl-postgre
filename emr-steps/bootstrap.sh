s3_code_folder=$1

sudo aws s3 cp s3://$s3_code_folder/jars/postgresql-42.3.3.jar /usr/lib/sqoop/lib/postgresql-42.3.3.jar
sudo aws s3 cp s3://$s3_code_folder/jars/hive-contrib-3.0.0.jar /usr/lib/hive-hcatalog/share/hcatalog/hive-contrib-3.0.0.jar