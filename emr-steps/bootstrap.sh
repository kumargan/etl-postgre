s3_code_folder=$1

sudo aws s3 cp s3://$s3_code_folder/jars/postgresql-42.3.3 /usr/lib/sqoop/lib/postgresql-42.3.3
