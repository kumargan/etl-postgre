db_conn_string=${1}
db_user=${2}
db_pwd=${3}
source_table=${4}
hive_temp_table_name=${5}
scoop_executors=${6}
last_n_days=${7}
job_name=${8}
file_path=${9}

start_date=$(date -d "$(date) -$last_n_days days" +%Y-%m-%d)

sqoop import --connect ${db_conn_string}  --username ${db_user} --password ${db_pwd} --table ${source_table}  --where "TRADE_TIME > '${start_date}'"  --hive-import  --hive-table ${hive_temp_table_name} --m ${scoop_executors}

if [ $? -eq 0 ]; then
	hive -e "INSERT INTO status VALUES('$job_name','$d','true')"
	echo "Sqoop successful "  >> $file_path
else
    echo " Sqoop Failed"  >> $file_path
fi

echo "starting sqoop for $last_n_days days, data pull start_date is $start_date "  >> $file_path
