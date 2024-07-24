#!/bin/bash

CSV_PATH=$1
DEL_DT=$2

mysql --local-infile=1 -u root -p'qwer123' << EOF
-- LOAD DATA INFILE '/var/lib/mysql-files/csv_${CSV_PATH}.csv'
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';
LOAD DATA LOCAL INFILE '$CSV_PATH'
INTO TABLE history_db.tmp_cmd_usage
CHARACTER SET latin1 
FIELDS TERMINATED BY ',' ENCLOSED BY '^' ESCAPED BY '\b'
LINES TERMINATED BY '\n';
EOF
