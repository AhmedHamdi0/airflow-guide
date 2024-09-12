-- LOAD DATA INFILE '/store_files_mysql/clean_store_transactions.csv' INTO TABLE clean_store_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

COPY clean_store_transactions FROM '/usr/local/airflow/store_files_airflow/clean_store_transactions.csv' DELIMITER ',' CSV HEADER;
