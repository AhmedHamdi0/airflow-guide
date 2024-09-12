-- SELECT DATE, STORE_LOCATION, ROUND((SUM(SP) - SUM(CP)), 2) AS lc_profit FROM clean_store_transactions WHERE DATE = SUBDATE(date(now()),1) GROUP BY STORE_LOCATION ORDER BY lc_profit DESC INTO OUTFILE '/store_files_mysql/location_wise_profit.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
-- SELECT DATE, STORE_ID, ROUND((SUM(SP) - SUM(CP)), 2) AS st_profit FROM clean_store_transactions WHERE DATE = SUBDATE(date(now()),1) GROUP BY STORE_ID ORDER BY st_profit DESC INTO OUTFILE '/store_files_mysql/store_wise_profit.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

COPY (SELECT DATE, STORE_LOCATION, ROUND((SUM(SP) - SUM(CP)), 2) AS lc_profit
      FROM clean_store_transactions
      WHERE DATE = CURRENT_DATE - INTERVAL '1 day'
      GROUP BY DATE, STORE_LOCATION
      ORDER BY lc_profit DESC)
TO '/usr/local/airflow/store_files_airflow/location_wise_profit.csv' DELIMITER ',' CSV HEADER;

COPY (SELECT DATE, STORE_ID, ROUND((SUM(SP) - SUM(CP)), 2) AS st_profit
      FROM clean_store_transactions
      WHERE DATE = CURRENT_DATE - INTERVAL '1 day'
      GROUP BY DATE, STORE_ID
      ORDER BY st_profit DESC)
TO '/usr/local/airflow/store_files_airflow/store_wise_profit.csv' DELIMITER ',' CSV HEADER;
