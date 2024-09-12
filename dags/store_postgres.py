from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator

from datacleaner import data_cleaner

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'Postgres Swift',
    'start_date': datetime(2024, 9, 7),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('store_postpg',default_args=default_args,schedule_interval='@daily', template_searchpath=['/usr/local/airflow/sql_files'], catchup=True) as dag:

    t1 = BashOperator(task_id='check_file_exists',
                      bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
                      retries=2, retry_delay=timedelta(seconds=15))

    t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)

    t3 = PostgresOperator(task_id='create_postgres_table', postgres_conn_id="postgres_conn", sql="create_table.sql")

    t4 = PostgresOperator(task_id='insert_into_table', postgres_conn_id="postgres_conn", sql="insert_into_table.sql")

    t5 = PostgresOperator(task_id='select_from_table', postgres_conn_id="postgres_conn", sql="select_from_table.sql")

    t6 = BashOperator(task_id='move_file1', bash_command='cat ~/store_files_airflow/location_wise_profit.csv && mv ~/store_files_airflow/location_wise_profit.csv ~/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date)

    t7 = BashOperator(task_id='move_file2', bash_command='cat ~/store_files_airflow/store_wise_profit.csv && mv ~/store_files_airflow/store_wise_profit.csv ~/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date)

    t8 = EmailOperator(task_id='send_email',
        to='ahmed88hamdi99@gmail.com',
        subject='Daily report generated',
        html_content=""" <h1>Congratulations! Your store reports are ready.</h1> """,
        files=['/usr/local/airflow/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date, '/usr/local/airflow/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date])

    t9 = BashOperator(task_id='rename_raw', bash_command='mv ~/store_files_airflow/raw_store_transactions.csv ~/store_files_airflow/raw_store_transactions_%s.csv' % yesterday_date)

    t1 >> t2 >> t3 >> t4 >> t5 >> [t6,t7] >> t8 >> t9
