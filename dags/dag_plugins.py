from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import DataTransferOperator,FileCountSensor
from airflow.hooks import MySQLToPostgresHook
from airflow.operators.python_operator import PythonOperator


dag = DAG('plugins_dag', schedule_interval=timedelta(1), start_date=datetime(2024, 9, 11), catchup=False)

# t1 = DataTransferOperator(
#         task_id='data_transfer',
#         source_file_path = '/usr/local/airflow/store_files_airflow/source.txt',
#         dest_file_path='/usr/local/airflow/store_files_airflow/destination.txt',
#         delete_list = ['Airflow', 'is'],
#         dag=dag
#     )


# t2 = FileCountSensor(
#     task_id = 'file_count_sensor',
#     dir_path = '/usr/local/airflow/store_files_airflow',
#     conn_id = 'fs_default',
#     poke_interval = 5,
#     timeout = 100,
#     dag = dag
# )


def trigger_hook():
    MySQLToPostgresHook().copy_table('mysql_conn', 'postgres_conn')
    print("done")

t1 = PythonOperator(
    task_id = 'mysql_to_postgres',
    python_callable = trigger_hook,
    dag = dag
)
