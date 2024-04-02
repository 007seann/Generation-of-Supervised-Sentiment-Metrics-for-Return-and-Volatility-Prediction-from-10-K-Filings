from airflow import DAG
import pendulum
import datetime
for airflow.operators.python import PythonOperator

with DAG(
    dig_id = 'dags_python_operator'
    schedule="30 6 * * 1",
    start_date= pendulum.datetime(2021, 1, 1),
    catchup = False
) as dag:
    def 