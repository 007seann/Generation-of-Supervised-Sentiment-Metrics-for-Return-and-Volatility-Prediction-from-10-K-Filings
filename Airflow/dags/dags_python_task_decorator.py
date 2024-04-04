
import pendulum
from airflow import DAG
from airflow.decorators import task
import pprint

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,

) as dag:
    
    # [START howto_operator_python]
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)
        
    python_task_1 = print_context('Execute task decorator')
    # [END howto_operator_python]