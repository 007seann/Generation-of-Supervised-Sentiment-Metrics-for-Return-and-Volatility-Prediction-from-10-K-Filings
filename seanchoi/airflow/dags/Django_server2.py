from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from datetime import timedelta
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys



def get_news_headlines(api_key, queries):
    all_headlines = []
    
    for query in queries:
        url = f"https://newsapi.org/v2/everything?q={query}&apiKey={api_key}"
        response = requests.get(url)
        data = response.json()
        
        if data['status'] == 'ok':
            headlines = [article['title'] for article in data['articles']]
            all_headlines.extend(headlines)
        else:
            raise Exception("Error retrieving news headlines")
    
    return all_headlines

queries = [
    "US interest rate rise",
    "inflation",
    "GDP growth",
    "unemployment rate",
    "central bank policy",
    # Add more queries as needed
]


def retrieve_headlines():
    api_key = "f4d5a864c6274805ba68b693dfc940d4"
    headlines = get_news_headlines(api_key, queries)
    
    for headline in headlines:
        print(headline)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'Django_web_server',
    default_args=default_args,
    description='Retrieve important economic event news headlines',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 4, 1),
    tags=["project"],
    catchup=False,
) as dag:

    with TaskGroup(group_id='webApplication', prefix_group_id=False) as webApplication:

        retrieve_events = BashOperator(
            task_id='retrieve_headlines',
            bash_command="echo 'retrieve_stock_prices'"
        )

        load_true_visualisations = BashOperator(
            task_id="load_true_visualisations",
            bash_command="echo 'load_true_visualisations'"
        )

        retrieve_stock_prices = BashOperator(
            task_id="retrieve_stock_prices",
            bash_command="echo 'retrieve_stock_prices'"
        )
    
        run_django_website = BashOperator(
            task_id='start_server',
            bash_command='cd /opt/airflow/Website/ && python manage.py runserver 0.0.0.0:8000',
        )
        

    [retrieve_events, retrieve_stock_prices] >> load_true_visualisations
    run_django_website 