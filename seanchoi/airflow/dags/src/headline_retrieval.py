import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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



