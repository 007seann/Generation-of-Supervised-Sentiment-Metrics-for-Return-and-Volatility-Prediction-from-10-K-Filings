FROM apache/airflow:2.6.2
ADD requirements.txt .
RUN pip install apache-airflow==2.6.2 -r requirements.txt