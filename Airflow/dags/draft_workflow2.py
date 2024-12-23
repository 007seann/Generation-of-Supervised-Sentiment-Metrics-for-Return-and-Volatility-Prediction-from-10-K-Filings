
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from airflow.operators.email_operator import EmailOperator


# Import the necessary function from the script in the src directory
from src.choose_branch_two import choose_branch_two
from src.training_model import training_model
from src.predict_and_store import predict_and_store
from src.plot_data_task import plot_data_task




with DAG("draft_workflow_two2",
    start_date=datetime(2023, 2 ,10), 
    schedule="@daily", 
    ##schedule_interval='*/30 * * * *', 
    catchup=False,
    tags=["project"],
    ) as dag:
    
    with TaskGroup('runModel') as runModel:
        check_for_trained_model = BashOperator(
            task_id="check_for_trained_model",
            bash_command=" echo 'check_for_trained_model'"
        )
        get_DB_data = BashOperator(
            task_id="get_new_data",
            bash_command=" echo 'get_DB_data'"
        )
        feed_to_model = PythonOperator(
            task_id="feed_to_model",
            python_callable=predict_and_store

        )


        
    with TaskGroup(group_id='webApplicationTrackingModelPerformance', prefix_group_id=False) as web_application_tracking_model_performance:
        new_data_visualisation = PythonOperator(
            task_id="new_data_visualisation",
            python_callable=training_model
        )

        prediction_visualisation = PythonOperator(
            task_id="prediction_visualisation",
            python_callable=plot_data_task
        )

        run_django_website = BashOperator(
            task_id='start_server',
             bash_command="echo 'start_server'"
            # bash_command='cd /opt/airflow/Website/ && python manage.py runserver 0.0.0.0:8000',
        )
        
    with TaskGroup(group_id='modelValidation', prefix_group_id=False) as modelValidation:

        validate_model = BranchPythonOperator(
            task_id="validate_model",
            python_callable=choose_branch_two,
            provide_context=True,  
        )
    
        good_accuracy = BashOperator(
            task_id="good_accuracy",
            bash_command="echo 'good_accuracy'"
        )

        bad_accuracy = BashOperator(
            task_id="bad_accuracy",
            bash_command=" echo 'bad_accuracy'"
        )

    
    with TaskGroup('furtherAction') as furtherAction:
        deploy_new_model = BashOperator(
            task_id="deploy_new_model",
            bash_command=" echo 'deploy_new_model'"
        )
        fallback_to_deployed_model = BashOperator(
            task_id="fallback_to_deployed_model",
            bash_command=" echo 'fallback_to_deployed_model'"
        )
        
        alert_email = EmailOperator(
            task_id='alert_email',
            to='s1909083@ed.ac.uk',
            subject='Alert Mail',
            html_content=""" Mail Test """,
            )
            
        merge_new_data_with_old = BashOperator(
            task_id="merge_new_data_with_old",
            bash_command=" echo 'merge_new_data'"
        )
        
        serve_prediction = BashOperator(
            task_id="serve_prediction",
            bash_command=" echo 'serve_prediction'"
        )
    


    get_DB_data >> new_data_visualisation
    check_for_trained_model >> get_DB_data >> feed_to_model
    new_data_visualisation >> validate_model
    validate_model >> [good_accuracy, bad_accuracy]
    feed_to_model >> prediction_visualisation >> run_django_website 
    prediction_visualisation >> validate_model
    good_accuracy >> [deploy_new_model,merge_new_data_with_old]
    bad_accuracy >> [fallback_to_deployed_model,alert_email]
    deploy_new_model >> serve_prediction
