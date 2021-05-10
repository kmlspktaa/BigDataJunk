from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(1),
    # 'end_date': datetime(2020, 10, 22),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(
    'producer_dag',
    default_args=default_args,
    description='script for capstone pipeline',
    # Continue to run DAG once per day
    execution_timeout=timedelta(minutes=35),
    schedule_interval=timedelta(days=1),
)


t1 = BashOperator(
   task_id='run_producer',
   bash_command='spark-submit /home/fieldemployee/Big_Data_Training/Kafka/cpProject/src/stocks/alphavantage_api.py ',
   dag=dag,
)



#t1.set_downstream(t2)

