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
    'all',
    default_args=default_args,
    description='script for sqoop_hive_hbase_processing',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),
)



t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='run_script',
    depends_on_past=False,
    bash_command='sh /home/fieldemployee/Big_Data_Training/HIVE_HBASE/gcp/scripts/all.sh ',
    dag=dag,
)
t1 >> t2

#t1.set_downstream(t2)

