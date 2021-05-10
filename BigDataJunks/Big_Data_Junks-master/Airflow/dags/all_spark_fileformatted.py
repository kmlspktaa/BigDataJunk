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
    'all_spark_fileformatted',
    default_args=default_args,
    description='script for sqoop_spark_hive_processing',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),
)


#t1 = BashOperator(
#   task_id='print_date',
#   bash_command='date',
#   dag=dag,
#)


t1 = BashOperator(
   task_id='cleaning_HDFS',
   bash_command='hdfs dfs -rm -r /user/input/*',
   dag=dag,
)

t2 = BashOperator(
   task_id='run_mysql_to_hdfs',
   depends_on_past=False,
   bash_command='spark-submit /home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/src/mysql_to_hdfs.py ',
   dag=dag,
)

t3 = BashOperator(
   task_id='run_postgresql_to_hdfs',
   depends_on_past=False,
   bash_command='spark-submit /home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/src/postgresql_to_hdfs.py ',
   dag=dag,
)

t4 = BashOperator(
   task_id='run_sqlserver_to_hdfs',
   depends_on_past=False,
   bash_command='spark-submit /home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/src/sqlserver_to_hdfs.py ',
   dag=dag,
)

t5= BashOperator(
   task_id='ftp_csv_to_lfs',
   depends_on_past=False,
   bash_command='sh /home/fieldemployee/bin/scripts/sftp_to_lfs.sh ',
   dag=dag,
)

t6 = BashOperator(
    task_id='run_sparkJobs',
    depends_on_past=False,
    bash_command='sh /home/fieldemployee/all_file_formatted.sh ',
    dag=dag,
)
t1 >> [t2, t3, t4, t5] >> t6 

#t1.set_downstream(t2)

