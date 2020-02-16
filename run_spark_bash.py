import os,sys,airflow,datetime
from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator

os.environ['SPARK_HOME'] = '/usr/local/spark/'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

args = {
  'owner': 'Airflow', 
  'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(dag_id='run_spark_with_bash', default_args=args, catchup=False, max_active_runs=1, schedule_interval='*/1 * * * *')

run_job_one = BashOperator(
  task_id='job_one',
  dag=dag,
  bash_command='spark-submit --class {{ params.class }} {{ params.jar }} {{ params.input_file }} {{ params.output_file }}',
  params={'class': 'com.github.moisvla.evangelist.JobOne'
          , 'jar': '/tmp/files/Evangelist-assembly-0.1.jar'
          , 'input_file': '/tmp/files/meetup.json'
          , 'output_file': '/tmp/files/rsvp.csv'}
  )

run_job_two = BashOperator(
  task_id='job_two',
  dag=dag,
  bash_command='spark-submit --class {{ params.class }} {{ params.jar }} {{ params.input_file }} {{ params.output_file }}',
  params={'class': 'com.github.moisvla.evangelist.JobTwo'
          , 'jar': '/tmp/files/Evangelist-assembly-0.1.jar'
          , 'input_file': '/tmp/files/rsvp.csv'
          , 'output_file': '/tmp/files/results.txt'}
  )

run_job_one >> run_job_two