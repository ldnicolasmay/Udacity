import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")


#
# TODO: Add a monthly `schedule_interval` argument to the following DAG
#
dag = DAG("lesson1.exercise2",
          schedule_interval="@daily",
          start_date=datetime.datetime.now() - datetime.timedelta(days=2))

task = PythonOperator(task_id="hello_world_task",
                      python_callable=hello_world,
                      dag=dag)

# Start Airflow on Udacity Coco workspace by running this command in Bash:
# $ /opt/airflow/start.sh
