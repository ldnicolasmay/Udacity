import datetime
import logging
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Operators and Tasks

def hello_world():
    logging.info("Hello World")


def current_time():
    logging.info(f"Current time is {datetime.datetime.utcnow().isoformat()}")


def working_dir():
    logging.info(f"Working directory is {os.getcwd()}")


def complete():
    logging.info("Congrats, your first multi-task pipelines is now complete!")


dag = DAG("lesson1.exercise3",
          schedule_interval="@daily",
          start_date=datetime.datetime.now() - datetime.timedelta(days=2))

hello_world_task = PythonOperator(task_id="hello_world_task",
                                  python_callable=hello_world,
                                  dag=dag)

# TODO: Create Tasks and Operators
current_time_task = PythonOperator(task_id="current_time_task",
                                   python_callable=current_time,
                                   dag=dag)

working_dir_task = PythonOperator(task_id="working_dir_task",
                                  python_callable=working_dir,
                                  dag=dag)

complete_task = PythonOperator(task_id="complete_task",
                               python_callable=complete,
                               dag=dag)

# TODO: Configure the task dependencies such that the graph looks like the following:
#
#                   -> current_time_task
#                  /                    \
#  hello_world_task                      -> complete_task
#                  \                    /
#                   -> working_dir_task
#

hello_world_task >> current_time_task
hello_world_task >> working_dir_task
current_time_task >> complete_task
working_dir_task >> complete_task
