import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


# Context and Templating

def log_details(*args, **kwargs):
    #
    # TODO: Log details of this run that were passed in by the Airflow context
    #
    #       Look here for context variables passed in on kwargs:
    #       https://airflow.apache.org/code.html#macros
    run_id = kwargs['run_id']
    previous_ds = kwargs.get('prev_ds')
    next_ds = kwargs.get('next_ds')

    logging.info(f"My execution date is {kwargs['ds']}")
    logging.info(f"My execution date is {kwargs['execution_date']}")
    logging.info(f"My run id is {run_id}")
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    if next_ds:
        logging.info(f"My next run will be {next_ds}")


dag = DAG("lesson1.exercise5",
          schedule_interval="@daily",
          start_date=datetime.datetime.now() - datetime.timedelta(days=2))

# TODO: Provide Context
list_task = PythonOperator(task_id="log_details",
                           python_callable=log_details,
                           provide_context=True,
                           dag=dag)
