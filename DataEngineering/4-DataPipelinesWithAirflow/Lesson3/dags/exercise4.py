import datetime

from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator
)

import sql_statements


# TODO: Create a DAG which performs the following functions:
#       1. Loads Trip data from S3 to Redshift
#       2. Performs a data quality check on Trips table in Redshift
#       3. Uses the FactsCalculatorOperator to creat a Facts table in Redshift
#          a. **NOTE**: To complete this step you must complete the FactsCalculatorOperator
#             skeleton define in plugins/operators/facts_calculator.py

start_date = datetime.datetime(2018, 1, 1)
end_date = datetime.datetime(2019, 1, 1)

dag = DAG(
    "lesson3.exercise4",
    start_date=start_date,
    end_date=end_date,
    schedule_interval="@monthly"
)


# TODO: Create trips table
create_trips_table = PostgresOperator(
    task_id='create_trips_table',
    postgres_conn_id='redshift',
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
    dag=dag
)


# TODO: Load trips data from S3 to Redshift.
#       Use the s3_bucket "udacity-dend"
#       Use the s3_key "data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
copy_trips = S3ToRedshiftOperator(
    task_id="copy_trips",
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials_redshift",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
    dag=dag
)


# TODO: Perform a data quality check on the Trips table
check_trips = HasRowsOperator(
    task_id="check_trips",
    redshift_conn_id="redshift",
    table="trips",
    dag=dag
)


# TODO: Use the FactsCalculatorOperator to create a Facts table in Redshift.
#       The fact column should be `tripduration` and the groupby_column should be `bikeid`
calculate_facts = FactsCalculatorOperator(
    task_id="calculate_facts",
    redshift_conn_id="redshift",
    origin_table="trips",
    destination_table="trips_facts",
    fact_column="tripduration",
    groupby_column="bikeid",
    dag=dag
)


# TODO: Define task ordering for the DAG tasks you defined.
create_trips_table >> copy_trips
copy_trips >> check_trips
check_trips >> calculate_facts
