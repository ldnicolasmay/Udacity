import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import HasRowsOperator
from airflow.operators import S3ToRedshiftOperator


import sql_statements


def check_greater_than_zero(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    logging.info(records)

    if len(records) < 1 or len(records[0]) < 1:
        logging.error(f"Data quality check failed; {table} returned no results")
        raise ValueError(f"Data quality check failed; {table} returned no results")

    num_records = records[0][0]
    if num_records < 1:
        logging.error(f"Data quality check failed; {table} contained 0 rows")
        raise ValueError(f"Data quality check failed; {table} contained 0 rows")

    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


dag = DAG(
    "lesson3.exercise1",
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2019, 1, 1, 0, 0, 0, 0),
    schedule_interval="@monthly",
    max_active_runs=1
)

create_trips_table = PostgresOperator(
    task_id='create_trips_table',
    postgres_conn_id='redshift',
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
    dag=dag
)

create_stations_table = PostgresOperator(
    task_id='create_stations_table',
    postgres_conn_id='redshift',
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
    dag=dag
)

copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials_redshift",
    s3_bucket="udacity-dend",
    s3_key="udac-data-pipelines/divvy/partitioned/{{execution_date.year}}/{{execution_date.month}}/divvy_trips.csv",
    dag=dag
)

copy_stations_task = S3ToRedshiftOperator(
    task_id="load_stations_from_s3_to_redshift",
    table="stations",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentaisl_redshift",
    s3_bucket="udacity-dend",
    s3_key="udac-data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
    dag=dag
)

check_trips = HasRowsOperator(
    task_id="check_trips_data",
    redshift_conn_id="redshift",
    table="trips",
    dag=dag
)

check_stations = HasRowsOperator(
    task_id="check_stations_data",
    redshift_conn_id="redshift",
    table="stations",
    dag=dag
)

location_traffic_task = PostgresOperator(
    task_id='calculate_location_traffic',
    postgres_conn_id='redshift',
    sql=f"""
        {sql_statements.LOCATION_TRAFFIC_SQL}
        WHERE end_time >= {{{{ prev_ds }}}} AND end_time < {{{{ next_ds }}}}
    """,  # from https://airflow.apache.org/docs/stable/macros-ref.html
    dag=dag
)

create_trips_table >> copy_trips_task
create_stations_table >> copy_stations_task
copy_trips_task >> check_trips
copy_stations_task >> check_stations
check_trips >> location_traffic_task
check_stations >> location_traffic_task
