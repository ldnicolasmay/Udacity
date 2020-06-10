import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements  # symbolically linked so IDE will highlight with error


def load_trip_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials_redshift")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")

    # TODO: How do we get the execution_date from our context?
    execution_date = kwargs["execution_date"]
    # execution_date = datetime.datetime.utcnow()

    sql_stmt = sql_statements.COPY_MONTHLY_TRIPS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
        year=execution_date.year,
        month=execution_date.month
    )
    redshift_hook.run(sql_stmt)


def load_station_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)


dag = DAG(
    "lesson2.exercise3",
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

copy_trips_task = PythonOperator(
    task_id="copy_trips_to_redshift",
    python_callable=load_trip_data_to_redshift,
    # op_args=[sql_statements.CREATE_TRIPS_TABLE_SQL],
    # op_kwargs={"sql_stmt": sql_statements.COPY_ALL_TRIPS_SQL},
    provide_context=True,
    dag=dag
)

copy_stations_task = PythonOperator(
    task_id="copy_stations_to_redshift",
    python_callable=load_station_data_to_redshift,
    # op_args=[sql_statements.CREATE_STATIONS_TABLE_SQL],
    # op_kwargs={"sql_stmt": sql_statements.COPY_STATIONS_SQL},
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
copy_trips_task >> location_traffic_task
copy_stations_task >> location_traffic_task
