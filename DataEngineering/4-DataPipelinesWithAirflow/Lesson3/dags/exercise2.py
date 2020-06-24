import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def oldest_rider():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        select birthyear from older_riders order by birthyear asc limit 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


def youngest_rider():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        select birthyear from younger_riders order by birthyear desc limit 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")


# # TODO: What are the dependencies here? How could we split this up into subtasks?
# def load_and_analyze(*args, **kwargs):
#     redshift_hook = PostgresHook("redshift")
#
#     # # Find all trips where the rider was over 80
#     # redshift_hook.run("""
#     #     begin;
#     #     drop table if exists older_riders;
#     #     create table older_riders as (
#     #         select * from trips where birthyear > 0 and birthyear <= 1945
#     #     );
#     #     commit;
#     # """)
#     # records = redshift_hook.get_records("""
#     #     select birthyear from older_riders order by birthyear asc limit 1
#     # """)
#     # if len(records) > 0 and len(records[0]) > 0:
#     #     logging.info(f"Oldest rider was born in {records[0][0]}")
#
#     # Find all trips where the rider was under 18
#     # redshift_hook.run("""
#     #     begin;
#     #     drop table if exists younger_riders;
#     #     create table younger_riders as (
#     #         select * from trips where birthyear > 2000
#     #     );
#     #     commit;
#     # """)
#     # records = redshift_hook.get_records("""
#     #     select birthyear from younger_riders order by birthyear desc limit 1
#     # """)
#     # if len(records) > 0 and len(records[0]) > 0:
#     #     logging.info(f"Youngest rider was born in {records[0][0]}")
#
#     # # Find out how often each bike is ridden
#     # redshift_hook.run("""
#     #     begin;
#     #     drop table if exists lifetime_riders;
#     #     create table lifetime_riders as (
#     #         select bikeid, COUNT(bikeid)
#     #         from trips
#     #         group by bikeid
#     #     );
#     #     commit;
#     # """)
#
#     # # Count the number of stations by city
#     # redshift_hook.run("""
#     #     begin;
#     #     drop table if exists city_station_counts;
#     #     create table city_station_counts as (
#     #         select city, count(city)
#     #         from stations
#     #         group by city
#     #     );
#     #     commit;
#     # """)

dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)

# load_and_analyze = PythonOperator(
#     task_id="load_and_analyze",
#     python_callable=load_and_analyze,
#     provide_context=True,
#     dag=dag
# )

oldest_riders_table = PostgresOperator(
    task_id="oldest_riders_table",
    postgres_conn_id="redshift",
    sql="""
        begin;
        drop table if exists older_riders;
        create table older_riders as (
            select * from trips where birthyear > 0 and birthyear <= 1945
        );
        commit;
    """,
    dag=dag
)

youngest_riders_table = PostgresOperator(
    task_id="youngest_riders_table",
    postgres_conn_id="redshift",
    sql="""
        begin;
        drop table if exists younger_riders;
        create table younger_riders as (
            select * from trips where birthyear > 2000
        );
        commit;
    """,
    dag=dag
)

log_oldest_rider = PythonOperator(
    task_id="log_oldest_rider",
    python_callable=oldest_rider,
    dag=dag
)

log_youngest_rider = PythonOperator(
    task_id="log_youngest_rider",
    python_callable=youngest_rider,
    dag=dag
)

count_bikerides_by_bike = PostgresOperator(
    task_id="count_bikerides_by_bike",
    postgres_conn_id="redshift",
    sql="""
        begin;
        drop table if exists lifetime_rides;
        create table lifetime_rides as (
            select bikeid, COUNT(bikeid)
            from trips
            group by bikeid
        );
        commit;
    """,
    dag=dag
)

count_stations_by_city = PostgresOperator(
    task_id="count_stations_by_city",
    postgres_conn_id="redshift",
    sql="""
        begin;
        drop table if exists city_station_counts;
        create table city_station_counts as (
            select city, count(city)
            from stations
            group by city
        );
        commit;
    """,
    dag=dag
)

oldest_riders_table >> log_oldest_rider
youngest_riders_table >> log_youngest_rider
