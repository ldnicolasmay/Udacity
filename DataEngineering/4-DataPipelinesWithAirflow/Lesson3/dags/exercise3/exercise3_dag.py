import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
# from airflow.operators import HasRowsOperator
# from airflow.operators import S3ToRedshiftOperator

from exercise3.lesson3_exercise3_subdag import get_s3_to_redshift_dag

import sql_statements

# start_date = datetime.datetime.utcnow()
start_date = datetime.datetime(2018, 1, 1, 0, 0, 0, 0)
end_date = datetime.datetime(2018, 6, 1, 0, 0, 0, 0)

dag = DAG(
    "lesson3.exercise3",
    start_date=start_date,
    end_date=end_date,
    schedule_interval="@monthly"
)

trips_task_id = "trips_subdag"
trips_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        parent_dag_name="lesson3.exercise3",
        task_id=trips_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials_redshift",
        table="trips",
        create_sql_stmt=sql_statements.CREATE_TRIPS_TABLE_SQL,
        s3_bucket="udacity-dend",
        s3_key="udac-data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
        start_date=start_date
    ),
    task_id=trips_task_id,
    dag=dag
)

stations_task_id = "stations_subdag"
stations_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        parent_dag_name="lesson3.exercise3",
        task_id=stations_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials_redshift",
        table="stations",
        create_sql_stmt=sql_statements.CREATE_STATIONS_TABLE_SQL,
        s3_bucket="udacity-dend",
        s3_key="udac-data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
        start_date=start_date
    ),
    task_id=stations_task_id,
    dag=dag
)

# TODO: Migrate Create task, Copy task, and Check task to the Subdag

# create_trips_table = PostgresOperator(
#     task_id="create_trips_table",
#     postgres_conn_id="redshift",
#     sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
#     dag=dag
# )

# copy_trips_task = S3ToRedshiftOperator(
#     task_id="load_trips_from_s3_to_redshift",
#     table="trips",
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials_redshift",
#     s3_bucket="udacity-dend",
#     s3_key="udac-data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
#     dag=dag
# )

# check_trips = HasRowsOperator(
#     task_id="check_trips_data",
#     redshift_conn_id="redshift",
#     table="trips",
#     dag=dag
# )

# create_stations_table = PostgresOperator(
#     task_id="create_stations_table",
#     postgres_conn_id="redshift",
#     sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
#     dag=dag
# )

# copy_stations_task = S3ToRedshiftOperator(
#     task_id="load_stations_from_s3_to_redshift",
#     table="stations",
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials_redshift",
#     s3_bucket="udacity-dend",
#     s3_key="udac-data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
#     dag=dag
# )

# check_stations = HasRowsOperator(
#     task_id="check_stations_data",
#     redshift_conn_id="redshift",
#     table="stations",
#     dag=dag
# )

location_traffic_task = PostgresOperator(
    task_id='calculate_location_traffic',
    postgres_conn_id='redshift',
    sql=f"""
        {sql_statements.LOCATION_TRAFFIC_SQL}
        WHERE end_time >= {{{{ prev_ds }}}} AND end_time < {{{{ next_ds }}}}
    """,  # from https://airflow.apache.org/docs/stable/macros-ref.html
    dag=dag
)

# create_trips_table >> copy_trips_task
# create_stations_table >> copy_stations_task
# copy_trips_task >> check_trips
# copy_stations_task >> check_stations
# check_trips >> location_traffic_task
# check_stations >> location_traffic_task

trips_subdag_task >> location_traffic_task
stations_subdag_task >> location_traffic_task
