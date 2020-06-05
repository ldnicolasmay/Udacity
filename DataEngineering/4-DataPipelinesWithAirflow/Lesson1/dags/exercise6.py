import datetime

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

# import sql_statments

CREATE_TRIPS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS trips (
    trip_id INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    bikeid INTEGER NOT NULL,
    tripduration DECIMAL(16,2) NOT NULL,
    from_station_id INTEGER NOT NULL,
    from_station_name VARCHAR(100) NOT NULL,
    to_station_id INTEGER NOT NULL,
    to_station_name VARCHAR(100) NOT NULL,
    usertype VARCHAR(20),
    gender VARCHAR(6),
    birthyear INTEGER,
    PRIMARY KEY(trip_id))
    DISTSTYLE ALL;
"""

CREATE_STATIONS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS stations (
    id INTEGER NOT NULL,
    name VARCHAR(250) NOT NULL,
    city VARCHAR(100) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    dpcapacity INTEGER NOT NULL,
    online_date TIMESTAMP NOT NULL,
    PRIMARY KEY(id))
    DISTSTYLE ALL;
"""

COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{{}}'
    SECRET_ACCESS_KEY '{{}}'
    IGNOREHEADER 1
    DELIMITER ','
"""

COPY_MONTHLY_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/partitioned/{year}/{month}/divvy_trips.csv"
)

COPY_ALL_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

COPY_STATIONS_SQL = COPY_SQL.format(
    "stations",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
)

LOCATION_TRAFFIC_SQL = """
    BEGIN;
    DROP TABLE IF EXISTS station_traffic;
    CREATE TABLE station_traffic AS
    SELECT
        DISTINCT(t.from_station_id) AS station_id,
        t.from_station_name AS station_name,
        num_departures,
        num_arrivals
    FROM trips t
    JOIN (
        SELECT
            from_station_id,
            COUNT(from_station_id) AS num_departures
        FROM trips
        GROUP BY from_station_id
    ) AS fs ON t.from_station_id = fs.from_station_id
    JOIN (
        SELECT
            to_station_id,
            COUNT(to_station_id) AS num_arrivals
        FROM trips
        GROUP BY to_station_id
    ) AS ts ON t.from_station_id = ts.to_station_id
"""


def load_trips_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials_redshift")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key)
    redshift_hook.run(sql_stmt)


def load_stations_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials_redshift")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = COPY_STATIONS_SQL.format(credentials.access_key, credentials.secret_key)
    redshift_hook.run(sql_stmt)


dag = DAG("lesson1.exercise6",
          start_date=datetime.datetime.now())

create_trips_table = PostgresOperator(task_id='create_trips_table',
                                      postgres_conn_id='redshift',
                                      sql=CREATE_TRIPS_TABLE_SQL,
                                      dag=dag)

create_stations_table = PostgresOperator(task_id='create_stations_table',
                                         postgres_conn_id='redshift',
                                         sql=CREATE_STATIONS_TABLE_SQL,
                                         dag=dag)

copy_trips_task = PythonOperator(task_id="copy_trips_to_redshift",
                                 python_callable=load_trips_data_to_redshift,
                                 dag=dag)

copy_stations_task = PythonOperator(task_id='copy_stations_to_redshift',
                                    python_callable=load_stations_data_to_redshift,
                                    dag=dag)

location_traffic_task = PostgresOperator(task_id='calculate_location_traffic',
                                         postgres_conn_id='redshift',
                                         sql=LOCATION_TRAFFIC_SQL,
                                         dag=dag)

create_trips_table >> copy_trips_task
create_stations_table >> copy_stations_task
copy_trips_task >> location_traffic_task
copy_stations_task >> location_traffic_task

# Took a while to get this to work, but I did it.

# For this to work, follow these steps
# 1. Start Airflow webserver and scheduler
# 2. Open browser to localhost:8080 for Airflow web UI
# 3. Click on Admin menu button
#    a. Click Connections
#    b. Create "redshift" connection:
#       i. Conn Id: redshift
#       ii. Conn Type: Postgres
#       iii. Host: redshift-cluster-1.choa9clr8kdj.us-west-2.redshift.amazonaws.com
#       iv. Schema: dev
#       v. Login: awsuser
#       vi. Password: *********
#       vii. Port: 5439

# Notes for Redshift cluster setup
# 1. Attach a IAM role that permits full Redshift access (e.g., "myRedshiftRole")
# 2. Don't accept default networking settings:
#    a. Choose default VPC in the region of interest (us-west-2 for Udacity projects)
#    b. Choose subnet that was created for the cluster (e.g., redshift-cluster-subnet-group-1)...
#       if a subnet for the cluster doesn't exist, create one
#    c. Attach a security group that allows port 5439 inbound and all outbound traffic (e.g. redshift-security-group)
#    d. Make the cluster/database publicly accessible
