import datetime

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements  # symbolically linked so IDE will highlight with error


def load_trips_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials_redshift")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key)
    redshift_hook.run(sql_stmt)


def load_stations_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials_redshift")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(credentials.access_key, credentials.secret_key)
    redshift_hook.run(sql_stmt)


dag = DAG("lesson1.exercise6",
          start_date=datetime.datetime.now())

create_trips_table = PostgresOperator(task_id='create_trips_table',
                                      postgres_conn_id='redshift',
                                      sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
                                      dag=dag)

create_stations_table = PostgresOperator(task_id='create_stations_table',
                                         postgres_conn_id='redshift',
                                         sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
                                         dag=dag)

copy_trips_task = PythonOperator(task_id="copy_trips_to_redshift",
                                 python_callable=load_trips_data_to_redshift,
                                 dag=dag)

copy_stations_task = PythonOperator(task_id='copy_stations_to_redshift',
                                    python_callable=load_stations_data_to_redshift,
                                    dag=dag)

location_traffic_task = PostgresOperator(task_id='calculate_location_traffic',
                                         postgres_conn_id='redshift',
                                         sql=sql_statements.LOCATION_TRAFFIC_SQL,
                                         dag=dag)

create_trips_table >> copy_trips_task
create_stations_table >> copy_stations_task
copy_trips_task >> location_traffic_task
copy_stations_task >> location_traffic_task

# Took a while to get this to work, but I did it.

# For this to work, follow these steps
# 1. Start Airflow webserver and scheduler
# 2. Open browser to localhost:8080 for Airflow web UI
# 3. Click on Admin menu button; Click Connections
#    a. Create "redshift" connection:
#       i. Conn Id: redshift
#       ii. Conn Type: Postgres
#       iii. Host: redshift-cluster-1.choa9clr8kdj.us-west-2.redshift.amazonaws.com
#       iv. Schema: dev
#       v. Login: awsuser
#       vi. Password: *********
#       vii. Port: 5439
#    b. Create "aws_credentials_redshift" connection:
#       i. Conn Id: "aws_credentials_redshift"
#       ii. Conn Type: Amazon Web Services
#       iii. Login: [access key for IAM role with full Redshift permissions]
#       iv. Password: [secret key for " " " " " "]

# Notes for Redshift cluster setup
# 1. Attach a IAM role that permits full Redshift access (e.g., "myRedshiftRole")
# 2. Don't accept default networking settings:
#    a. Choose default VPC in the region of interest (us-west-2 for Udacity projects)
#    b. Choose subnet that was created for the cluster (e.g., redshift-cluster-subnet-group-1)...
#       if a subnet for the cluster doesn't exist, create one
#    c. Attach a security group that allows port 5439 inbound and all outbound traffic (e.g. redshift-security-group)
#    d. Make the cluster/database publicly accessible
