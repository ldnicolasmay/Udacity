import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 1, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG(
    'lesson4.sparkify_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log-data/2018/11/",
    json_as="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song-data/",
    json_as="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="songplays",
    sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="users",
    sql_stmt=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="songs",
    sql_stmt=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="artists",
    sql_stmt=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="time",
    sql_stmt=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    data_quality_checks=[
        {"test_sql": "SELECT COUNT(*) FROM staging_events", "expected_result": [(8056,)]},
        {"test_sql": "SELECT COUNT(*) FROM staging_songs", "expected_result": [(385252,)]},
        {"test_sql": "SELECT COUNT(*) FROM songplays", "expected_result": [(7039,)]},
        {"test_sql": "SELECT COUNT(*) FROM artists", "expected_result": [(45266,)]},
        {"test_sql": "SELECT COUNT(*) FROM songs", "expected_result": [(384824,)]},
        {"test_sql": "SELECT COUNT(*) FROM users", "expected_result": [(104,)]},
        {"test_sql": "SELECT COUNT(*) FROM time", "expected_result": [(7039,)]},
        {"test_sql": "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", "expected_result": [(0,)]},
        {"test_sql": "SELECT COUNT(*) FROM songplays WHERE start_time IS NULL", "expected_result": [(0,)]},
        {"test_sql": "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", "expected_result": [(0,)]},
        {"test_sql": "SELECT COUNT(*) FROM songs WHERE songid IS NULL", "expected_result": [(0,)]},
        {"test_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected_result": [(0,)]},
        {"test_sql": "SELECT COUNT(*) FROM time WHERE start_time IS NULL", "expected_result": [(0,)]},
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Define task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
