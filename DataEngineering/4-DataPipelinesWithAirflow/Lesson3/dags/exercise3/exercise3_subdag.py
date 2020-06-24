import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import HasRowsOperator
from airflow.operators import S3ToRedshiftOperator

# import sql_statements


# Returns a DAG which creates a table if it does not exists, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality check is performed to assert that at least one row of data is present.
def get_s3_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_sql_stmt,
        s3_bucket,
        s3_key,
        *args, **kwargs
):
    dag = DAG(
        dag_id=f"{parent_dag_name}.{task_id}",  # subdags' dag_id must follow this naming pattern
        **kwargs
    )

    # TODO: Move the create table code here
    create_table_task = PostgresOperator(
        task_id=f"create_{table}_table",
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt,
        dag=dag
    )

    # TODO: Move the S3ToRedshiftOperator code here
    copy_table_task = S3ToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        dag=dag
    )

    # TODO: Move the check table code here
    check_table_task = HasRowsOperator(
        task_id=f"check_{table}_data",
        redshift_conn_id=redshift_conn_id,
        table=table,
        dag=dag
    )

    # TODO: Define ordering of tasks within this subdag
    create_table_task >> copy_table_task
    copy_table_task >> check_table_task

    return dag
