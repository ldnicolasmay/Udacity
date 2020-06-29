from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON AS '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_key="",
                 aws_secret="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_as="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_as = json_as

    def execute(self, context):
        self.log.info("Establishing Redshift Postgres hook")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from staging table: {self.table}")
        redshift_hook.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        self.log.info(f"SOURCE: s3://{self.s3_bucket}/{self.s3_key}")
        self.log.info(f"TARGET: {self.table} in Redshift {self.redshift_conn_id}")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.aws_key,
            self.aws_secret,
            self.json_as
        )

        self.log.info(f"START:  {datetime.utcnow().isoformat()}")
        redshift_hook.run(formatted_sql)
        self.log.info(f"FINISH: {datetime.utcnow().isoformat()}")
