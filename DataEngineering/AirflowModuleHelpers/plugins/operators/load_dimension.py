from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_key="",
                 aws_secret="",
                 table="",
                 sql_stmt="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Establish Redshift Postgres hook")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Clearing data from dimension table: {self.table}")
            redshift_hook.run(f"TRUNCATE TABLE public.{self.table}")

        self.log.info(f"Loading data from staging tables to {self.table}")
        redshift_hook.run(f"INSERT INTO public.{self.table} {self.sql_stmt}")
