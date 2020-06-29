from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_key="",
                 aws_secret="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        self.log.info("Establishing Redshift Postgres hook")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading data from staging tables to {self.table}")
        redshift_hook.run(f"INSERT INTO public.{self.table} {self.sql_stmt}")
