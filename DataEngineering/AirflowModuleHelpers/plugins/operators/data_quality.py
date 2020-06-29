from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        self.log.info("Establish Redshift Postgres hook")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running data quality checks")
        for check in self.data_quality_checks:
            self.log.info(f"Running check: {check}")
            result = redshift_hook.get_records(check["test_sql"])
            if result != check["expected_result"]:
                raise ValueError(f"Data quality check failed: {check['test_sql']}; " +
                                 f"expected {check['expected_result']}; returned {result}")
