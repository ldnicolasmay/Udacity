import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FactsCalculatorOperator(BaseOperator):

    facts_sql_template = """
    drop table if exists {destination_table};
    create table {destination_table} as
    select
        {groupby_column},
        max({fact_column}) as max_{fact_column},
        min({fact_column}) as min_{fact_column},
        avg({fact_column}) as average_{fact_column}
    from {origin_table}
    group by {groupby_column};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 origin_table="",
                 destination_table="",
                 fact_column="",
                 groupby_column="",
                 *args, **kwargs):
        super(FactsCalculatorOperator, self).__init__(*args, **kwargs)

        # TODO: Set attributes from __init__ instantiation arguments
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table
        self.destination_table = destination_table
        self.fact_column = fact_column
        self.groupby_column = groupby_column

    def execute(self, context):
        # TODO: Fetch the Redshift hook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # TODO: Format the `facts_sql_template` and run the query against Redshift
        formatted_facts_sql = FactsCalculatorOperator.facts_sql_template.format(
            origin_table=self.origin_table,
            destination_table=self.destination_table,
            fact_column=self.fact_column,
            groupby_column=self.groupby_column
        )
        redshift_hook.run(formatted_facts_sql)
