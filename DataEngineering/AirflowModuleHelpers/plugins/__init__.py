from airflow.plugins_manager import AirflowPlugin

# import DataEngineering.AirflowModuleHelpers.plugins.operators
# import operators
from operators.has_rows import *
from operators.s3_to_redshift import *
from operators.facts_calculator import *

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        HasRowsOperator,
        S3ToRedshiftOperator,
        FactsCalculatorOperator
    ]
