# from __future__ import division, absolute_import, print_function
# 
# from airflow.plugins_manager import AirflowPlugin
#
# from operators.has_rows import *
# from operators.s3_to_redshift import *
# from operators.facts_calculator import *
#
# # Defining the plugin class
# class UdacityPlugin(AirflowPlugin):
#     name = "udacity_plugin"
#     operators = [
#         HasRowsOperator,
#         S3ToRedshiftOperator,
#         FactsCalculatorOperator
#     ]

from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
    
import operators
import helpers

# Define the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.HasRowsOperator,
        operators.S3ToRedshiftOperator,
        operators.FactsCalculatorOperator,
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]

