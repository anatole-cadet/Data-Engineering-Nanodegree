from os import name
from airflow.plugins_manager import AirflowPlugin
from sqlalchemy.sql import operators
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator


class UdacityProjectsPlugins(AirflowPlugin):
    name="project_udacity_plugins"
    __all__ = [
        'StageToRedshiftOperator',
        'LoadFactOperator',
        'LoadDimensionOperator',
        'DataQualityOperator',
    ]
