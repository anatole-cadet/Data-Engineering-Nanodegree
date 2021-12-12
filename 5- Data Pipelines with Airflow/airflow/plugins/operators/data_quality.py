from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#from plugins.helpers.sql_queries import SqlQueries
from helpers.sql_queries import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 query_check="",
                 conn_id="",
                 data_quality_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.query_check = query_check
        self.conn_id = conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        list_results = []
        
        self.log.info("Data quality start")

        redshift_hook = PostgresHook(self.conn_id)
        for e in self.data_quality_checks:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {e['table']}")

            if ((records[0][0] > 0) != (e['expected_result'])):
                self.log.info(f"Data checked not valid : {e['table']}")
                list_results.append(0)
                
        if 0 in list_results:
            self.log.info("Data loaded are not valid for one or more tables")
        else:
            self.log.info(" ***** Data quality checked finished with success********")