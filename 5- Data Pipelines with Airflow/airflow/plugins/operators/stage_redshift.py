from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


from helpers.sql_queries import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto'
        EMPTYASNULL
        BLANKSASNULL
        """


    @apply_defaults
    def __init__(self,
                 s3_bucket          = "",
                 s3_key             = "",
                 redshift_conn_id   = "",
                 aws_credentials_id  = "",
                 table              = "", 
                 query              = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket= s3_bucket
        self.s3_key             = s3_key
        self.redshift_conn_id   = redshift_conn_id
        self.aws_credentials_id  = aws_credentials_id
        self.table              = table
        self.query              = query


    def execute(self, context):
        query_obj = SqlQueries()
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials_data = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")
        self.staging_s3_to_redshift(self.table,credentials_data.access_key,credentials_data.secret_key)
                
    
    def staging_s3_to_redshift(self,table,access_key,secret_key):
        self.log.info(f"Staging data from s3 to Amazon Redshift according to the table {table} ")
        redshift_hook = PostgresHook("redshift")
        try:
            sql_smt =  self.COPY_SQL.format(table,self.s3_key,access_key,secret_key)
            redshift_hook.run(sql_smt)
            self.log.info(f"Staging data for table {table} finished with success")
        except Exception as m:
            self.log.info(f"[Error:stagging {table}] :{m} ")





