from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table= "",
                 query_dim = "",
                 conn_id="",
                 operation_type="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table =table
        self.query_dim = query_dim
        self.conn_id = conn_id
        self.operation_type = operation_type

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift_hook = PostgresHook(self.conn_id)
        try:
            if self.operation_type == 2:
                self.log.info(f"Truncate data of dimension table {self.table}")
                redshift_hook.run(f"TRUNCATE TABLE {self.table}")       

            self.log.info(f"Insert data to dimension table {self.table}")         
            query_insert = "INSERT INTO {} \n {}".format(self.table,self.query_dim)
            redshift_hook.run(query_insert)
            self.log.info(f"Insert data to dimension table {self.table} finished with success") 
        except Exception as m:
            self.log.info(f"[Error-loading into dimension table {self.table}]: {m}")
