from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 query_fact = "",
                 conn_id = "",
                 table = "",
                 operation_type="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query_fact = query_fact
        self.table = table
        self.operation_type = operation_type
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        try:
            if self.operation_type == 2:
                self.log.info(f"Delete data from fact table {self.table}")
                redshift_hook.run(f"DELETE FROM {self.table}")
            
            self.log.info(f"Insert data to fact table {self.table}")
            query_insert = "INSERT INTO {} \n {}".format(self.table,self.query_fact)
            redshift_hook.run(query_insert)
            self.log.info(f"Insert data to fact table {self.table} finished with success") 
        except Exception as m:
            self.log.info(f"[Error-loading into fact table {self.table}]: {m}")
