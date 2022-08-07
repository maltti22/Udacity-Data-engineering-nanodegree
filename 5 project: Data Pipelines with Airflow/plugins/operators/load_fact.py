from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 sql_statement = "",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.sql_statement=sql_statement
        
        

    def execute(self, context):
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Loading fact table')
        insert_query = 'INSERT INTO {} ({})'.format(self.destination_table, self.sql_statement)
        redshift.run(insert_query)
        self.log.info(f"destination_table {self.destination_table}")
        records = redshift.get_records(f"select top 10 * from {self.destination_table}")
        for row in records:
            self.log.info(row)