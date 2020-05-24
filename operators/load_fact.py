from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

""" Initializing BaseOperator"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.query=query

    def execute(self, context):
        connection=PostgresHook(self.redshift_conn_id).get_conn()
        hook_cursor=connection.cursor()
        hook_cursor.execute(self.query)
        connection.commit()
        self.log.info('LoadFactOperator Ran Sucessfully')
