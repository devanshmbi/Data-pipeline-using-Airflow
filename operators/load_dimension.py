from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

""" Initializing BaseOperator"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.query=query

    def execute(self, context):
        connection=PostgresHook(self.redshift_conn_id).get_conn()
        hook_cursor=connection.cursor()
        hook_cursor.execute(self.query)
        connection.commit()
        self.log.info('LoadDimensionOperator Ran Sucessfully')

"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_dimension_table_insert = """
    INSERT INTO {} {}
    """
    load_dimension_table_truncate = """
    TRUNCATE TABLE {} 
    """

    @apply_defaults
    def __init__(self,
             query="",
             redshift_conn_id="",
             t_name="",
             operation="",
             *args, **kwargs):

    super(LoadDimensionOperator, self).__init__(*args, **kwargs)
    self.query=query
    self.redshift_conn_id=redshift_conn_id
    self.t_name=t_name
    self.operation=operation

    def execute(self, context):
    self.log.info(f"Started LoadDimensionOperator {self.t_name} started with mode {self.operation} ")
    redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

    if(self.operation == "append"):
        redshift_hook.run(LoadDimensionOperator.load_dimension_table_insert.format(self.t_name, self.query)) 
    if(self.operation == "truncate"):
        redshift_hook.run(LoadDimensionOperator.load_dimension_table_truncate.format(self.t_name)) 
        redshift_hook.run(LoadDimensionOperator.load_dimension_table_insert.format(self.t_name, self.query)) 
    self.log.info(f"Ending LoadDimensionOperator {self.t_name} with a Success on Operation  {self.operation}")


"""