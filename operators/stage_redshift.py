from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

""" Initializing BaseOperator"""
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    """ Inserting copy command for redshift """
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS json 'auto'
        TIMEFORMAT as 'epochmillisecs';
    """
    
    """ Applying defaults to the initialized varibales"""
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region

    def execute(self, context):
        """This block will connect to AWS and redshift using aws credentials\
        furthermore, it will delete the data from tables before loading fresh data"""
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        """ This will load data to redshift"""
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )
        redshift.run(formatted_sql)

"""       
def execute(self, context):

    if not (self.file_format == 'csv' or self.file_format == 'json'):
        raise ValueError(f"file format {self.file_format} is not csv or json")
    if self.file_format == 'json':

        file_format = "format json '{}'".format(self.json_path)
    else:
        file_format = "format CSV"
    aws_hook = AwsHook(self.aws_conn_id)
    credentials = aws_hook.get_credentials()
    redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

    staging_to_redshift_sql = StageToRedshiftOperator.copysql.format(table_name = self.table_name,
        path = self.table_path,aws_key = credentials.access_key,aws_secret = credentials.secret_key,file_format =file_format)

    self.log.info("Clearing data from destination Redshift table")
    redshift.run("DELETE FROM {};".format(self.table_name))

    self.log.info(f'now loading {self.table_name} to redshift...')
    redshift.run(staging_to_redshift_sql)
    self.log.info(f'{self.table_name} loaded...')

"""





