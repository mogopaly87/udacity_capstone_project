from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers.sql_queries import SqlQueries


class ValidateRedshiftOperator(BaseOperator):
    """Run validation queries for data quality

    Args:
        BaseOperator (ABS): Base operator
    """
    
    # template_fields: tuple = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                *args, 
                **kwargs):
        super(ValidateRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
    
    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        conn = redshift.get_conn()
        cursor = conn.cursor()
        cursor.execute(SqlQueries.test_stations_for_empty.format(self.table))
        if len(cursor.fetchall()) > 1:
            self.log.info("The table was poplulated correctly")
        else:
            raise ValueError(f"The {self.table} table is empty.")
        

class ValidateNullOperator(BaseOperator):
    """Run validation queries to check that primary key is not null

    Args:
        BaseOperator (ABS): Base operator
    """
    
    
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                *args, 
                **kwargs):
        super(ValidateNullOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        
    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        conn = redshift.get_conn()
        cursor = conn.cursor()
        cursor.execute(SqlQueries.test_stations_for_null.format(self.table))
        if len(cursor.fetchall()) > 1:
            raise ValueError(f"""PASSED: The primary key column of table {self.table} 
                            contains null value""")
        else:
            self.log.info(f"Primary key column passed integrity test ")