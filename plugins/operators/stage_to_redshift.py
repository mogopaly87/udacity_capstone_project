from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    """Load staging data from S3 to staging tables in Redshift.
    
    Operator to perform copy operations for any json file from
    S3 to RedShift
    """
    
    template_fields: tuple = ("s3_key",)
    copy_readings_sql = """
        COPY {} (year, month, tavg, tmin, tmax, prcp, wspd, pres, tsun, station_id)
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        CSV        
    """
    copy_stations_sql = """
        COPY {} (id, english_name, country, region, latitude, longitude, elevation, timezone, start, "end")
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        DELIMITER ',' ESCAPE
        removequotes;    
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        
    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Clearing data from destination Redshift table {self.table}')
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
        print("S3 path >>>", s3_path)
        sql_to_use = ""
        
        if self.table == "staging_readings":
            sql_to_use = StageToRedshiftOperator.copy_readings_sql
        elif self.table == "staging_station":
            sql_to_use = StageToRedshiftOperator.copy_stations_sql
            
        formatted_sql = sql_to_use.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )
        print("printing formatted sql >>>> ",formatted_sql)
        redshift.run(formatted_sql)