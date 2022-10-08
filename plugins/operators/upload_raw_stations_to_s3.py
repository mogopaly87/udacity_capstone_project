from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import boto3
import requests


class UploadToS3Operator(BaseOperator):
    
    """Upload files to Amazon S3
    """
    
    @apply_defaults
    def __init__(self,
                    aws_conn_id="",
                    key="",
                    dest_bucket_name="",
                    region_name="",
                    aws_access_key_id="",
                    aws_secret_access_key="",
                    *args, **kwargs):
        
        super(UploadToS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.key = key
        self.dest_bucket_name = dest_bucket_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        
        
    
    def execute(self, context):
        
        s3_client = boto3.client('s3', region_name=self.region_name,
                            aws_access_key_id = self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key)
        
        url = "https://bulk.meteostat.net/v2/stations/full.json.gz"
        file_name = url.split("/")[-1]
        response = requests.get(url)
        
        check_result = s3_client.list_objects_v2(Bucket=f"{self.dest_bucket_name}", Prefix=f"{self.key}/{file_name}")
        
        if 'Contents' in check_result:
            self.log.error(f"Stations file {file_name} already exists!\nUpload unsuccessful")
        else:
            s3_client.put_object(Bucket=self.dest_bucket_name,
                                Key=f"{self.key}/{file_name}",
                                Body=response.content)
        