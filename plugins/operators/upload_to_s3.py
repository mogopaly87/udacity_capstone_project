from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class UploadToS3Operator(BaseOperator):
    
    """Upload files to Amazon S3
    """
    
    @apply_defaults
    def __init__(self,
                    aws_conn_id="",
                    key="",
                    bucket_name="",
                    source_dir="",
                    *args, **kwargs):
        
        super(UploadToS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.key = key
        self.bucket_name = bucket_name
        self.source_dir = source_dir
        
        
    
    def execute(self, context):
        
        for f in os.listdir(self.source_dir):
            
            file_name = os.path.join(self.source_dir, f)
            
            if os.path.isfile(file_name):
                s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
                s3_hook.load_file(
                filename=file_name,
                key=f"{self.key}/{file_name.split('/')[-1]}",
                bucket_name=self.bucket_name
            )
        