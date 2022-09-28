from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.bash import BashOperator
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3


class DownloadSationDataOperator(BaseOperator):
    """Download station data from Meteostat Developers
    Daily Data API directry into staging S3 Bucket

    Args:
        BaseOperator (_type_): _description_
    """
    
    @apply_defaults
    def __init__(self,
                station_ids:list,
                region_name="",
                aws_access_key_id="",
                aws_secret_access_key="",
                dest_bucket_name="",
                key="",
                *args, 
                **kwargs):
        
        super(DownloadSationDataOperator, self).__init__(*args, **kwargs)
        self.station_ids = station_ids
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.dest_bucket_name = dest_bucket_name
        self.key = key
        
    def execute(self, context):  

        s3_client = boto3.client('s3', region_name=self.region_name,
                            aws_access_key_id = self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key)

        for id in self.station_ids :
            url2 = f"https://bulk.meteostat.net/v2/monthly/{id}.csv.gz"

            file_name = url2.split("/")[-1]
            self.log.info(f"Downloading file >>>>> {file_name}")
            response = requests.get(url2)

            s3_client.put_object(Bucket=self.dest_bucket_name,
                                Key=f"{self.key}/{file_name}",
                                Body=response.content)
