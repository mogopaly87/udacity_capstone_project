from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.bash import BashOperator
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class DownloadSationDataOperator(BaseOperator):
    """Download and unzip station data

    Args:
        BaseOperator (_type_): _description_
    """
    
    @apply_defaults
    def __init__(self,
                station_ids:list,
                *args, 
                **kwargs):
        
        super(DownloadSationDataOperator, self).__init__(*args, **kwargs)
        self.station_ids = station_ids
        
        
    def execute(self, context):
        
        # list_of_station_id = ["01001", "00FAY"]   

        for id in self.station_ids :
            
            url2 = f"https://bulk.meteostat.net/v2/monthly/{id}.csv.gz"

            file_name = url2.split("/")[-1]
            self.log.info(f"Downloading file >>>>> {file_name}")
            
            with open(f"/tmp/downloads/{file_name}", "wb") as file:
                response = requests.get(url2)
                file.write(response.content)
