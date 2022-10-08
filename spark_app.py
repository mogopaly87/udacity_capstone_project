from util import get_spark_session
from read_data import read
from transform_data import (transform_write_clean_readings,
                            write_clean_station_data_to_s3)
from download_readings_by_station_id import download_readings_data_by_id, get_focused_station_data_df
import os
import configparser
import boto3


s3_client = boto3.client('s3')
obj = s3_client.get_object(Bucket="udacity-dend2-mogo", Key="test_config.ini")

config = configparser.ConfigParser()
config.read_string(obj['Body'].read().decode())

bucket = config['S3']['BUCKET']
destination_dir = config['S3']['S3_OUTPUT_DESTINATION']
source_dir = config['S3']['S3_INPUT_SOURCE']
spark = get_spark_session()


def main():
    
    focused_df = get_focused_station_data_df(spark, "s3://udacity-dend2-mogo/testing/full.json.gz")

    # Download readings for each station ID in the list_of_station_ids and save in s3 bucket
    # s3://udacity-dend2-mogo/raw_files/
    download_readings_data_by_id(focused_df, 
                                s3_client, 
                                "udacity-dend2-mogo",
                                "raw_files")
    s3_objects = read(bucket)
    
    transform_write_clean_readings(spark, s3_objects, source_dir, destination_dir)
    write_clean_station_data_to_s3(focused_df, destination_dir)
    

if __name__ == "__main__":
    main()