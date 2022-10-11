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
raw_readings_data = config['S3']['S3_RAW_READINGS_INPUT']
clean_readings_data = config['S3']['S3_CLEAN_READINGS_OUTPUT']
raw_station_data = config['S3']['S3_RAW_STATION_INPUT']
clean_station_data = config['S3']['S3_CLEAN_STATION_OUTPUT']
key = config['S3']['PREFIX']
spark = get_spark_session()


def main():
    
    focused_df = get_focused_station_data_df(spark, raw_station_data)

    # Download readings for each station ID in the list_of_station_ids and save in s3 bucket
    # s3://udacity-dend2-mogo/raw_files/
    # download_readings_data_by_id(focused_df, 
    #                             s3_client, 
    #                             bucket,
    #                             key)
    s3_objects = read(bucket)
    
    transform_write_clean_readings(spark, s3_objects, raw_readings_data, clean_readings_data)
    # write_clean_station_data_to_s3(focused_df, clean_station_data)
    

if __name__ == "__main__":
    main()