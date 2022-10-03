from util import get_spark_session
from read_data import read
from transform_data import transform
import os
import configparser
import boto3


# source_dir = os.environ['S3_INPUT_SOURCE']
# destination_dir = os.environ['S3_OUTPUT_DESTINATION']
# bucket = os.environ['BUCKET']

s3_client = boto3.client('s3')
obj = s3_client.get_object(Bucket="udacity-dend2-mogo", Key="test_config.ini")

config = configparser.ConfigParser()
config.read_string(obj['Body'].read().decode())

bucket = config['S3']['BUCKET']
destination_dir = config['S3']['S3_OUTPUT_DESTINATION']
source_dir = config['S3']['S3_INPUT_SOURCE']

def main():
    
    spark = get_spark_session()
    s3_objects = read(bucket)
    
    transform(spark, s3_objects, source_dir, destination_dir)
    
    

if __name__ == "__main__":
    main()