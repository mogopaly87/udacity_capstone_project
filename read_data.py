import boto3
import os 
import configparser


s3_client = boto3.client('s3')
obj = s3_client.get_object(Bucket="udacity-dend2-mogo", Key="test_config.ini")
config = configparser.ConfigParser()
config.read_string(obj['Body'].read().decode())
prefix = config['S3']['PREFIX']

def read(bucket):
    """Reads and returns list of S3 bucket objects.
    Takes one argument of an S3 bucket name, uses Boto3 resource API to
    retrieve objects of specified S3 bucket and prefix.
    The Prefix is gotten from an environment variable 'PREFIX'.

    Args:
        bucket (S3 Object): An S3 bucket object

    Returns:
        list: List of S3 bucket objects
    """
    s3_res = boto3.resource('s3')
    data = s3_res.Bucket(bucket)
    objects = [object for object in data.objects.filter(Prefix=prefix)]

    return objects

