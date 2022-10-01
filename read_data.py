import boto3
import os 


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
    prefix = os.environ['PREFIX']
    s3_res = boto3.resource('s3')
    data = s3_res.Bucket(bucket)
    objects = [object for object in data.objects.filter(Prefix=prefix)]

    return objects

