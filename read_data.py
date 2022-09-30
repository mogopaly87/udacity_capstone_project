import boto3
import os 


def read(bucket):
    prefix = os.environ['PREFIX']
    s3_res = boto3.resource('s3')
    data = s3_res.Bucket(bucket)
    objects = [object for object in data.objects.filter(Prefix="raw_files")]

    return objects

