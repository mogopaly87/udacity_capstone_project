import pandas as pd
import json, datetime, requests
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import explode, col
from pyspark.sql import SparkSession
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import airflow.utils.dates as dates
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from operators.upload_stations_to_s3 import UploadToS3Operator
import boto3
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor



SPARK_TASK = [
    {
        'Name': 'spark_app',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ["spark-submit", "--deploy-mode", "client", "/home/hadoop/spark_app.py"],
        },
    }
]


# Retrieve AWS credentials from Airflow
aws_access_key_id = Variable.get("access_key_id")
aws_secret_access_key = Variable.get("secret_access_key")



with DAG(
    dag_id="upload_example",
    schedule_interval=None,
    start_date=datetime.now()
) as dag:
    
    
        load_station_reading_to_s3 = UploadToS3Operator(
        task_id="load_station",
        aws_conn_id="aws_default",
        key="testing",
        dest_bucket_name="udacity-dend2-mogo",
        region_name="us-east-1",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


        clean_to_csv = EmrAddStepsOperator(
            task_id = "clean_to_csv",
            job_flow_id = "j-2OOK3880THXLV",
            aws_conn_id = "aws_default",
            steps = SPARK_TASK
        )

    # step_second = EmrStepSensor(
    #     task_id='watch_emr_step',
    #     job_flow_id="aws_default",
    #     step_id="{{ task_instance.xcom_pull(task_ids='clean_to_csv', key='return_value')[0] }}",
    #     aws_conn_id='aws_default',
    # )
    
    
load_station_reading_to_s3 >> clean_to_csv
    


