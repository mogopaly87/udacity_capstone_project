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
from operators.upload_raw_stations_to_s3 import UploadToS3Operator
from operators.stage_to_redshift import StageToRedshiftOperator
from operators.validation_operator import ValidateRedshiftOperator, ValidateNullOperator
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
emr_id = Variable.get("emr_id")
raw_station_data = Variable.get("raw_station_data_key")
clean_readings_data = Variable.get("clean_readings_data_key")
clean_station_data = Variable.get("clean_station_data_key")
my_s3_bucket = Variable.get("my_s3_bucket")



with DAG(
    dag_id="upload_example",
    schedule_interval=None,
    start_date=datetime.now()
) as dag:
    
        load_station_reading_to_s3 = UploadToS3Operator(
        task_id="load_station",
        aws_conn_id="aws_default",
        key=raw_station_data,
        dest_bucket_name=my_s3_bucket,
        region_name="us-east-1",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        )


        clean_to_csv = EmrAddStepsOperator(
            task_id = "clean_to_csv",
            job_flow_id = emr_id,
            aws_conn_id = "aws_default",
            steps = SPARK_TASK
        )

        watch_emr_step = EmrStepSensor(
            task_id='watch_emr_step',
            job_flow_id=emr_id,
            step_id="{{ task_instance.xcom_pull(task_ids='clean_to_csv', key='return_value')[0] }}",
            aws_conn_id='aws_default',
        )
        
        stage_readings_to_redshift = StageToRedshiftOperator(
        task_id='stage_readings_to_redshift',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_default",
        s3_bucket=my_s3_bucket,
        s3_key=clean_readings_data,
        table="staging_readings",
        )
        
        stage_station_data_to_redshift = StageToRedshiftOperator(
        task_id='stage_station_data_to_redshift',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_default",
        s3_bucket=my_s3_bucket,
        s3_key=clean_station_data,
        table="staging_station",
        )
        
        validate_station_count = ValidateRedshiftOperator(
        task_id='validate_station_count',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_default",
        table="staging_station"
        )
        
        validate_station_primary_key = ValidateNullOperator(
        task_id='validate_station_primary_key',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_default",
        table="staging_station"
        )
        
        validate_readings_count = ValidateRedshiftOperator(
        task_id='validate_readings_count',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_default",
        table="staging_readings"
        )
        
    
    
load_station_reading_to_s3 >> clean_to_csv >> watch_emr_step >> stage_readings_to_redshift >> stage_station_data_to_redshift

stage_station_data_to_redshift >> [validate_station_count, validate_station_primary_key, validate_readings_count]
