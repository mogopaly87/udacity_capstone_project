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
from operators.download_unzip_station_data import DownloadSationDataOperator 
from operators.upload_to_s3 import UploadToS3Operator
import boto3
from airflow.models import Variable



# A function that returns a Spark object
spark = (SparkSession.builder \
    .config("spark.sql.catalogImplementation","in-memory") \
    .getOrCreate())

# Retrieve AWS credentials from Airflow
aws_access_key_id = Variable.get("access_key_id")
aws_secret_access_key = Variable.get("secret_access_key")


# A function that reads any json file with the option of passing a schema
def json_to_dataframe(json_file, schema_object=None):
    """Read json and return dataframe

    Args:
        json_file (_str_): a string containing the path to the json file
        schema (_StructType_): a StructType object containing the schema (Optional)
    """
    df = spark.read.option("multiLine", True).json(json_file, schema=schema_object)
    
    return df


def get_useful_columns_of_station_data():
    """Return only columns of interest to this project

    Returns:
        dataframe: Returns a Spark DataFrame of columns of interest to this project
    """
    df = json_to_dataframe("dags/station.json")

    # Filter for columns in stations dataframe I am interested in.
    focus_df = df.select("id", col("name.en").alias("english_name"), "country",
                "region", col("location.latitude").alias("latitude"), 
                col("location.longitude").alias("longitude"),
                col("location.elevation").alias("elevation"),
                "timezone", col("inventory.daily.start").alias("start"),
                col("inventory.daily.end").alias("end")
                )    
    
    return focus_df


def get_none_null_columns_station_data_df():
    """Returns useful columns where the 'start' and 'end' columns have no null values.

    Returns:
        dataframe: Returns a Spark DataFrame
    """
    useful_col_df = get_useful_columns_of_station_data()
    none_null_focus_df = useful_col_df.where(useful_col_df.start.isNotNull() \
                                    & useful_col_df.end.isNotNull())
    
    return none_null_focus_df
    

def get_station_ids_as_list()->list:
    """Returns a list of station IDs

    Returns:
        list: Returns a list containing station IDs of all stations that do not have null
        values in both 'start' and 'end' columns
    """
    none_null_columns = get_none_null_columns_station_data_df()
    station_ids = [data[0] for data in none_null_columns.select('id').collect()][0:10]
    
    return station_ids


def transform_to_clean_s3():
    """_summary_
    """   
    access_key_id = Variable('access_key_id')
    secret_access_key_id = Variable('secret_access_key')

    s3 = boto3.resource('s3', region='us-east-1',
                        aws_access_key_id=access_key_id,
                        aws_secret_access_key=secret_access_key_id)


with DAG(
    dag_id="upload_example",
    schedule_interval="@daily",
    start_date=datetime.now()
) as dag:
    
    
    # task = PythonOperator(
    #     task_id="load_to_s3",
    #     python_callable=load_to_s3
    # )
    

    
    download_station_data_to_s3 = DownloadSationDataOperator(
        task_id="api_to_local",
        station_ids=get_station_ids_as_list(),
        region_name="us-east-1",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        dest_bucket_name="udacity-dend2-mogo",
        key="raw_files")
    
    # unzip_files = BashOperator(
    #     task_id="unzip_files",
    #     cwd="/tmp/downloads/",
    #     bash_command="gunzip *.gz"
    # )
    
    # load_station_reading_to_s3 = UploadToS3Operator(
    #     task_id="load_station",
    #     aws_conn_id="aws_conn_id",
    #     key="testing",
    #     bucket_name="udacity-dend2-mogo",
    #     source_dir="/tmp/downloads/"
    # )
    

# download_station_data_to_s3 >> unzip_files >> load_station_reading_to_s3