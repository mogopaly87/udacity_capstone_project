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



# A function that returns a Spark object
spark = (SparkSession.builder \
    .config("spark.sql.catalogImplementation","in-memory") \
    .getOrCreate())


# A function that reads any json file with the option of passing a schema
def json_to_dataframe(json_file, schema_object=None):
    """Read json and return dataframe

    Args:
        json_file (_str_): a string containing the path to the json file
        schema (_StructType_): a StructType object containing the schema (Optional)
    """
    df = spark.read.option("multiLine", True).json(json_file, schema=schema_object)
    
    return df

def get_all_station_id_as_list():
    
    df = json_to_dataframe("dags/station.json")

    # Filter for columns in stations dataframe I am interested in.
    focus_df = df.select("id", col("name.en").alias("english_name"), "country",
                "region", col("location.latitude").alias("latitude"), 
                col("location.longitude").alias("longitude"),
                col("location.elevation").alias("elevation"),
                "timezone", col("inventory.daily.start").alias("start"),
                col("inventory.daily.end").alias("end")
                )
    none_null_focus_df = focus_df.where(focus_df.start.isNotNull() & focus_df.end.isNotNull())
    
    station_ids = [data[0] for data in none_null_focus_df.select('id').collect()][0:10]
    return station_ids


    
def transform_to_clean_s3():
    """_summary_
    """   


with DAG(
    dag_id="upload_example",
    schedule_interval="@daily",
    start_date=datetime.now()
) as dag:
    
    # task = PythonOperator(
    #     task_id="load_to_s3",
    #     python_callable=load_to_s3
    # )
    
    download_station_data_to_local = DownloadSationDataOperator(
        task_id="api_to_local",
        station_ids=get_all_station_id_as_list())
    
    unzip_files = BashOperator(
        task_id="unzip_files",
        cwd="/tmp/downloads/",
        bash_command="gunzip *.gz"
    )
    
    load_station_reading_to_s3 = UploadToS3Operator(
        task_id="load_station",
        aws_conn_id="aws_conn_id",
        key="testing",
        bucket_name="udacity-dend2-mogo",
        source_dir="/tmp/downloads/"
    )
    

download_station_data_to_local >> unzip_files >> load_station_reading_to_s3