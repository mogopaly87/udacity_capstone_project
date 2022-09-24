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
from operators.download_unzip_station_data import downloadSationDataOperator



# A function that returns a Spark object
def get_spark_object():
    spark = SparkSession.builder \
    .config("spark.sql.catalogImplementation","in-memory") \
    .getOrCreate()
    
    return spark


# A function that reads any json file with the option of passing a schema
def json_to_dataframe(json_file, schema_object=None):
    """Read json and return dataframe

    Args:
        json_file (_str_): a string containing the path to the json file
        schema (_StructType_): a StructType object containing the schema (Optional)
    """
    spark = get_spark_object()
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
    
    station_ids = [data[0] for data in none_null_focus_df.select('id').collect()]
    return station_ids



def load_to_s3():
    """Load station data to raw bucket
    """
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    s3_hook.load_file(
        filename=f"dags/station.json",
        key=f"testing/station3.json",
        bucket_name="udacity-dend2-mogo"
    )
    
    
def upload_station_data_to_s3():
    
    pass


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
    
    download_station_data_to_local = downloadSationDataOperator(
        task_id="api_to_local",
        station_ids=get_all_station_id_as_list())
    
    # unzip_files = BashOperator(
    #     task_id="unzip_files",
    #     cwd="/tmp/downloads/",
    #     bash_command=""
    # )
