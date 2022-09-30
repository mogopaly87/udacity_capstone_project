import requests, json, boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit



spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()


s3_res = boto3.resource('s3', region_name='us-east-1',
                        aws_access_key_id="AKIA57FIZFN26IJZ2RFX",
                        aws_secret_access_key="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")


data = s3_res.Bucket("udacity-dend2-mogo")

col_names = ["year", "month", "tavg", "tmin", "tmax", "prcp", "wspd",
                "pres", "tsun"]
# TO DO
# 1. Create Spark schema for "df_with_new_col" dataframe before writing it to the
#    'clean_data' S3 bucket
        
        
for my_objs in data.objects.filter(Prefix="raw_files"):
    # if my_objs.key.endswith(".json"):
    file_name_gz = my_objs.key.split("/")[-1]
    if file_name_gz.endswith(".gz"):        
        file_name_csv = file_name_gz.split(".")
        file_name_csv = "{0}{1}".format(file_name_csv[0], file_name_csv[1])
        # if len(file_name_csv) > 1:
        df = spark.read.csv("s3://udacity-dend2-mogo/raw_files/{0}".format(file_name_gz))
        df_with_headers = df.toDF(*col_names)
        
        df_with_new_col = df_with_headers.withColumn("station_id", lit("{0}".format(file_name_csv)))

        
        # TO DO
        # 1. Use schema created for "df_with_new_col" dataframe before writing it to the
        #    'clean_data' S3 bucket
        
        df_with_new_col \
                        .coalesce(16) \
                        .write \
                        .partitionBy('year', 'station_id') \
                        .option("header", True) \
                        .csv("s3://udacity-dend2-mogo/clean_data/{0}" \
                        .format(file_name_csv))