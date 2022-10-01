from pyspark.sql.functions import lit


def transform(spark, s3_objects:list, source_dir:str, destination_dir:str) -> None:
    """Transforms a list of S3 bucket objects and writes data as csv to 
    designated 'clean' output S3 bucket.
    

    Args:
        spark (SparkSession): A Spark Session instance
        s3_objects (list): A list containing S3 bucket objects to be transformed
        source_dir (str): S3 bucket source of files in .gz format to be transformed. 
        E.g.: 's3://<bucket_name>/<folder>'
        destination_dir (str): S3 bucket destination of cleaned output files. 
        E.g.: 's3://<bucket_name>/<folder>'
    """
    
    col_names = ["year", "month", "tavg", "tmin", "tmax", "prcp", "wspd",
                    "pres", "tsun"]
    
    for my_objs in s3_objects:
        # Iterate over list of S3 objects to extract the file names of each object
        file_name_gz = my_objs.key.split("/")[-1]
        if file_name_gz.endswith(".gz"):        
            file_name_csv = file_name_gz.split(".")
            file_name_csv = "{0}{1}".format(file_name_csv[0], file_name_csv[1])
            
            df = spark.read.csv("{0}/{1}".format(source_dir, file_name_gz))
            df_with_headers = df.toDF(*col_names)
            
            df_with_new_col = df_with_headers.withColumn("station_id", lit("{0}".format(file_name_csv)))
            
            # TO DO
            # 1. Create Spark schema for "df_with_new_col" dataframe before writing it to the
            #    'clean_data' S3 bucket

            # 2. Use schema created for "df_with_new_col" dataframe before writing it to the
            #    'clean_data' S3 bucket
            
            df_with_new_col \
                        .coalesce(16) \
                        .write \
                        .partitionBy('year', 'station_id') \
                        .option("header", True) \
                        .csv("{0}/{1}" \
                        .format(destination_dir, file_name_csv))