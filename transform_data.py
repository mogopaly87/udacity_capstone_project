from pyspark.sql.functions import lit


def transform_write_clean_readings(spark, s3_objects:list, source_dir:str, destination_dir:str) -> None:
    """Transforms a list of S3 bucket objects and writes data as csv to 
    designated 'clean' output S3 bucket.
    

    Args:
        spark (SparkSession): A Spark Session instance\n
        s3_objects (list): A list containing S3 bucket objects to be transformed
        source_dir (str): S3 bucket source of files in .gz format to be transformed - e.g.,
        's3://<bucket_name>/<folder>'\n
        destination_dir (str): S3 bucket destination of cleaned output files.
        e.g. 's3://<bucket_name>/<folder>'
    """
    
    
    
    for my_objs in s3_objects:
        # Iterate over list of S3 objects to extract the file names of each object
        file_name_gz = my_objs.key.split("/")[-1]
        if file_name_gz.endswith(".gz"):        
            file_name_csv = file_name_gz.split(".")
            file_name_csv = "{0}".format(file_name_csv[0])
            
            df = spark.read.csv("{0}/{1}".format(source_dir, file_name_gz))
            col_names = ["year", "month", "tavg", "tmin", "tmax", "prcp", "wspd",
                    "pres", "tsun"]
            df_with_headers = df.toDF(*col_names)
            
            # Enforce data types for each column
            changedTypes = df_with_headers.withColumn("year", df_with_headers["year"].cast("int")) \
                    .withColumn("month", df_with_headers["month"].cast("int")) \
                    .withColumn("tavg", df_with_headers["tavg"].cast("float")) \
                    .withColumn("tmin", df_with_headers["tmin"].cast("float")) \
                    .withColumn("tmax", df_with_headers["tmax"].cast("float")) \
                    .withColumn("prcp", df_with_headers["prcp"].cast("float")) \
                    .withColumn("wspd", df_with_headers["wspd"].cast("float")) \
                    .withColumn("pres", df_with_headers["pres"].cast("float")) \
                    .withColumn("tsun", df_with_headers["tsun"].cast("int"))
            
            df_with_new_col = changedTypes.withColumn("station_id", lit("{0}".format(file_name_csv)))
                        
            df_with_new_col \
                        .coalesce(16) \
                        .write \
                        .option("header", True) \
                        .csv("{0}/{1}" \
                        .format(destination_dir, file_name_csv))
                        


def write_clean_station_data_to_s3(focused_dataframe, output:str):
    """Receives focused stations data dataframe and writes it to
    specified S3 bucket

    Args:
        focused_dataframe (Spark DataFrame): a spark dataframe
    """
    focused_dataframe.coalesce(16) \
    .write \
    .option("header", True) \
    .csv("{0}/{1}" \
    .format(output, "station.csv"))