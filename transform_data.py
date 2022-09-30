from pyspark.sql.functions import lit


def transform(spark, s3_objects:list, source_dir, destination_dir):
    
    col_names = ["year", "month", "tavg", "tmin", "tmax", "prcp", "wspd",
                    "pres", "tsun"]
    
    for my_objs in s3_objects:
        file_name_gz = my_objs.key.split("/")[-1]
        if file_name_gz.endswith(".gz"):        
            file_name_csv = file_name_gz.split(".")
            file_name_csv = "{0}{1}".format(file_name_csv[0], file_name_csv[1])
            
            df = spark.read.csv("{0}/{1}".format(source_dir, file_name_gz))
            df_with_headers = df.toDF(*col_names)
            
            df_with_new_col = df_with_headers.withColumn("station_id", lit("{0}".format(file_name_csv)))
            
            df_with_new_col \
                        .coalesce(16) \
                        .write \
                        .partitionBy('year', 'station_id') \
                        .option("header", True) \
                        .csv("{0}/{1}" \
                        .format(destination_dir, file_name_csv))