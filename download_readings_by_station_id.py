import requests
from pyspark.sql.functions import col

def download_readings_data_by_id(spark, s3_client,
                                dest_bucket_name:str="",
                                key:str="")-> None:
    # Save a list of all station IDs
    list_of_station_ids = get_list_of_station_ids(spark, "s3://udacity-dend2-mogo/testing/full.json.gz")
    
    for id in list_of_station_ids :
        url = f"https://bulk.meteostat.net/v2/monthly/{id}.csv.gz"

        file_name = url.split("/")[-1]
        print(f"Downloading file >>>>> {file_name}")
        response = requests.get(url)

        s3_client.put_object(Bucket=dest_bucket_name,
                            Key=f"{key}/{file_name}",
                            Body=response.content)
        

def get_list_of_station_ids(spark, json_file, schema_object=None):
    """Read the json file containing data about each station. Return station ids

    Args:
        json_file (_str_): a string containing the path to the json file
        schema (_StructType_): a StructType object containing the schema (Optional)
    """
    df = spark.read.option("multiLine", True).json(json_file, schema=schema_object)
    
    # Filter for columns in stations dataframe I am interested in.
    focus_df = df.select("id", col("name.en").alias("english_name"), "country",
                "region", col("location.latitude").alias("latitude"), 
                col("location.longitude").alias("longitude"),
                col("location.elevation").alias("elevation"),
                "timezone", col("inventory.daily.start").alias("start"),
                col("inventory.daily.end").alias("end")
                )
    # Remove rows where 'start' and 'end' columns are null. Save in dataframe
    none_null_focus_df = focus_df.where(focus_df.start.isNotNull() \
                                    & focus_df.end.isNotNull())
    
    # Collect all station ids into a list
    station_ids = [data[0] for data in none_null_focus_df.select('id').collect()][0:10]
    
    return station_ids