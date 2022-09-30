from util import get_spark_session
from read_data import read
from transform_data import transform
import os

source_dir = os.environ['S3_INPUT_SOURCE']
destination_dir = os.environ['S3_OUTPUT_DESTINATION']
bucket = os.environ['BUCKET']
# TO DO
# 1. Create Spark schema for "df_with_new_col" dataframe before writing it to the
#    'clean_data' S3 bucket
        
        

# TO DO
# 1. Use schema created for "df_with_new_col" dataframe before writing it to the
#    'clean_data' S3 bucket



def main():
    
    spark = get_spark_session()
    s3_objects = read(bucket)
    
    transform(spark, s3_objects, source_dir, destination_dir)
    
    

if __name__ == "__main__":
    main()