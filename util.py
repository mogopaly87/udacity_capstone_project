from pyspark.sql import SparkSession

def get_spark_session() -> SparkSession:
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    
    return spark