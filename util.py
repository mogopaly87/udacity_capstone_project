from pyspark.sql import SparkSession

def get_spark_session() -> SparkSession:
    """Gets a Spark Session instance.\n
    Instantiates and returns a Spark Session instance.

    Returns:
        SparkSession: Spark Session instance
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    return spark