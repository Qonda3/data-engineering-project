from pyspark.sql import SparkSession
import os

def get_spark(app_name: str = "prepared_layer"):
    builder = (SparkSession.builder
               .appName(app_name)
               .config("spark.sql.shuffle.partitions", "200")
               .config("spark.sql.session.timeZone", "UTC"))
    if os.getenv("ENV", "local") == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    return spark
