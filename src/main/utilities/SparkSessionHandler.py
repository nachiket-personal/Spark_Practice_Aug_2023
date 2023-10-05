from pyspark.sql import SparkSession

def create_spark_session(app_name, master):
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
    return spark