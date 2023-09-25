from pyspark import SparkContext

def create_spark_context(master, app_name):
    sc = SparkContext(master, app_name)
    return sc

