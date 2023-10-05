from pyspark.sql.types import StructType, StructField, StringType

from src.main.utilities.SparkSessionHandler import create_spark_session

spark = create_spark_session("EmptyDataframeExample", "local[2]")

empty_rdd = spark.sparkContext.emptyRDD()
print("Empty RDD: ",empty_rdd.collect())

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("Name", StringType(), True)
    ]
)

# input_df = spark.createDataFrame(data=empty_rdd, schema=columns)
# input_df.show(5, False)

empty_df = spark.createDataFrame([], schema=schema)
empty_df.show()