from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSessionExample").master("local[2]").getOrCreate()

data = [(1, "John"), (2, "Bob"), (3, "Alice"), (4, "Alice XYZZZZZZZZZZZZZZZZZZZZZZ"), (5, "Smith YYYYYYYYYYYY"), (6, "Alex ZZZZZZZZZZZZZZZZZZZZ")]
columns = ["id", "name"]
input_df = spark.createDataFrame(data=data, schema=columns)
input_df.show(4, False)
'''
+---+------------------------------+
|id |name                          |
+---+------------------------------+
|1  |John                          |
|2  |Bob                           |
|3  |Alice                         |
|4  |Alice XYZZZZZZZZZZZZZZZZZZZZZZ|
+---+------------------------------+
'''

input_df.printSchema()
'''
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
'''

spark.stop()