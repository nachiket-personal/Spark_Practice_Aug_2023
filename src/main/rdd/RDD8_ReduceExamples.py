from src.main.utilities.SparkContextHandler import create_spark_context
sc = create_spark_context("local[2]", "ReduceExample")

rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.reduce(lambda x, y: x + y)
print("reduce: ", result)

rdd = sc.parallelize([(1, 2), (3, 4), (1, 6), (3, 7)])
result = rdd.reduceByKey(lambda x, y: x + y)
print("reduceByKey: ", result.collect())