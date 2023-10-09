from src.main.utilities.SparkSessionHandler import create_spark_session

spark = create_spark_session("ReadParquetExample", "local[2]")

emp_parquet_input = "..\\resources\\output\\emp_parquet_df_output"
emp_parquet_df = spark.read.parquet(emp_parquet_input)
emp_parquet_df.show(2, False)
'''
+------+---+------+
|name  |Age|Gender|
+------+---+------+
|Daniel|32 |'M'   |
|Alex  |33 |'M'   |
+------+---+------+
'''
emp_parquet_df.printSchema()