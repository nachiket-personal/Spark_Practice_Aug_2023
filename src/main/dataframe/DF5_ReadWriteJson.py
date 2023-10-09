from src.main.utilities.SparkSessionHandler import create_spark_session

spark = create_spark_session("ReadJsonExample", "local[2]")

emp_json_input = "..\\resources\\output\\emp_json_df_output"
emp_json_df = spark.read.json(emp_json_input)

emp_json_df.show(2, False)
'''
+---+------+------+
|Age|Gender|name  |
+---+------+------+
|32 |'M'   |Daniel|
|33 |'M'   |Alex  |
+---+------+------+
'''
emp_json_df.printSchema()
'''
root
 |-- Age: long (nullable = true)
 |-- Gender: string (nullable = true)
 |-- name: string (nullable = true)
 '''


zip_codes_json_input = "..\\resources\\input\\zip_codes.json"
zip_codes_json_df = spark.read.option("multiline", True).json(zip_codes_json_input)
zip_codes_json_df.show(2, False)
'''
+-------------------+------------+-----+-----------+-------+
|City               |RecordNumber|State|ZipCodeType|Zipcode|
+-------------------+------------+-----+-----------+-------+
|PASEO COSTA DEL SUR|2           |PR   |STANDARD   |704    |
|BDA SAN LUIS       |10          |PR   |STANDARD   |709    |
+-------------------+------------+-----+-----------+-------+
'''
zip_codes_json_df.printSchema()
'''
root
 |-- City: string (nullable = true)
 |-- RecordNumber: long (nullable = true)
 |-- State: string (nullable = true)
 |-- ZipCodeType: string (nullable = true)
 |-- Zipcode: long (nullable = true)
'''

emp_parquet_output = "..\\resources\\output\\emp_parquet_df_output"
emp_json_df.write.parquet(emp_parquet_output)