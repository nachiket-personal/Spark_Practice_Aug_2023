from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

from src.main.utilities.SparkSessionHandler import create_spark_session

spark = create_spark_session("ReadCsvExample", "local[2]")

#emp_input_path = "..\\resources\\input\\employee.csv"
emp_input_path2 = "C:\\Users\\Administrator\\PycharmProjects\\Spark_Practice_Aug_2023\\src\\main\\resources\\input\\employee.csv"
#emp_df = spark.read.option("header", True).option("inferSchema", True).csv(emp_input_path)
#emp_df.show()
'''
+-----+--------+---+-------+--------+------+
|empid|    name|age|   dept|  salary|gender|
+-----+--------+---+-------+--------+------+
|    1|  Daniel| 32|     IT| 80000.5|   'M'|
|    2|    Alex| 33|     IT|78000.25|   'M'|
|    3|Angelina| 25|     IT| 93000.3|   'F'|
|    4|     Bob| 44|Finance|90000.99|   'M'|
|    5| Charlie| 42|Finance|65000.99|   'F'|
|    6|     Tom| 40|Finance| 55000.9|   'M'|
|    7|   Harry| 38|  Audit| 69000.1|   'M'|
|    8|   Steve| 52|  Audit|65000.99|   'F'|
|    9|    John| 40|  Audit| 59000.9|   'M'|
|   10|   Diana| 38|  Audit| 69000.1|   'F'|
+-----+--------+---+-------+--------+------+
'''
#emp_df.printSchema()
'''
root
 |-- empid: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- dept: string (nullable = true)
 |-- salary: double (nullable = true)
 |-- gender: string (nullable = true)
 '''

#emp_input_path2 = "..\\resources\\input\\employee2.csv"
# emp_df2 = spark.read.option("header", True).option("inferSchema", True).option("delimiter", "|").csv(emp_input_path2)
# emp_df2.show()
# emp_df2.printSchema()


# emp_df3 = spark.read.options(header=True, inferschema=True, delimiter='|').format("csv").load(emp_input_path2)
# emp_df3.show()
# emp_df3.printSchema()

# emp_df4 = spark.read.csv(emp_input_path)
# emp_df4.show()
'''
+-----+--------+---+-------+--------+------+
|  _c0|     _c1|_c2|    _c3|     _c4|   _c5|
+-----+--------+---+-------+--------+------+
|empid|    name|age|   dept|  salary|gender|
|    1|  Daniel| 32|     IT|80000.50|   'M'|
|    2|    Alex| 33|     IT|78000.25|   'M'|
|    3|Angelina| 25|     IT|93000.30|   'F'|
|    4|     Bob| 44|Finance|90000.99|   'M'|
|    5| Charlie| 42|Finance|65000.99|   'F'|
|    6|     Tom| 40|Finance|55000.90|   'M'|
|    7|   Harry| 38|  Audit|69000.10|   'M'|
|    8|   Steve| 52|  Audit|65000.99|   'F'|
|    9|    John| 40|  Audit|59000.90|   'M'|
|   10|   Diana| 38|  Audit|69000.10|   'F'|
+-----+--------+---+-------+--------+------+
'''
#emp_df4.printSchema()
'''
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 '''

emp_schema = (StructType()
          .add("Employee_Id", StringType(), True)
          .add("Name", StringType(), True)
          .add("Age", IntegerType(), True)
          .add("Department", StringType(), True)
          .add("Salary", FloatType(), True)
          .add("Gender", StringType(), True)
          )


emp_df5 = spark.read.option("header", True).schema(schema=emp_schema).csv(emp_input_path2)
#emp_df5.show(3)
'''
+-----------+--------+---+----------+--------+------+
|Employee_Id|    Name|Age|Department|  Salary|Gender|
+-----------+--------+---+----------+--------+------+
|          1|  Daniel| 32|        IT| 80000.5|   'M'|
|          2|    Alex| 33|        IT|78000.25|   'M'|
|          3|Angelina| 25|        IT| 93000.3|   'F'|
+-----------+--------+---+----------+--------+------+
'''
#emp_df5.printSchema()
'''
root
 |-- Employee_Id: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- Age: integer (nullable = true)
 |-- Department: string (nullable = true)
 |-- Salary: float (nullable = true)
 |-- Gender: string (nullable = true)
 '''

#emp_df6 = emp_df5.select("Employee_Id", "Department", "Salary")
#emp_df6.show(2, False)
'''
+-----------+----------+--------+
|Employee_Id|Department|Salary  |
+-----------+----------+--------+
|1          |IT        |80000.5 |
|2          |IT        |78000.25|
+-----------+----------+--------+
'''
#emp_df6.printSchema()
'''
root
 |-- Employee_Id: string (nullable = true)
 |-- Department: string (nullable = true)
 |-- Salary: float (nullable = true)
 '''

emp_df7 = emp_df5.select(col("Name").alias("name"), col("Age"), col("Gender"))
emp_df7.show(2, False)
'''
+------+---+------+
|Name  |Age|Gender|
+------+---+------+
|Daniel|32 |'M'   |
|Alex  |33 |'M'   |
+------+---+------+
'''

#emp_df5.select(emp_df5["Employee_Id"].alias("EmpId"), emp_df5["Salary"].alias("Sal")).show(2, False)
'''
+-----+--------+
|EmpId|Sal     |
+-----+--------+
|1    |80000.5 |
|2    |78000.25|
+-----+--------+
'''

#emp_df5.select(emp_df5["Employee_Id"].alias("EmpId"), col("Salary").alias("Sal")).show(2, False)
'''
+-----+--------+
|EmpId|Sal     |
+-----+--------+
|1    |80000.5 |
|2    |78000.25|
+-----+--------+
'''

#emp_df5.select(emp_df5.Employee_Id).show(2, False)
'''
+-----------+
|Employee_Id|
+-----------+
|1          |
|2          |
+-----------+
'''

emp_csv_output = "..\\resources\\output\\emp_csv_df_output"
emp_df7.write.option("header", True).mode("overwrite").format("csv").save(emp_csv_output)

#emp_df7.write.option("header", True).mode("append").format("csv").save(emp_csv_output)
#emp_df7.write.option("header", True).mode("ignore").format("csv").save(emp_csv_output)
#emp_df7.write.option("header", True).mode("error").format("csv").save(emp_csv_output)

emp_json_output = "..\\resources\\output\\emp_json_df_output"
emp_df7.write.mode("overwrite").format("json").save(emp_json_output)
spark.stop()