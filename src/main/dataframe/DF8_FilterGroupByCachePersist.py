from pyspark.sql.functions import col, sum, max, round
from pyspark.storagelevel import StorageLevel

from src.main.utilities.SparkSessionHandler import create_spark_session

spark = create_spark_session("FilterExample", "local[2]")

emp_input_path = "..\\resources\\input\\employee.csv"
emp_df = spark.read.option("header", True).csv(emp_input_path)
#emp_df.show(20, False)
'''
+-----+--------+---+-------+--------+------+
|empid|name    |age|dept   |salary  |gender|
+-----+--------+---+-------+--------+------+
|1    |Daniel  |32 |IT     |80000.50|'M'   |
|2    |Alex    |33 |IT     |78000.25|'M'   |
|3    |Angelina|25 |IT     |93000.30|'F'   |
|4    |Bob     |44 |Finance|90000.99|'M'   |
|5    |Charlie |42 |Finance|65000.99|'F'   |
|6    |Tom     |40 |Finance|55000.90|'M'   |
|7    |Harry   |38 |Audit  |69000.10|'M'   |
|8    |Steve   |52 |Audit  |65000.99|'F'   |
|9    |John    |40 |Audit  |59000.90|'M'   |
|10   |Diana   |38 |Audit  |69000.10|'F'   |
+-----+--------+---+-------+--------+------+
'''

#emp_df.cache()
#emp_df.persist(StorageLevel.MEMORY_ONLY)
#emp_df.persist(StorageLevel.MEMORY_AND_DISK)

#emp_df.filter(col("dept") == "IT").show(10, False)
'''
+-----+--------+---+----+--------+------+
|empid|name    |age|dept|salary  |gender|
+-----+--------+---+----+--------+------+
|1    |Daniel  |32 |IT  |80000.50|'M'   |
|2    |Alex    |33 |IT  |78000.25|'M'   |
|3    |Angelina|25 |IT  |93000.30|'F'   |
+-----+--------+---+----+--------+------+
'''

#emp_df.filter((col("dept") == "IT") & (col("gender") == "'M'")).show(10, False)
'''
+-----+------+---+----+--------+------+
|empid|name  |age|dept|salary  |gender|
+-----+------+---+----+--------+------+
|1    |Daniel|32 |IT  |80000.50|'M'   |
|2    |Alex  |33 |IT  |78000.25|'M'   |
+-----+------+---+----+--------+------+
'''

#emp_df.filter((col("dept") == "IT") | (col("dept") == "Audit")).show(10, False)
'''
+-----+--------+---+-----+--------+------+
|empid|name    |age|dept |salary  |gender|
+-----+--------+---+-----+--------+------+
|1    |Daniel  |32 |IT   |80000.50|'M'   |
|2    |Alex    |33 |IT   |78000.25|'M'   |
|3    |Angelina|25 |IT   |93000.30|'F'   |
|7    |Harry   |38 |Audit|69000.10|'M'   |
|8    |Steve   |52 |Audit|65000.99|'F'   |
|9    |John    |40 |Audit|59000.90|'M'   |
|10   |Diana   |38 |Audit|69000.10|'F'   |
+-----+--------+---+-----+--------+------+
'''

#emp_df.where(col("name").startswith("An")).show(20, False)
'''
+-----+--------+---+----+--------+------+
|empid|name    |age|dept|salary  |gender|
+-----+--------+---+----+--------+------+
|3    |Angelina|25 |IT  |93000.30|'F'   |
+-----+--------+---+----+--------+------+
'''

#emp_df.where(col("name").contains("ie")).show(20, False)
'''
+-----+-------+---+-------+--------+------+
|empid|name   |age|dept   |salary  |gender|
+-----+-------+---+-------+--------+------+
|1    |Daniel |32 |IT     |80000.50|'M'   |
|5    |Charlie|42 |Finance|65000.99|'F'   |
+-----+-------+---+-------+--------+------+
'''

#emp_df.where(col("dept").like("%nan%")).show(20, False)
'''
+-----+-------+---+-------+--------+------+
|empid|name   |age|dept   |salary  |gender|
+-----+-------+---+-------+--------+------+
|4    |Bob    |44 |Finance|90000.99|'M'   |
|5    |Charlie|42 |Finance|65000.99|'F'   |
|6    |Tom    |40 |Finance|55000.90|'M'   |
+-----+-------+---+-------+--------+------+
'''

#dept_list = ["Audit", "Science"]
#emp_df.where(col("dept").isin(dept_list)).show(20, False)
'''
+-----+-----+---+-----+--------+------+
|empid|name |age|dept |salary  |gender|
+-----+-----+---+-----+--------+------+
|7    |Harry|38 |Audit|69000.10|'M'   |
|8    |Steve|52 |Audit|65000.99|'F'   |
|9    |John |40 |Audit|59000.90|'M'   |
|10   |Diana|38 |Audit|69000.10|'F'   |
+-----+-----+---+-----+--------+------+
'''

#emp_df.groupBy(col("dept")).count().show(10, False)
'''
+-------+-----+
|dept   |count|
+-------+-----+
|Audit  |4    |
|Finance|3    |
|IT     |3    |
+-------+-----+
'''

#emp_df.printSchema()
'''root
 |-- empid: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- salary: string (nullable = true)
 |-- gender: string (nullable = true)
 '''

emp_df2 = emp_df.withColumn("salary", col("salary").cast("float"))
#emp_df2.printSchema()
'''
root
 |-- empid: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- salary: float (nullable = true)
 |-- gender: string (nullable = true)
 '''

#(emp_df2.groupBy(col("dept")).sum("salary").alias("sum_salary").show(10, False))
'''
+-------+---------------+
|dept   |sum(salary)    |
+-------+---------------+
|Audit  |262002.08984375|
|Finance|210002.87890625|
|IT     |251001.046875  |
+-------+---------------+
'''

# emp_df3 = emp_df2.groupBy(col("dept")).sum("salary")
# emp_df3.show(10, False)
'''
+-------+---------------+
|dept   |sum(salary)    |
+-------+---------------+
|Audit  |262002.08984375|
|Finance|210002.87890625|
|IT     |251001.046875  |
+-------+---------------+
'''

# (emp_df3
#  .withColumn("sum_salary", round(col("sum(salary)"), 2))
#  .show(10, False))
'''
+-------+---------------+----------+
|dept   |sum(salary)    |sum_salary|
+-------+---------------+----------+
|Audit  |262002.08984375|262002.09 |
|Finance|210002.87890625|210002.88 |
|IT     |251001.046875  |251001.05 |
+-------+---------------+----------+
'''

# (emp_df2
#  .groupBy("dept", "gender")
#  .max("salary")
#  .select("dept", "gender", col("max(salary)")
#  .alias("max_salary"))
#  .show(10, False))
'''
+-------+------+----------+
|dept   |gender|max_salary|
+-------+------+----------+
|Finance|'F'   |65000.99  |
|IT     |'F'   |93000.3   |
|Finance|'M'   |90000.99  |
|IT     |'M'   |80000.5   |
|Audit  |'M'   |69000.1   |
|Audit  |'F'   |69000.1   |
+-------+------+----------+
'''


emp_df2.groupBy(col("dept")).agg(sum("salary").alias("agg_sum_salary"), max("salary").alias("agg_max_salary")).show()
'''
+-------+--------------+--------------+
|   dept|agg_sum_salary|agg_max_salary|
+-------+--------------+--------------+
|  Audit|     262002.09|      69000.10|
|Finance|     210002.88|      90000.99|
|     IT|     251001.05|      93000.30|
+-------+--------------+--------------+
'''