from pyspark.sql import DataFrame

from src.main.utilities.SparkSessionHandler import create_spark_session

spark = create_spark_session("SparkSqlExample", "local[2]")

emp_input_path = "..\\resources\\input\\employee.csv"
emp_df: DataFrame = spark.read.option("header", True).csv(emp_input_path)
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

dept_input_path = "..\\resources\\input\\department.csv"
dept_df: DataFrame = spark.read.option("header", True).csv(dept_input_path)
#dept_df.show(20, False)
'''
+------+--------+-----------+
|deptid|deptname|location   |
+------+--------+-----------+
|1     |Finance |New York   |
|2     |Audit   |Chicago    |
|3     |IT      |Los Angeles|
|4     |Comp    |Las Vegas  |
|5     |Math    |Colorado   |
+------+--------+-----------+
'''

emp_df.createOrReplaceTempView("EMP")
dept_df.createOrReplaceTempView("DEPT")

#emp_sql_df: DataFrame = spark.sql("SELECT * FROM EMP")
#emp_sql_df.show(20, False)
'''
+-----+--------+---+-----------+--------+------+
|empid|name    |age|dept       |salary  |gender|
+-----+--------+---+-----------+--------+------+
|1    |Daniel  |32 |IT         |80000.50|'M'   |
|2    |Alex    |33 |IT         |78000.25|'M'   |
|3    |Angelina|25 |IT         |93000.30|'F'   |
|4    |Bob     |44 |Finance    |90000.99|'M'   |
|5    |Charlie |42 |Finance    |65000.99|'F'   |
|6    |Tom     |40 |Finance    |55000.90|'M'   |
|7    |Harry   |38 |Audit      |69000.10|'M'   |
|8    |Steve   |52 |Audit      |65000.99|'F'   |
|9    |John    |40 |Audit      |59000.90|'M'   |
|10   |Diana   |38 |Audit      |69000.10|'F'   |
|11   |Al      |41 |Science    |61000.90|'M'   |
|12   |Dan     |38 |Electronics|69000.10|'F'   |
+-----+--------+---+-----------+--------+------+
'''

#emp_age_gt_filter_sql_df: DataFrame = spark.sql("SELECT * FROM EMP WHERE age > 38 ")
#emp_age_gt_filter_sql_df.show(20, False)
'''
+-----+-------+---+-------+--------+------+
|empid|name   |age|dept   |salary  |gender|
+-----+-------+---+-------+--------+------+
|4    |Bob    |44 |Finance|90000.99|'M'   |
|5    |Charlie|42 |Finance|65000.99|'F'   |
|6    |Tom    |40 |Finance|55000.90|'M'   |
|8    |Steve  |52 |Audit  |65000.99|'F'   |
|9    |John   |40 |Audit  |59000.90|'M'   |
|11   |Al     |41 |Science|61000.90|'M'   |
+-----+-------+---+-------+--------+------+
'''
#emp_age_lt_filter_sql_df: DataFrame = spark.sql("SELECT * FROM EMP WHERE age < 38 ")
#emp_age_lt_filter_sql_df.show(20, False)
'''
+-----+--------+---+----+--------+------+
|empid|name    |age|dept|salary  |gender|
+-----+--------+---+----+--------+------+
|1    |Daniel  |32 |IT  |80000.50|'M'   |
|2    |Alex    |33 |IT  |78000.25|'M'   |
|3    |Angelina|25 |IT  |93000.30|'F'   |
+-----+--------+---+----+--------+------+
'''

# emp_age_sort_sql_df: DataFrame = spark.sql("SELECT * FROM EMP WHERE age > 38 ORDER BY age DESC ")
# emp_age_sort_sql_df.show(20, False)
'''
+-----+-------+---+-------+--------+------+
|empid|name   |age|dept   |salary  |gender|
+-----+-------+---+-------+--------+------+
|8    |Steve  |52 |Audit  |65000.99|'F'   |
|4    |Bob    |44 |Finance|90000.99|'M'   |
|5    |Charlie|42 |Finance|65000.99|'F'   |
|11   |Al     |41 |Science|61000.90|'M'   |
|6    |Tom    |40 |Finance|55000.90|'M'   |
|9    |John   |40 |Audit  |59000.90|'M'   |
+-----+-------+---+-------+--------+------+
'''

#emp_dept_join_df: DataFrame = spark.sql("SELECT * FROM EMP e INNER JOIN DEPT d ON e.dept == d.deptname")
#emp_dept_join_df.show(20, False)
'''
+-----+--------+---+-------+--------+------+------+--------+-----------+
|empid|name    |age|dept   |salary  |gender|deptid|deptname|location   |
+-----+--------+---+-------+--------+------+------+--------+-----------+
|1    |Daniel  |32 |IT     |80000.50|'M'   |3     |IT      |Los Angeles|
|2    |Alex    |33 |IT     |78000.25|'M'   |3     |IT      |Los Angeles|
|3    |Angelina|25 |IT     |93000.30|'F'   |3     |IT      |Los Angeles|
|4    |Bob     |44 |Finance|90000.99|'M'   |1     |Finance |New York   |
|5    |Charlie |42 |Finance|65000.99|'F'   |1     |Finance |New York   |
|6    |Tom     |40 |Finance|55000.90|'M'   |1     |Finance |New York   |
|7    |Harry   |38 |Audit  |69000.10|'M'   |2     |Audit   |Chicago    |
|8    |Steve   |52 |Audit  |65000.99|'F'   |2     |Audit   |Chicago    |
|9    |John    |40 |Audit  |59000.90|'M'   |2     |Audit   |Chicago    |
|10   |Diana   |38 |Audit  |69000.10|'F'   |2     |Audit   |Chicago    |
+-----+--------+---+-------+--------+------+------+--------+-----------+
'''