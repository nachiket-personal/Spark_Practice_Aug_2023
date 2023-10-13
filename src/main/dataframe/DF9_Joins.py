from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast

from src.main.utilities.SparkSessionHandler import create_spark_session

spark = create_spark_session("JoinExample", "local[2]")

emp_input_path = "..\\resources\\input\\employee.csv"
emp_df = spark.read.option("header", True).csv(emp_input_path)

emp_df.repartition(200)
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

emp_dept_inner_join_df: DataFrame = emp_df.join(broadcast(dept_df), emp_df["dept"] == dept_df["deptname"], "inner")
emp_dept_inner_join_df.show(20, False)
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

#emp_dept_left_join_df: DataFrame = emp_df.join(dept_df, emp_df["dept"] == dept_df["deptname"], "left")
#emp_dept_left_join_df.show(20, False)
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

#emp_dept_right_join_df: DataFrame = emp_df.join(dept_df, emp_df["dept"] == dept_df["deptname"], "right")
#emp_dept_right_join_df.show(20, False)
'''
+-----+--------+----+-------+--------+------+------+--------+-----------+
|empid|name    |age |dept   |salary  |gender|deptid|deptname|location   |
+-----+--------+----+-------+--------+------+------+--------+-----------+
|6    |Tom     |40  |Finance|55000.90|'M'   |1     |Finance |New York   |
|5    |Charlie |42  |Finance|65000.99|'F'   |1     |Finance |New York   |
|4    |Bob     |44  |Finance|90000.99|'M'   |1     |Finance |New York   |
|10   |Diana   |38  |Audit  |69000.10|'F'   |2     |Audit   |Chicago    |
|9    |John    |40  |Audit  |59000.90|'M'   |2     |Audit   |Chicago    |
|8    |Steve   |52  |Audit  |65000.99|'F'   |2     |Audit   |Chicago    |
|7    |Harry   |38  |Audit  |69000.10|'M'   |2     |Audit   |Chicago    |
|3    |Angelina|25  |IT     |93000.30|'F'   |3     |IT      |Los Angeles|
|2    |Alex    |33  |IT     |78000.25|'M'   |3     |IT      |Los Angeles|
|1    |Daniel  |32  |IT     |80000.50|'M'   |3     |IT      |Los Angeles|
|null |null    |null|null   |null    |null  |4     |Comp    |Las Vegas  |
|null |null    |null|null   |null    |null  |5     |Math    |Colorado   |
+-----+--------+----+-------+--------+------+------+--------+-----------+
'''

#emp_dept_left_semi_join_df: DataFrame = emp_df.join(dept_df, emp_df["dept"] == dept_df["deptname"], "leftsemi")
#emp_dept_left_semi_join_df.show(20, False)
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

#emp_dept_left_anti_join_df: DataFrame = emp_df.join(dept_df, emp_df["dept"] == dept_df["deptname"], "leftanti")
#   emp_dept_left_anti_join_df.show(20, False)
'''
+-----+----+---+-----------+--------+------+
|empid|name|age|dept       |salary  |gender|
+-----+----+---+-----------+--------+------+
|11   |Al  |41 |Science    |61000.90|'M'   |
|12   |Dan |38 |Electronics|69000.10|'F'   |
+-----+----+---+-----------+--------+------+
'''

spark.stop()