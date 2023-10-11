from pyspark.sql.functions import col, round, concat, lit, when, concat_ws, split, udf
from pyspark.sql.types import FloatType

from src.main.utilities.SparkSessionHandler import create_spark_session

spark = create_spark_session("TransformationsExample", "local[2]")

emp_input_path = "..\\resources\\input\\employee.csv"
emp_df = spark.read.option("header", True).csv(emp_input_path)
#emp_df.show(5, False)
'''
+-----+--------+---+-------+--------+------+
|empid|name    |age|dept   |salary  |gender|
+-----+--------+---+-------+--------+------+
|1    |Daniel  |32 |IT     |80000.5 |'M'   |
|2    |Alex    |33 |IT     |78000.25|'M'   |
|3    |Angelina|25 |IT     |93000.3 |'F'   |
|4    |Bob     |44 |Finance|90000.99|'M'   |
|5    |Charlie |42 |Finance|65000.99|'F'   |
+-----+--------+---+-------+--------+------+
'''
#emp_df.printSchema()
'''
root
 |-- empid: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- salary: string (nullable = true)
 |-- gender: string (nullable = true)
 '''

#Using withColumn
#1. change the datatype
#2. Update value of existing column
#3. Generate new column
#4. Using lit() we are assigning default value to each record in dataframe

def increase_salary(salary, dept):
    if dept == "IT":
        salary_addition = salary * 1.10
    elif dept == "Finance":
        salary_addition = salary * 1.07
    else:
        salary_addition = salary * 1.05

    return salary_addition


increase_salary_udf = udf(increase_salary, FloatType())


emp_df2 = (emp_df
           .withColumn("salary", col("salary").cast("float"))
           .withColumn("increase_salary", round(col("salary") * 1.10, 2).cast("float"))
           .withColumn("company_name", lit("XYZ"))
           .withColumn("empid_name1", concat(col("empid"), col("name")))
           .withColumn("empid_name2", concat(col("empid"), lit("_"), col("name")))
           .withColumn("empid_name_company_name", concat(col("empid"), lit("_"), col("name"), lit("_"), col("company_name")))
           .withColumn("empid_name_company_name_ws",
                       concat_ws("-", col("empid"), col("name"), col("company_name")))
           .withColumn("gender_in_fullname", when(col("gender") == "'M'", "Male")
                       .when(col("gender")== "'F'", "Female")
                       .otherwise("NA")
                       )
           .withColumn("address", lit("New York, USA, 103345"))
           .withColumn("city", split(col("address"), ",").getItem(0))
           .withColumn("country", split(col("address"), ",").getItem(1))
           .withColumn("pincode", split(col("address"), ",").getItem(2))
           .withColumn("dept_wise_salary_increase", increase_salary_udf(col("salary"), col("dept")))
           )
'''
+-----+--------+---+-------+--------+------+---------------+------------+-----------+-----------+-----------------------+--------------------------+------------------+---------------------+--------+-------+-------+-------------------------+
|empid|name    |age|dept   |salary  |gender|increase_salary|company_name|empid_name1|empid_name2|empid_name_company_name|empid_name_company_name_ws|gender_in_fullname|address              |city    |country|pincode|dept_wise_salary_increase|
+-----+--------+---+-------+--------+------+---------------+------------+-----------+-----------+-----------------------+--------------------------+------------------+---------------------+--------+-------+-------+-------------------------+
|1    |Daniel  |32 |IT     |80000.5 |'M'   |88000.55       |XYZ         |1Daniel    |1_Daniel   |1_Daniel_XYZ           |1-Daniel-XYZ              |Male              |New York, USA, 103345|New York| USA   | 103345|88000.55                 |
|2    |Alex    |33 |IT     |78000.25|'M'   |85800.28       |XYZ         |2Alex      |2_Alex     |2_Alex_XYZ             |2-Alex-XYZ                |Male              |New York, USA, 103345|New York| USA   | 103345|85800.27                 |
|3    |Angelina|25 |IT     |93000.3 |'F'   |102300.33      |XYZ         |3Angelina  |3_Angelina |3_Angelina_XYZ         |3-Angelina-XYZ            |Female            |New York, USA, 103345|New York| USA   | 103345|102300.33                |
|4    |Bob     |44 |Finance|90000.99|'M'   |99001.09       |XYZ         |4Bob       |4_Bob      |4_Bob_XYZ              |4-Bob-XYZ                 |Male              |New York, USA, 103345|New York| USA   | 103345|96301.06                 |
|5    |Charlie |42 |Finance|65000.99|'F'   |71501.09       |XYZ         |5Charlie   |5_Charlie  |5_Charlie_XYZ          |5-Charlie-XYZ             |Female            |New York, USA, 103345|New York| USA   | 103345|69551.055                |
+-----+--------+---+-------+--------+------+---------------+------------+-----------+-----------+-----------------------+--------------------------+------------------+---------------------+--------+-------+-------+-------------------------+
'''
emp_df2.printSchema()
'''
root
 |-- empid: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- salary: float (nullable = true)
 |-- gender: string (nullable = true)
 |-- increase_salary: float (nullable = true)
 |-- company_name: string (nullable = false)
 |-- empid_name1: string (nullable = true)
 |-- empid_name2: string (nullable = true)
 |-- empid_name_company_name: string (nullable = true)
 |-- empid_name_company_name_ws: string (nullable = false)
 |-- gender_in_fullname: string (nullable = false)
 |-- address: string (nullable = false)
 |-- city: string (nullable = true)
 |-- country: string (nullable = true)
 |-- pincode: string (nullable = true)
 |-- dept_wise_salary_increase: float (nullable = true)
 '''

emp_df3 = (emp_df2
           .select("empid", "name", "dept", "salary", "dept_wise_salary_increase")
            .withColumnRenamed("dept", "department")
           .drop("salary")
           )
#emp_df3.show(10, False)
'''
+-----+--------+----------+-------------------------+
|empid|name    |department|dept_wise_salary_increase|
+-----+--------+----------+-------------------------+
|1    |Daniel  |IT        |88000.55                 |
|2    |Alex    |IT        |85800.27                 |
|3    |Angelina|IT        |102300.33                |
|4    |Bob     |Finance   |96301.06                 |
|5    |Charlie |Finance   |69551.055                |
|6    |Tom     |Finance   |58850.96                 |
|7    |Harry   |Audit     |72450.11                 |
|8    |Steve   |Audit     |68251.04                 |
|9    |John    |Audit     |61950.945                |
|10   |Diana   |Audit     |72450.11                 |
+-----+--------+----------+-------------------------+
'''

spark.stop()