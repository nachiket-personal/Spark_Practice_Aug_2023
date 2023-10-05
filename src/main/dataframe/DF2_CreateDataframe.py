from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType, DateType, \
    ArrayType, MapType
from datetime import date

spark = SparkSession.builder.appName("CreateDataframeExample").master("local[2]").getOrCreate()

emp_input_data = [("ABC", "XYZ", "511012", "F", 30000.10, 34),
        ("PQR", "LMN", "511013", "M", 45000.10, 24),
        ("PQR", "NTY", "511014", "M", 25000.10, 55),
        ("IJK", "OPT", "511015", "F", 97000.10, 36),
        ("WTA", "TYU", "511016", "M", 55000.10, 27)
        ]

emp_schema = StructType(
    [
        StructField("FirstName", StringType(), True),
        StructField("LastName", StringType(), True),
        StructField("PinCode", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Salary", FloatType(), True),
        StructField("Age", IntegerType(), True)
    ]
)

emp_df = spark.createDataFrame(data=emp_input_data, schema=emp_schema)
emp_df.show()
'''
+---------+--------+-------+------+-------+---+
|FirstName|LastName|PinCode|Gender| Salary|Age|
+---------+--------+-------+------+-------+---+
|      ABC|     XYZ| 511012|     F|30000.1| 34|
|      PQR|     LMN| 511013|     M|45000.1| 24|
|      PQR|     NTY| 511014|     M|25000.1| 55|
|      IJK|     OPT| 511015|     F|97000.1| 36|
|      WTA|     TYU| 511016|     M|55000.1| 27|
+---------+--------+-------+------+-------+---+
'''

emp_df.printSchema()
'''
root
 |-- FirstName: string (nullable = true)
 |-- LastName: string (nullable = true)
 |-- PinCode: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Salary: float (nullable = true)
 |-- Age: integer (nullable = true)
 '''

emp_nested_data = [
        (("ABC", "XYZ"), "511012", "F", 30000.10, 34, True, date(1988, 6, 5), ["Spark", "Python", "AWS"], {"Q1": 8, "Q2": 9}),
        (("PQR", "LMN"), "511013", "M", 45000.10, 24, False, date(1997, 2, 25), ["Java", "SQL", "UI"], {"Q1": 5, "Q2": 7}),
        (("PQR", "NTY"), "511014", "M", 25000.10, 55, True, date(1988, 11, 17), ["Backend", "DevOps", "Splunk"], {"Q1": 9, "Q2": 8}),
        (("IJK", "OPT"), "511015", "F", 97000.10, 36, False, date(1988, 9, 21), ["Airflow", "Python", "Angular"], {"Q1": 8, "Q2": 8}),
        (("WTA", "TYU"), "511016", "M", 55000.10, 27, True, date(1988, 12, 27), ["Hadoop", "Hive", "Pig"], {"Q1": 7, "Q2": 9})
]

emp_nested_schema = StructType([
    StructField("name", StructType([
        StructField("FirstName", StringType(), True),
        StructField("LastName", StringType(), True)
    ])),
    StructField("PinCode", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Salary", FloatType(), True),
    StructField("Age", IntegerType(), True),
    StructField("isOnPayroll", BooleanType(), True),
    StructField("BirthDate", DateType(), True),
    StructField("Skills", ArrayType(StringType()), True),
    StructField("Ratings", MapType(StringType(), IntegerType()), True)
])

emp_nested_df = spark.createDataFrame(data=emp_nested_data, schema=emp_nested_schema)
emp_nested_df.show(5, False)
'''
+----------+-------+------+-------+---+-----------+----------+--------------------------+------------------+
|name      |PinCode|Gender|Salary |Age|isOnPayroll|BirthDate |Skills                    |Ratings           |
+----------+-------+------+-------+---+-----------+----------+--------------------------+------------------+
|[ABC, XYZ]|511012 |F     |30000.1|34 |true       |1988-06-05|[Spark, Python, AWS]      |[Q1 -> 8, Q2 -> 9]|
|[PQR, LMN]|511013 |M     |45000.1|24 |false      |1997-02-25|[Java, SQL, UI]           |[Q1 -> 5, Q2 -> 7]|
|[PQR, NTY]|511014 |M     |25000.1|55 |true       |1988-11-17|[Backend, DevOps, Splunk] |[Q1 -> 9, Q2 -> 8]|
|[IJK, OPT]|511015 |F     |97000.1|36 |false      |1988-09-21|[Airflow, Python, Angular]|[Q1 -> 8, Q2 -> 8]|
|[WTA, TYU]|511016 |M     |55000.1|27 |true       |1988-12-27|[Hadoop, Hive, Pig]       |[Q1 -> 7, Q2 -> 9]|
+----------+-------+------+-------+---+-----------+----------+--------------------------+------------------+
'''
emp_nested_df.printSchema()
'''
root
 |-- name: struct (nullable = true)
 |    |-- FirstName: string (nullable = true)
 |    |-- LastName: string (nullable = true)
 |-- PinCode: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Salary: float (nullable = true)
 |-- Age: integer (nullable = true)
 |-- isOnPayroll: boolean (nullable = true)
 |-- BirthDate: date (nullable = true)
 |-- Skills: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- Ratings: map (nullable = true)
 |    |-- key: string
 |    |-- value: integer (valueContainsNull = true)
 '''


spark.stop()