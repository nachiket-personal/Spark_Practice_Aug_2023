from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

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

spark.stop()