from src.main.utilities.SparkContextHandler import create_spark_context
import os.path as path
import shutil

def remove_existing_file(emp_output_path):
    if path.exists(emp_output_path):
        shutil.rmtree(emp_output_path)

def read_text_file(input_path):
    input_rdd = sc.textFile(input_path)
    return input_rdd

sc = create_spark_context("local[2]", "EmployeeTransformationsExample")
emp_input_path = "..\\resources\\input\\employee.csv"
emp_rdd = read_text_file(emp_input_path)

header = emp_rdd.first()
emp_rdd_skip_header = emp_rdd.filter(lambda line: line != header)

split_emp_rdd = emp_rdd_skip_header.map(lambda line: line.split(","))
print("split_emp_rdd: ", split_emp_rdd.collect())
#split_emp_rdd:  [['1', 'Daniel', '32', 'IT', '80000.50', "'M'"], ['2', 'Alex', '33', 'IT', '78000.25', "'M'"], ['3', 'Angelina', '25', 'IT', '93000.30', "'F'"], ['4', 'Bob', '44', 'Finance', '90000.99', "'M'"], ['5', 'Charlie', '42', 'Finance', '65000.99', "'F'"], ['6', 'Tom', '40', 'Finance', '55000.90', "'M'"], ['7', 'Harry', '38', 'Audit', '69000.10', "'M'"], ['8', 'Steve', '52', 'Audit', '65000.99', "'F'"], ['9', 'John', '40', 'Audit', '59000.90', "'M'"], ['10', 'Diana', '38', 'Audit', '69000.10', "'F'"]]

# #filter data based on Dept
filtered_condition = lambda data: "Audit" in data[3]
filtered_emp_rdd = split_emp_rdd.filter(filtered_condition)
filtered_emp_rdd.foreach(print)

#Fetch Emp Name, Age
emp_rdd_with_name_age = split_emp_rdd.map(lambda data: (data[1], int(data[2])))
print("emp_rdd_with_name_age: ", emp_rdd_with_name_age.collect())
#emp_rdd_with_name_age:  [('Daniel', '32'), ('Alex', '33'), ('Angelina', '25'), ('Bob', '44'), ('Charlie', '42'), ('Tom', '40'), ('Harry', '38'), ('Steve', '52'), ('John', '40'), ('Diana', '38')]

#Find Oldest Employee Name with Age
oldest_employee = emp_rdd_with_name_age.reduce(lambda x, y: x if x[1] > y[1] else y)
print("oldest_employee: ", oldest_employee)
#oldest_employee:  ('Steve', 52)

#Find Youngest Employee Name with Age
youngest_employee = emp_rdd_with_name_age.reduce(lambda x, y: x if x[1] < y[1] else y)
print("youngest_employee: ", youngest_employee)
#youngest_employee:  ('Angelina', 25)