from SparkContextHandler import create_spark_context
import os.path as path
import shutil

def remove_existing_file(emp_output_path):
    if path.exists(emp_output_path):
        shutil.rmtree(emp_output_path)

def read_text_file(input_path):
    input_rdd = sc.textFile(input_path)
    return input_rdd

sc = create_spark_context("yarn", "EmployeeTransformationsExample")
emp_input_path = "..\\resources\\input\\employee.csv"
emp_rdd = read_text_file(emp_input_path)
#print(emp_rdd.collect())
#['empid,name,age,dept,salary,gender', "1,Daniel,32,IT,80000.50,'M'", "2,Alex,33,IT,78000.25,'M'", "3,Angelina,25,IT,93000.30,'F'", "4,Bob,44,Finance,90000.99,'M'", "5,Charlie,42,Finance,65000.99,'F'", "6,Tom,40,Finance,55000.90,'M'", "7,Harry,38,Audit,69000.10,'M'", "8,Steve,52,Audit,65000.99,'F'", "9,John,40,Audit,59000.90,'M'", "10,Diana,38,Audit,69000.10,'F'"]

def parse_csv_file(line):
    print("line: ", line)
    data = line.split(",")
    if len(data)!=6:
        raise ValueError("Data is wrong...")

    empid = data[0]
    empname = data[1]
    age = data[2]
    dept = data[3]
    salary = float(data[4])

    if dept == "IT":
        increased_salary = round(salary * 1.10, 3)
    elif dept == "Finance":
        increased_salary = round(salary * 1.07, 3)
    else:
        increased_salary = round(salary * 1.05, 3)

    return (empid, empname, age, dept, salary, increased_salary)


emp_rdd_header = emp_rdd.first()
filter_emp_header = emp_rdd.filter(lambda line: line != emp_rdd_header)
parse_rdd = filter_emp_header.map(lambda line: parse_csv_file(line))
print("parse rdd", parse_rdd.collect())

emp_increased_salary_path = "..\\resources\\output\\emp_increased_salary"

header = "(empid,name,age,dept,salary,gender)"
emp_rdd_header = sc.parallelize([header], 1)
filtered_rdd_header = parse_rdd.union(emp_rdd_header)

remove_existing_file(emp_increased_salary_path)
filtered_rdd_header.saveAsTextFile(emp_increased_salary_path)
