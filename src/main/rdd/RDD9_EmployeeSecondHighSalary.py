from src.main.utilities.SparkContextHandler import create_spark_context

def read_text_file(input_path):
    input_rdd = sc.textFile(input_path)
    return input_rdd

sc = create_spark_context("local[4]", "Employee2ndHighSalaryExample")
emp_input_path = "..\\resources\\input\\employee.csv"
emp_rdd = read_text_file(emp_input_path)
#print("emp_rdd: ", emp_rdd.collect())

header = emp_rdd.first()

def parse_csv_file(line):
    data = line.split(",")
    emp_name = data[1]
    salary = data[4]
    return (emp_name, salary)


emp_rdd_skip_header = emp_rdd.filter(lambda line: line != header)
emp_salary_rdd = emp_rdd_skip_header.map(lambda line: parse_csv_file(line))
#print("emp_salary_rdd: ", emp_salary_rdd.collect())

sorted_emp_rdd = emp_salary_rdd.sortBy(lambda data: data[1], ascending=False)
#print("sorted_emp_rdd: ", sorted_emp_rdd.collect())
#sorted_emp_rdd:  [('Angelina', '93000.30'), ('Bob', '90000.99'), ('Daniel', '80000.50'), ('Alex', '78000.25'), ('Harry', '69000.10'), ('Diana', '69000.10'), ('Charlie', '65000.99'), ('Steve', '65000.99'), ('John', '59000.90'), ('Tom', '55000.90')]

emp_top_2_records = sorted_emp_rdd.take(2)
#print("emp_top_2_records: ", emp_top_2_records)
#emp_top_2_records:  [('Angelina', '93000.30'), ('Bob', '90000.99')]

second_top = emp_top_2_records[-1]
#print("first employee: ", emp_top_2_records[0])
#first employee:  ('Angelina', '93000.30')

#print("second employee: ", second_top)
#second employee:  ('Bob', '90000.99')

print(f"The Second Top Salary of the employee is {second_top[1]} and employee name is: {second_top[0]}")

sc.stop()