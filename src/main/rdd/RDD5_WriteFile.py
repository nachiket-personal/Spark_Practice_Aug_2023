from SparkContextHandler import create_spark_context
import os.path as path
import shutil

#sc = create_spark_context("yarn", "write_csv_example")
sc = create_spark_context("local[4]", "write_example")

def remove_existing_file(emp_output_path):
    if path.exists(emp_output_path):
        shutil.rmtree(emp_output_path)

def read_text_file(input_path):
    input_rdd = sc.textFile(input_path)
    return input_rdd


def write_text_file(input_rdd, output_path):
    input_rdd.saveAsTextFile(output_path)


def write_seq_file(input_rdd, output_path):
    output_rdd = (input_rdd.map(lambda line: line.split(",")).map(lambda data: (data[3], data[4])))
    print("output_rdd: ", output_rdd.collect())
    return output_rdd.saveAsSequenceFile(output_path)


emp_input_path = "..\\resources\\input\\employee.csv"
emp_rdd = read_text_file(emp_input_path)
#print(emp_rdd.collect())
#['empid,name,age,dept,salary,gender', "1,Daniel,32,IT,80000.50,'M'", "2,Alex,33,IT,78000.25,'M'", "3,Angelina,25,IT,93000.30,'F'", "4,Bob,44,Finance,90000.99,'M'", "5,Charlie,42,Finance,65000.99,'F'", "6,Tom,40,Finance,55000.90,'M'", "7,Harry,38,Audit,69000.10,'M'", "8,Steve,52,Audit,65000.99,'F'", "9,John,40,Audit,59000.90,'M'", "10,Diana,38,Audit,69000.10,'F'"]

emp_output_path = "..\\resources\\output\\emp_text_rdd"
remove_existing_file(emp_output_path)
write_text_file(emp_rdd, emp_output_path)

emp_seq_output_path = "..\\resources\\output\\emp_sequence_rdd"
write_seq_file(emp_rdd, emp_seq_output_path)
