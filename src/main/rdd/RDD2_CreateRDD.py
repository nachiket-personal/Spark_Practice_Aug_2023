from pyspark import SparkContext

sc = SparkContext("local[2]", "CreateRDD_Example")

list_data = [1,2,4,5,6]

rdd_list = sc.parallelize(list_data)
print(rdd_list.collect())
#[1, 2, 4, 5, 6]

tuple_data = [(1, "emp1"), (2, "emp2"), (3, "emp3")]
rdd_tuple = sc.parallelize(tuple_data)
print(rdd_tuple.collect())
#[(1, 'emp1'), (2, 'emp2'), (3, 'emp3')]

rdd_file = sc.textFile("..\\resources\\employee.csv")
print(rdd_file.collect())
#['empid,name,age,dept,salary,gender', "1,Daniel,32,IT,80000.50,'M'", "2,Alex,33,Comp,78000.25,'M'", "3,Angelina,25,Finance,93000.30,'F'", "4,Bob,44,Audit,99000.99,'M'", "5,Charlie,44,Audit,99000.99,'F'"]

rdd_empty = sc.emptyRDD()
print("Empty RDD: ", rdd_empty.collect())
#Empty RDD:  []

rdd_empty_list = sc.parallelize([])
print("Empty RDD List: ", rdd_empty_list.collect())
#Empty RDD List:  []

print("First Record: ", rdd_file.first())
#First Record:  empid,name,age,dept,salary,gender

print("Count File: ", rdd_file.count())
#Count File:  6

